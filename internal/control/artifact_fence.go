package control

import (
	"cellhive/internal/artifacts"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrArtifactUnavailable = errors.New("control: artifact is being reclaimed, has uncertain I/O, or requires a complete upload and seal")

const artifactSchema = `
CREATE TABLE IF NOT EXISTS artifact_state(kind TEXT NOT NULL,id TEXT NOT NULL,ns TEXT NOT NULL,worker TEXT NOT NULL,state TEXT NOT NULL,op TEXT NOT NULL,holder TEXT NOT NULL,last_error TEXT NOT NULL DEFAULT '',PRIMARY KEY(kind,id));
CREATE INDEX IF NOT EXISTS artifact_state_scope ON artifact_state(ns,worker,state);
CREATE TABLE IF NOT EXISTS artifact_ops(kind TEXT NOT NULL,id TEXT NOT NULL,path TEXT NOT NULL,mode TEXT NOT NULL,op TEXT NOT NULL UNIQUE,holder TEXT NOT NULL,ns TEXT NOT NULL,worker TEXT NOT NULL,PRIMARY KEY(kind,id,path));
CREATE INDEX IF NOT EXISTS artifact_ops_scope ON artifact_ops(ns,worker);
CREATE TABLE IF NOT EXISTS artifact_receipts(id TEXT NOT NULL,path TEXT NOT NULL,sha TEXT NOT NULL,size INTEGER NOT NULL,ns TEXT NOT NULL,worker TEXT NOT NULL,PRIMARY KEY(id,path));
CREATE TABLE IF NOT EXISTS asset_manifests(id TEXT PRIMARY KEY,manifest TEXT NOT NULL);
`

// ArtifactOp is an unexpired exclusion, not a time lease. A successful local
// transaction alone is never permission for external I/O; capture must confirm it.
type ArtifactOp struct{ Kind, ID, Path, Mode, Op, Holder string }
type ArtifactReceipt struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// AssetSessionToken preserves normal CF deduplication. A legacy token deleted
// before a trusted content manifest existed cannot safely reuse that identity;
// a fresh opaque suffix lets the same source manifest be uploaded as new data.
func (s *Store) AssetSessionToken(ctx context.Context, ns, worker, canonical string) (string, error) {
	id := ns + "/" + worker + "/" + canonical
	if _, _, err := artifactScope("asset", id); err != nil {
		return "", err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return "", err
	}
	token := canonical
	err = tx(ctx, c, func(t *sql.Tx) error {
		if err := checkPurgeFenceTx(ctx, t, ns, worker); err != nil {
			return err
		}
		var legacyDeleted bool
		if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_state WHERE kind='asset' AND id=? AND state='deleted') AND NOT EXISTS(SELECT 1 FROM asset_manifests WHERE id=?)`, id, id).Scan(&legacyDeleted); err != nil {
			return err
		}
		if legacyDeleted {
			token = canonical + "-" + rand.Text()
		}
		return nil
	})
	return token, err
}

func artifactScope(kind, id string) (string, string, error) {
	if err := gcMarkKey(kind, id); err != nil {
		return "", "", err
	}
	if kind == "bundle" {
		if strings.Contains(id, "/") {
			return "", "", ErrArtifactUnavailable
		}
		return "", "", nil
	}
	p := strings.Split(id, "/")
	if len(p) != 3 || p[0] == "" || p[1] == "" || p[2] == "" {
		return "", "", ErrArtifactUnavailable
	}
	return p[0], p[1], nil
}

func artifactBusyTx(ctx context.Context, t *sql.Tx, kind, id string, deleted bool) error {
	var busy bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_state WHERE kind=? AND id=? AND (state IN ('deleting','uncertain') OR (? AND state='deleted')))`, kind, id, deleted).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return ErrArtifactUnavailable
	}
	return nil
}

// BeginArtifact reserves a file upload or a whole-token seal. Different files
// may upload concurrently; a seal and GC require every file upload to quiesce.
func (s *Store) BeginArtifact(ctx context.Context, kind, id, path, mode, holder string) (ArtifactOp, error) {
	ns, worker, err := artifactScope(kind, id)
	if err != nil {
		return ArtifactOp{}, err
	}
	if holder == "" || (mode != "upload" && mode != "seal") || (mode == "seal" && kind != "asset") {
		return ArtifactOp{}, ErrArtifactUnavailable
	}
	if mode == "seal" {
		path = ""
	} else if kind == "asset" && (path == "" || strings.HasPrefix(path, "/") || strings.Contains(path, "..") || strings.Contains(path, "\\")) {
		return ArtifactOp{}, ErrArtifactUnavailable
	}
	c, err := s.cell(ctx)
	if err != nil {
		return ArtifactOp{}, err
	}
	op := ArtifactOp{kind, id, path, mode, rand.Text(), holder}
	err = tx(ctx, c, func(t *sql.Tx) error {
		if kind == "asset" {
			if err := checkPurgeFenceTx(ctx, t, ns, worker); err != nil {
				return err
			}
		}
		if err := artifactBusyTx(ctx, t, kind, id, false); err != nil {
			return err
		}
		var busy bool
		if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_ops WHERE kind=? AND id=? AND (path=? OR mode='seal' OR ?='seal'))`, kind, id, path, mode).Scan(&busy); err != nil {
			return err
		}
		if busy {
			return ErrArtifactUnavailable
		}
		_, err := t.ExecContext(ctx, `INSERT INTO artifact_ops(kind,id,path,mode,op,holder,ns,worker) VALUES(?,?,?,?,?,?,?,?)`, kind, id, path, mode, op.Op, holder, ns, worker)
		return err
	})
	return op, err
}

func matchArtifactOp(ctx context.Context, t *sql.Tx, op ArtifactOp) error {
	if op.Op == "" || op.Holder == "" {
		return ErrArtifactUnavailable
	}
	var found bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_ops WHERE kind=? AND id=? AND path=? AND mode=? AND op=? AND holder=?)`, op.Kind, op.ID, op.Path, op.Mode, op.Op, op.Holder).Scan(&found); err != nil {
		return err
	}
	if !found {
		return ErrArtifactUnavailable
	}
	return nil
}

// FinishArtifact releases only this executor's known-complete I/O. For an
// unknown upload error/timeout it must NOT be called. A read-only seal or a
// definite ConditionalCreate precondition failure may abort with success=false.
// A successful seal supplies the verified immutable manifest as sha.
func (s *Store) FinishArtifact(ctx context.Context, op ArtifactOp, sha string, size int64, success bool) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if err := matchArtifactOp(ctx, t, op); err != nil {
			return err
		}
		ns, worker, err := artifactScope(op.Kind, op.ID)
		if err != nil {
			return err
		}
		if success {
			if op.Mode == "seal" {
				var manifest map[string]artifacts.File
				if json.Unmarshal([]byte(sha), &manifest) != nil || manifest == nil {
					return ErrArtifactUnavailable
				}
				var prior string
				err := t.QueryRowContext(ctx, `SELECT manifest FROM asset_manifests WHERE id=?`, op.ID).Scan(&prior)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil && prior != sha {
					return ErrArtifactUnavailable
				}
				if errors.Is(err, sql.ErrNoRows) {
					parts := strings.Split(op.ID, "/")
					if artifacts.ManifestToken(manifest) != parts[2] {
						if err := artifactBusyTx(ctx, t, "asset", op.ID, true); err != nil {
							if errors.Is(err, ErrArtifactUnavailable) {
								return fmt.Errorf("%w: deleted legacy token has no trusted original manifest; publish a new token", err)
							}
							return err
						}
					}
					if _, err := t.ExecContext(ctx, `INSERT INTO asset_manifests(id,manifest) VALUES(?,?)`, op.ID, sha); err != nil {
						return err
					}
				}
			}
			if op.Kind == "bundle" || op.Mode == "seal" {
				if _, err := t.ExecContext(ctx, `DELETE FROM artifact_state WHERE kind=? AND id=? AND state='deleted'`, op.Kind, op.ID); err != nil {
					return err
				}
			}
			if op.Kind == "asset" && op.Mode == "upload" {
				if _, err := t.ExecContext(ctx, `INSERT INTO artifact_receipts(id,path,sha,size,ns,worker) VALUES(?,?,?,?,?,?) ON CONFLICT(id,path) DO UPDATE SET sha=excluded.sha,size=excluded.size`, op.ID, op.Path, sha, size, ns, worker); err != nil {
					return err
				}
			}
			if _, err := t.ExecContext(ctx, `INSERT INTO gc_marks(kind,id,marked_ms) VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET marked_ms=excluded.marked_ms`, op.Kind, op.ID, s.now().UnixMilli()); err != nil {
				return err
			}
		}
		_, err = t.ExecContext(ctx, `DELETE FROM artifact_ops WHERE op=? AND holder=?`, op.Op, op.Holder)
		return err
	})
}

func checkArtifactReferenceTx(ctx context.Context, t *sql.Tx, kind, id string) error {
	if id == "" {
		return nil
	}
	ns, worker, err := artifactScope(kind, id)
	if err != nil {
		return err
	}
	if err := artifactBusyTx(ctx, t, kind, id, true); err != nil {
		return err
	}
	var busy bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_ops WHERE kind=? AND id=?)`, kind, id).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return ErrArtifactUnavailable
	}
	if kind == "asset" {
		if err := checkPurgeFenceTx(ctx, t, ns, worker); err != nil {
			return err
		}
	}
	return nil
}

// ArtifactReceipts are hashes of known-successful uploads, used by the CF
// adapter to verify content without guessing its client's private hash format.
func (s *Store) ArtifactReceipts(ctx context.Context, id string) (map[string]ArtifactReceipt, error) {
	if _, _, err := artifactScope("asset", id); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT path,sha,size FROM artifact_receipts WHERE id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ArtifactReceipt{}
	for rows.Next() {
		var p string
		var r ArtifactReceipt
		if err := rows.Scan(&p, &r.SHA256, &r.Size); err != nil {
			return nil, err
		}
		out[p] = r
	}
	return out, rows.Err()
}

func (s *Store) ClaimArtifactGC(ctx context.Context, kind, id, holder string, grace time.Duration) (ArtifactOp, bool, error) {
	ns, worker, err := artifactScope(kind, id)
	if err != nil {
		return ArtifactOp{}, false, err
	}
	if holder == "" {
		return ArtifactOp{}, false, ErrArtifactUnavailable
	}
	c, err := s.cell(ctx)
	if err != nil {
		return ArtifactOp{}, false, err
	}
	op := ArtifactOp{Kind: kind, ID: id, Mode: "delete", Op: rand.Text(), Holder: holder}
	claimed := false
	err = tx(ctx, c, func(t *sql.Tx) error {
		if kind == "asset" {
			if err := checkPurgeFenceTx(ctx, t, ns, worker); err != nil {
				return err
			}
		}
		if err := artifactBusyTx(ctx, t, kind, id, false); err != nil {
			return err
		}
		var busy, referenced bool
		if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_ops WHERE kind=? AND id=?)`, kind, id).Scan(&busy); err != nil {
			return err
		}
		if busy {
			return nil
		}
		if kind == "bundle" {
			err = t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM version_bundle_refs WHERE sha=?)`, id).Scan(&referenced)
		} else {
			p := strings.Split(id, "/")
			err = t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM versions WHERE ns=? AND worker=? AND assets_sha=?)`, ns, worker, p[2]).Scan(&referenced)
		}
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}
		var marked int64
		err = t.QueryRowContext(ctx, `SELECT marked_ms FROM gc_marks WHERE kind=? AND id=?`, kind, id).Scan(&marked)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if s.now().UnixMilli()-marked < grace.Milliseconds() {
			return nil
		}
		if _, err := t.ExecContext(ctx, `INSERT INTO artifact_state(kind,id,ns,worker,state,op,holder) VALUES(?,?,?,?,'deleting',?,?) ON CONFLICT(kind,id) DO UPDATE SET state='deleting',op=excluded.op,holder=excluded.holder,last_error=''`, kind, id, ns, worker, op.Op, holder); err != nil {
			return err
		}
		if kind == "asset" {
			if _, err := t.ExecContext(ctx, `DELETE FROM artifact_receipts WHERE id=?`, id); err != nil {
				return err
			}
		}
		claimed = true
		return nil
	})
	return op, claimed, err
}

func (s *Store) FinishArtifactGC(ctx context.Context, op ArtifactOp, cause error) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		state, msg := "deleted", ""
		if cause != nil {
			state, msg = "uncertain", cause.Error()
		}
		result, err := t.ExecContext(ctx, `UPDATE artifact_state SET state=?,last_error=? WHERE kind=? AND id=? AND op=? AND holder=? AND state='deleting'`, state, msg, op.Kind, op.ID, op.Op, op.Holder)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrArtifactUnavailable
		}
		if cause == nil {
			_, err = t.ExecContext(ctx, `DELETE FROM gc_marks WHERE kind=? AND id=?`, op.Kind, op.ID)
		}
		return err
	})
}

func fencePurgeArtifactsTx(ctx context.Context, t *sql.Tx, ns, worker, generation string) error {
	var busy bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_ops WHERE ns=? AND (?='' OR worker=?)) OR EXISTS(SELECT 1 FROM artifact_state WHERE ns=? AND (?='' OR worker=?) AND state IN ('deleting','uncertain'))`, ns, worker, worker, ns, worker, worker).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return ErrPurgeInProgress
	}
	if _, err := t.ExecContext(ctx, `INSERT OR IGNORE INTO artifact_state(kind,id,ns,worker,state,op,holder)
	 SELECT 'asset',id,ns,worker,'deleted',?,'' FROM (
	 SELECT id,ns,worker FROM artifact_receipts WHERE ns=? AND (?='' OR worker=?)
	 UNION SELECT ns||'/'||worker||'/'||assets_sha,ns,worker FROM versions WHERE ns=? AND (?='' OR worker=?) AND assets_sha<>'')`, generation, ns, worker, worker, ns, worker, worker); err != nil {
		return err
	}
	_, err := t.ExecContext(ctx, `DELETE FROM artifact_receipts WHERE ns=? AND (?='' OR worker=?)`, ns, worker, worker)
	return err
}

// FencePurgeAssetKeys adds exact tombstones for legacy/unregistered objects
// discovered by the purge's existing cold listing. It never infers a manifest.
// The current executor's durable running fence excludes all overlapping I/O.
func (s *Store) FencePurgeAssetKeys(ctx context.Context, ns, worker, holder string, keys []string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var job, op string
		if err := t.QueryRowContext(ctx, `SELECT job_id,op_id FROM purges WHERE ns=? AND worker=? AND holder=? AND state='running'`, ns, worker, holder).Scan(&job, &op); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPurgeInProgress
			}
			return err
		}
		if job == "" || op == "" || holder == "" {
			return ErrPurgeInProgress
		}
		seen := map[string]bool{}
		for _, key := range keys {
			p := strings.SplitN(key, "/", 5)
			if len(p) != 5 || p[0] != "assets" || p[1] != ns || (worker != "" && p[2] != worker) {
				return ErrPurgeInProgress
			}
			id := p[1] + "/" + p[2] + "/" + p[3]
			if seen[id] {
				continue
			}
			seen[id] = true
			if _, _, err := artifactScope("asset", id); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx, `INSERT OR IGNORE INTO artifact_state(kind,id,ns,worker,state,op,holder) VALUES('asset',?,?,?,'deleted',?,?)`, id, ns, p[2], op, holder); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReclamationStatus exposes blocking records without execution credentials.
func (s *Store) ReclamationStatus(ctx context.Context) ([]map[string]string, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT 'purge',ns||'/'||worker,state,last_error FROM purges WHERE state NOT IN ('pending','failed') UNION ALL SELECT kind,id,state,last_error FROM artifact_state WHERE state IN ('deleting','uncertain') UNION ALL SELECT kind,id,'upload-or-seal','' FROM artifact_ops LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]string{}
	for rows.Next() {
		var k, id, state, msg string
		if err := rows.Scan(&k, &id, &state, &msg); err != nil {
			return nil, err
		}
		out = append(out, map[string]string{"kind": k, "id": id, "state": state, "error": msg})
	}
	return out, rows.Err()
}
