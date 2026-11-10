package server

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
)

const maxCellImportBytes int64 = 64 << 30

var cellTransfers = make(chan struct{}, 2)

// Bound memory while serializing same-scope transfers through owner release.
var cellTransferLocks = func() [256]chan struct{} {
	var locks [256]chan struct{}
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
	}
	return locks
}()

func beginCellTransfer(ctx context.Context) (func(), error) {
	select {
	case cellTransfers <- struct{}{}:
		return func() { <-cellTransfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

var errImportTargetExists = errors.New("import target must be a new cell without local or replicated data")

func (s *Server) transferOwner(w http.ResponseWriter, r *http.Request) (cell.Scope, uint64, bool) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, 400, "bad_scope", err.Error())
		return sc, 0, false
	}
	if s.Store == nil || s.Owner == nil || s.Replica == nil {
		writeErr(w, 503, "no_storage", "storage and owner manager required")
		return sc, 0, false
	}
	if r.URL.Path == "/v1/control/cell/import" {
		_, err = s.Owner.Claim(r.Context(), sc, time.Now())
	} else {
		err = s.claimForRead(r.Context(), sc)
	}
	if err != nil {
		writeErr(w, 409, "not_owner", err.Error())
		return sc, 0, false
	}
	o, _, err := s.Owner.Resolve(r.Context(), sc)
	if err != nil {
		writeErr(w, 409, "not_owner", err.Error())
		return sc, 0, false
	}
	if err = s.verifyTransferOwner(r.Context(), sc, o.Epoch); err != nil {
		writeErr(w, 409, "not_owner", err.Error())
		return sc, 0, false
	}
	return sc, o.Epoch, true
}

func (s *Server) handleCellExport(w http.ResponseWriter, r *http.Request) {
	unlock, lockErr := lockCellTransfer(r.Context(), r.URL.Query().Get("scope"))
	if lockErr != nil {
		writeErr(w, 503, "transfer_busy", lockErr.Error())
		return
	}
	defer unlock()
	releaseBudget, budgetErr := beginCellTransfer(r.Context())
	if budgetErr != nil {
		writeErr(w, 503, "transfer_busy", budgetErr.Error())
		return
	}
	defer releaseBudget()
	sc, epoch, ok := s.transferOwner(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	releaseOwner := s.releaseTransferOwner(ctx, sc, epoch)
	defer func() { _ = releaseOwner() }()
	dir, err := os.MkdirTemp("", "cellhive-export-")
	if err != nil {
		writeErr(w, 500, "export_failed", err.Error())
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "image.sqlite")
	// Resident SQLite includes committed writes whose fleet upload may still be queued.
	c, release, err := s.Store.AcquireResident(ctx, sc)
	if !sc.IsReservedClass() || sc.Class == DOClass {
		// A local generic activation cannot represent workerd's SQLite file.
		c = nil
	}
	if err == nil {
		if c != nil {
			err = c.Snapshot(ctx, path)
		} else {
			if s.Uploader != nil {
				err = s.Uploader.Flush(ctx)
			}
			if err == nil {
				var pf *replica.PageFetcher
				pf, err = s.Replica.NewPageFetcher(ctx, sc, transferDataEpoch(sc, epoch))
				if err == nil {
					_, err = pf.Materialize(ctx, path, nil)
					_ = pf.Close()
				}
			}
		}
		release()
	}
	if err == nil {
		err = checkSQLite(ctx, path)
	}
	if err == nil {
		err = s.verifyTransferOwner(ctx, sc, epoch)
	}
	if err != nil {
		writeErr(w, 500, "export_failed", err.Error())
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeErr(w, 500, "export_failed", err.Error())
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		writeErr(w, 500, "export_failed", err.Error())
		return
	}
	if err := releaseOwner(); err != nil {
		writeErr(w, 503, "export_handoff_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	w.Header().Set("Content-Disposition", `attachment; filename="cell.sqlite"`)
	w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	_, _ = io.Copy(w, f)
}

func checkSQLite(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	var header [16]byte
	_, err = io.ReadFull(f, header[:])
	_ = f.Close()
	if err != nil || string(header[:]) != "SQLite format 3\x00" {
		return errors.New("expected a complete SQLite database file")
	}
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open(cellstore.DriverName, u.String()+"?mode=ro&immutable=1&_query_only=1")
	if err != nil {
		return err
	}
	defer db.Close()
	var result string
	if err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("SQLite integrity check: %s", result)
	}
	return nil
}

func (s *Server) verifyTransferOwner(ctx context.Context, sc cell.Scope, epoch uint64) error {
	if err := s.Replica.RefreshResourceSnapshot(ctx, sc); err != nil {
		return err
	}
	blocked, err := s.Replica.ResourceBlocked(ctx, sc)
	if err != nil {
		return err
	}
	if blocked {
		return replica.ErrResourceDeleted
	}
	o, _, err := s.Owner.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if o.Node != s.Cfg.NodeID || o.Session != s.Owner.Session || !s.ownerLive(ctx, o, time.Now()) {
		return owner.ErrNotOwner
	}
	if o.Epoch != epoch {
		return owner.ErrEpochMismatch
	}
	return nil
}

func (s *Server) handleCellImport(w http.ResponseWriter, r *http.Request) {
	target, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil || target.Class == DOClass {
		writeErr(w, 400, "bad_scope", "import requires a data scope, not a DO shard owner scope")
		return
	}
	// Resolve storage identity only on the authoritative control owner. Forward
	// before consuming the request body or acquiring any transfer/shard locks.
	if !target.IsReservedClass() && s.Control != nil && s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	unlock, lockErr := lockCellTransfer(r.Context(), r.URL.Query().Get("scope"))
	if lockErr != nil {
		writeErr(w, 503, "transfer_busy", lockErr.Error())
		return
	}
	defer unlock()
	releaseBudget, budgetErr := beginCellTransfer(r.Context())
	if budgetErr != nil {
		writeErr(w, 503, "transfer_busy", budgetErr.Error())
		return
	}
	defer releaseBudget()
	shardEpoch, verifyShard, releaseShard, err := s.acquireImportShardEpoch(r.Context(), target)
	if err != nil {
		writeErr(w, 409, "import_shard_unavailable", err.Error())
		return
	}
	defer func() { _ = releaseShard() }()
	sc, epoch, ok := s.transferOwner(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	releaseOwner := s.releaseTransferOwner(ctx, sc, epoch)
	defer func() { _ = releaseOwner() }()
	dir, err := os.MkdirTemp("", "cellhive-import-")
	if err != nil {
		writeErr(w, 500, "import_failed", err.Error())
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "image.sqlite")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		writeErr(w, 500, "import_failed", err.Error())
		return
	}
	_, err = io.Copy(f, http.MaxBytesReader(w, r.Body, maxCellImportBytes))
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = checkSQLite(ctx, path)
	}
	if err != nil {
		writeErr(w, 400, "invalid_sqlite", err.Error())
		return
	}
	dataEpoch := epoch
	if !sc.IsReservedClass() {
		dataEpoch = shardEpoch
	}
	spool := filepath.Join(dir, "snapshot.ltx")
	watermark, err := encodeImportedSQLite(ctx, path, spool, !sc.IsReservedClass())
	if err != nil {
		writeErr(w, 400, "invalid_sqlite", err.Error())
		return
	}
	st, err := os.Stat(spool)
	if err != nil {
		writeErr(w, 500, "import_failed", err.Error())
		return
	}
	// The Store gate excludes generic activations. Native DO activation is
	// excluded separately by the shard owner held from before body ingestion.
	published := false
	err = s.Store.ForgetWithVerify(ctx, sc, func() error {
		if s.Store.IsOpen(sc) {
			return errImportTargetExists
		}
		local, e := s.Store.Path(sc)
		if e != nil {
			return e
		}
		if _, e = os.Stat(local); e == nil {
			return errImportTargetExists
		} else if !os.IsNotExist(e) {
			return e
		}
		if e = verifyShard(); e != nil {
			return e
		}
		if e = s.verifyTransferOwner(ctx, sc, epoch); e != nil {
			return e
		}
		pf, e := s.Replica.NewPageFetcher(ctx, sc, dataEpoch)
		if e == nil {
			_ = pf.Close()
			return errImportTargetExists
		}
		if !errors.Is(e, replica.ErrNoPageIndex) {
			return e
		}
		if _, _, e = s.Replica.AppendSnapshotFile(ctx, sc, dataEpoch, spool, st.Size()); e != nil {
			return e
		}
		if e = verifyShard(); e != nil {
			return e
		}
		if e = s.verifyTransferOwner(ctx, sc, epoch); e != nil {
			return e
		}
		published = true
		return nil
	})
	if err == nil && !published {
		err = errors.New("target is pinned and cannot be imported")
	}
	if err != nil {
		status := 500
		if errors.Is(err, errImportTargetExists) {
			status = 409
		}
		writeErr(w, status, "import_failed", err.Error())
		return
	}
	if err := releaseOwner(); err != nil {
		writeErr(w, 503, "import_handoff_failed", err.Error())
		return
	}
	if err := releaseShard(); err != nil {
		writeErr(w, 503, "import_handoff_failed", err.Error())
		return
	}
	s.enqueuePendingTimers(sc)
	writeJSON(w, 200, map[string]any{"scope": sc.String(), "epoch": dataEpoch, "owner_epoch": epoch, "txid": watermark, "imported": true})
}

func encodeImportedSQLite(ctx context.Context, path, spool string, nativeDO bool) (uint64, error) {
	c, err := cellstore.OpenAt(ctx, path)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	f, err := os.OpenFile(spool, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	out := bufio.NewWriterSize(f, 256<<10)
	var enc *ltx.NativeEncoder
	wm, _, _, err := c.SnapshotStream(ctx, ltx.DefaultSnapshotPartBytes, func(_, _ int, watermark uint64, ps, commit, first uint32, pages [][]byte) error {
		if enc == nil {
			if nativeDO {
				watermark = 1
			}
			var e error
			enc, e = ltx.NewNativeEncoder(out, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 1, MaxTXID: watermark, Timestamp: time.Now().UnixMilli()})
			if e != nil {
				return e
			}
		}
		for i, page := range pages {
			pg := first + uint32(i)
			if pg == uint32(0x40000000)/ps+1 {
				continue
			}
			if e := enc.WritePage(ltx.WALPage{PageNo: pg, Data: page}); e != nil {
				return e
			}
		}
		return nil
	})
	if err == nil && enc == nil {
		err = errors.New("empty SQLite image")
	}
	if err == nil {
		err = enc.Close()
	}
	if err == nil {
		err = out.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if nativeDO {
		wm = 1
	}
	return wm, err
}

// Transfer ownership is temporary for DO shards. Once the complete SQLite
// image is durable (import) or local (export), native runtime may claim it.
func (s *Server) releaseTransferOwner(ctx context.Context, sc cell.Scope, epoch uint64) func() error {
	released := sc.Class != "__do__"
	return func() error {
		if released {
			return nil
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		err := s.Owner.Release(releaseCtx, sc, epoch)
		if err == nil {
			released = true
		}
		return err
	}
}

func lockCellTransfer(ctx context.Context, scope string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(scope))
	lock := cellTransferLocks[hash.Sum32()%uint32(len(cellTransferLocks))]
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// acquireImportShard uses the existing native runtime owner as a distributed
// import barrier. No new object-store record or LTX format is needed.
func (s *Server) acquireImportShard(ctx context.Context, sc cell.Scope) (func() error, func() error, error) {
	_, verify, release, err := s.acquireImportShardEpoch(ctx, sc)
	return verify, release, err
}

func (s *Server) acquireImportShardEpoch(ctx context.Context, sc cell.Scope) (dataEpoch uint64, verify func() error, release func() error, err error) {
	noop := func() error { return nil }
	if sc.IsReservedClass() {
		return 0, noop, noop, nil
	}
	if s.Owner == nil || s.Replica == nil {
		return 0, nil, nil, errors.New("storage and owner manager required")
	}
	raw, err := base64.RawURLEncoding.DecodeString(sc.ID)
	if err != nil {
		return 0, nil, nil, errors.New("invalid native DO storage identity")
	}
	parts := strings.SplitN(string(raw), "/", 4)
	if len(parts) != 4 || parts[0] == "" || parts[1] != sc.Class || parts[2] != sc.Class || parts[3] == "" || base64.RawURLEncoding.EncodeToString(raw) != sc.ID {
		return 0, nil, nil, errors.New("invalid native DO storage identity")
	}
	p, err := s.projectionStrict(ctx)
	if err != nil {
		return 0, nil, nil, err
	}
	worker := ""
	for _, app := range p.Apps {
		if app.Namespace != sc.Namespace {
			continue
		}
		for _, w := range app.Workers {
			if w.Version.StorageID != parts[0] {
				continue
			}
			if worker != "" {
				return 0, nil, nil, errors.New("ambiguous native DO storage identity")
			}
			worker = w.Worker
		}
	}
	if worker == "" {
		return 0, nil, nil, errors.New("native DO storage identity is not deployed in this namespace")
	}
	shard := owner.DOScope(sc.Namespace, worker, sc.Class, cell.DOShard(sc.Namespace, worker, sc.Class, parts[3]))
	if err := shard.Validate(); err != nil {
		return 0, nil, nil, err
	}
	o, unlock, err := s.Owner.ClaimExclusive(ctx, shard, time.Now())
	if err != nil {
		return 0, nil, nil, err
	}
	release = func() error {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			err = unlock(cleanup)
			if err == nil || errors.Is(err, owner.ErrEpochMismatch) || cleanup.Err() != nil {
				return err
			}
		}
		return err
	}
	verify = func() error { return s.verifyTransferOwner(ctx, shard, o.Epoch) }
	if err = verify(); err != nil {
		_ = release()
		return 0, nil, nil, err
	}
	return o.Epoch, verify, release, nil
}

func transferDataEpoch(sc cell.Scope, ownerEpoch uint64) uint64 {
	if !sc.IsReservedClass() {
		return ^uint64(0)
	}
	return ownerEpoch
}

// RunCellTransferCleanupLoop retries failed temporary shard releases even when
// idle eviction and rebalancing are disabled.
func (s *Server) RunCellTransferCleanupLoop(ctx context.Context) {
	if s.Owner == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pass, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := s.Owner.RetryExclusiveReleases(pass)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.Log.Warn("cell transfer owner cleanup failed", "err", err)
		}
	}
}
