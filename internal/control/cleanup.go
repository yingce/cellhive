package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cellhive/internal/cell"
)

var ErrCleanupPending = errors.New("control: resource data cleanup is pending")
var ErrResourceScopeShared = errors.New("control: resource data scope is still owned by another registered resource")
var ErrCleanupStale = errors.New("control: stale cleanup lease")

type ResourceCleanup struct {
	Namespace        string   `json:"namespace"`
	Kind             string   `json:"kind"`
	Name             string   `json:"name"`
	JobID            string   `json:"job_id"`
	DataScope        string   `json:"data_scope"`
	SourceGeneration string   `json:"source_generation"`
	DeadLetters      []string `json:"dead_letter_queues"`
	State            string   `json:"state"`
	RequestedMs      int64    `json:"requested_ms"`
	UpdatedMs        int64    `json:"updated_ms"`
	Attempts         int      `json:"attempts"`
	LastError        string   `json:"last_error,omitempty"`
	NextRetryMs      int64    `json:"next_retry_ms,omitempty"`
	LeaseToken       string   `json:"-"`
	LeaseUntilMs     int64    `json:"-"`
	RetiredEpoch     uint64   `json:"-"`
}

// ResourceDataScope matches the data-plane identity. KV may use a bare ID or
// explicit scope; Queue resources use their queue name in the namespace.
func ResourceDataScope(ns, kind, name, ref string) (cell.Scope, error) {
	sc := cell.Scope{Namespace: ns, Class: "__" + kind + "__", ID: name}
	if kind == "kv" && ref != "" {
		if parsed, err := cell.ParseScope(ref); err == nil {
			sc = parsed
		} else {
			sc.ID = ref
		}
	}
	if sc.Namespace != ns || sc.Class != "__"+kind+"__" {
		return sc, fmt.Errorf("control: resource scope crosses namespace/kind")
	}
	return sc, sc.Validate()
}

const cleanupColumns = `ns,kind,name,job_id,data_scope,source_generation,dead_letters,state,requested_ms,updated_ms,attempts,last_error,next_retry_ms,lease_token,lease_until_ms`

func scanCleanup(row interface{ Scan(...any) error }) (ResourceCleanup, error) {
	var j ResourceCleanup
	var dlqs string
	err := row.Scan(&j.Namespace, &j.Kind, &j.Name, &j.JobID, &j.DataScope, &j.SourceGeneration, &dlqs, &j.State, &j.RequestedMs, &j.UpdatedMs, &j.Attempts, &j.LastError, &j.NextRetryMs, &j.LeaseToken, &j.LeaseUntilMs)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	if err != nil {
		return j, err
	}
	if err := json.Unmarshal([]byte(dlqs), &j.DeadLetters); err != nil {
		return j, err
	}
	return j, nil
}
func (s *Store) ResourceCleanup(ctx context.Context, ns, kind, name string) (ResourceCleanup, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return ResourceCleanup{}, err
	}
	return scanCleanup(c.DB.QueryRowContext(ctx, `SELECT `+cleanupColumns+` FROM resource_cleanups WHERE ns=? AND kind=? AND name=?`, ns, kind, name))
}

func (s *Store) PendingResourceCleanups(ctx context.Context, limit int) ([]ResourceCleanup, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	now := s.now().UnixMilli()
	rows, err := c.DB.QueryContext(ctx, `SELECT `+cleanupColumns+` FROM resource_cleanups WHERE ((state IN ('pending','failed') AND next_retry_ms<=?) OR (state='running' AND lease_until_ms<=?)) AND attempts<10 ORDER BY requested_ms LIMIT ?`, now, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ResourceCleanup{}
	for rows.Next() {
		j, err := scanCleanup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) ClaimResourceCleanup(ctx context.Context, j ResourceCleanup) (ResourceCleanup, bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return j, false, err
	}
	now := s.now().UnixMilli()
	token := newStorageID()
	claimed := false
	err = tx(ctx, c, func(t *sql.Tx) error {
		result, err := t.ExecContext(ctx, `UPDATE resource_cleanups SET state='running',attempts=attempts+1,updated_ms=?,lease_token=?,lease_until_ms=? WHERE ns=? AND kind=? AND name=? AND job_id=? AND attempts<10 AND ((state IN ('pending','failed') AND next_retry_ms<=?) OR (state='running' AND lease_until_ms<=?))`, now, token, now+120000, j.Namespace, j.Kind, j.Name, j.JobID, now, now)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		claimed = n == 1
		return err
	})
	if err != nil || !claimed {
		return j, false, err
	}
	j, err = s.ResourceCleanup(ctx, j.Namespace, j.Kind, j.Name)
	return j, true, err
}
func (s *Store) FinishResourceCleanup(ctx context.Context, j ResourceCleanup, cause error) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	state := "complete"
	message := ""
	next := int64(0)
	if cause != nil {
		state = "failed"
		message = cause.Error()
		if len(message) > 4096 {
			message = message[:4096]
		}
		shift := j.Attempts
		if shift > 6 {
			shift = 6
		}
		next = s.now().Add(time.Duration(1<<shift) * time.Second).UnixMilli()
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		result, err := t.ExecContext(ctx, `UPDATE resource_cleanups SET state=?,updated_ms=?,last_error=?,next_retry_ms=?,lease_token='',lease_until_ms=0 WHERE ns=? AND kind=? AND name=? AND job_id=? AND lease_token=? AND state='running'`, state, s.now().UnixMilli(), message, next, j.Namespace, j.Kind, j.Name, j.JobID, j.LeaseToken)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrCleanupStale
		}
		if cause == nil {
			if _, err := t.ExecContext(ctx, `INSERT INTO resource_retirements(ns,data_scope,job_id,epoch_floor,updated_ms) VALUES(?,?,?,?,?) ON CONFLICT(data_scope) DO UPDATE SET job_id=excluded.job_id,epoch_floor=MAX(epoch_floor,excluded.epoch_floor),gc_cursor='',last_error='',updated_ms=excluded.updated_ms`, j.Namespace, j.DataScope, j.JobID, j.RetiredEpoch, s.now().UnixMilli()); err != nil {
				return err
			}
		}
		return nil
	})
}

// PruneCompletedResourceCleanups removes completed jobs older than beforeMs.
// A completed row is kept so repeated deletes stay idempotent; after a long
// grace period it is safe to retire it and keep the periodic scan bounded.
func (s *Store) PruneCompletedResourceCleanups(ctx context.Context, beforeMs int64, limit int) (int, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	c, err := s.cell(ctx)
	if err != nil {
		return 0, err
	}
	removed := 0
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM resource_cleanups WHERE rowid IN (SELECT rowid FROM resource_cleanups WHERE state='complete' AND updated_ms<? LIMIT ?)`, beforeMs, limit)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		removed = int(n)
		return nil
	})
	return removed, err
}

func (s *Store) RetryResourceCleanup(ctx context.Context, ns, kind, name string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		res, err := t.ExecContext(ctx, `UPDATE resource_cleanups SET state='pending',attempts=0,last_error='',next_retry_ms=0,lease_token='',lease_until_ms=0,updated_ms=? WHERE ns=? AND kind=? AND name=? AND (state IN ('pending','failed') OR (state='running' AND lease_until_ms<=?))`, s.now().UnixMilli(), ns, kind, name, s.now().UnixMilli())
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrCleanupStale
		}
		return nil
	})
}

// A bounded successful data pass is progress, not a failed retry. It must not
// exhaust the failure budget of a resource with more than 500 replica objects.
func (s *Store) ContinueResourceCleanup(ctx context.Context, j ResourceCleanup) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		result, err := t.ExecContext(ctx, `UPDATE resource_cleanups SET state='pending',attempts=MAX(attempts-1,0),lease_token='',lease_until_ms=0,next_retry_ms=0,updated_ms=? WHERE ns=? AND kind=? AND name=? AND job_id=? AND lease_token=? AND state='running'`, s.now().UnixMilli(), j.Namespace, j.Kind, j.Name, j.JobID, j.LeaseToken)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrCleanupStale
		}
		return nil
	})
}

func (s *Store) ValidateResourceCleanupLease(ctx context.Context, j ResourceCleanup) error {
	current, err := s.ResourceCleanup(ctx, j.Namespace, j.Kind, j.Name)
	if err != nil {
		return err
	}
	if current.JobID != j.JobID || current.State != "running" || current.LeaseToken != j.LeaseToken || current.LeaseUntilMs <= s.now().UnixMilli() {
		return ErrCleanupStale
	}
	return nil
}
