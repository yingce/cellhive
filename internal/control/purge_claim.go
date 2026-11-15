package control

import (
	"context"
	"crypto/rand"
	"database/sql"
)

// ClaimPurge only records exclusion. The coordinator must durably replicate
// this transaction before performing any external I/O. There is no TTL takeover.
func (s *Store) ClaimPurge(ctx context.Context, candidate Purge, holder string) (Purge, error) {
	if holder == "" {
		return Purge{}, ErrPurgeInProgress
	}
	c, err := s.cell(ctx)
	if err != nil {
		return Purge{}, err
	}
	j := candidate
	j.OpID, j.Holder, j.State = rand.Text(), holder, "running"
	err = tx(ctx, c, func(t *sql.Tx) error {
		if err := checkPurgeFenceTx(ctx, t, j.Namespace, j.Worker); err != nil {
			return err
		}
		var deleted bool
		query, args := `SELECT EXISTS(SELECT 1 FROM apps WHERE ns=? AND deleted_ms>0)`, []any{j.Namespace}
		if j.Worker != "" {
			query, args = `SELECT EXISTS(SELECT 1 FROM workers WHERE ns=? AND name=? AND deleted_ms>0)`, []any{j.Namespace, j.Worker}
		}
		if err := t.QueryRowContext(ctx, query, args...).Scan(&deleted); err != nil {
			return err
		}
		if !deleted {
			return ErrPurgeInProgress
		}
		if j.JobID == "" {
			j.JobID = rand.Text()
		}
		res, err := t.ExecContext(ctx, `UPDATE purges SET state='running', job_id=?, op_id=?, holder=?, updated_ms=?
			WHERE ns=? AND worker=? AND job_id=? AND requested_ms=? AND state IN ('pending','failed')`,
			j.JobID, j.OpID, j.Holder, s.now().UnixMilli(), j.Namespace, j.Worker, candidate.JobID, candidate.RequestedMs)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrPurgeInProgress
		}
		return fencePurgeArtifactsTx(ctx, t, j.Namespace, j.Worker, j.JobID)
	})
	return j, err
}

// CompletePurge is called only by the original executor after every synchronous
// deletion has a known successful result. Never use it to recover uncertain I/O.
func (s *Store) CompletePurge(ctx context.Context, j Purge) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if err := matchPurgeClaim(ctx, t, j); err != nil {
			return err
		}
		if j.Worker == "" {
			if err := purgeAppRowsTx(ctx, t, j.Namespace); err != nil {
				return err
			}
		} else if err := purgeWorkerRowsTx(ctx, t, j.Namespace, j.Worker); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM purges WHERE ns=? AND worker=? AND job_id=? AND op_id=? AND holder=?`, j.Namespace, j.Worker, j.JobID, j.OpID, j.Holder); err != nil {
			return err
		}
		return txAudit(ctx, t, j.Namespace, "purge", "purge.done", j.JobID)
	})
}

func matchPurgeClaim(ctx context.Context, t *sql.Tx, j Purge) error {
	if j.JobID == "" || j.OpID == "" || j.Holder == "" {
		return ErrPurgeInProgress
	}
	var matches bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM purges WHERE ns=? AND worker=? AND job_id=? AND op_id=? AND holder=? AND state='running')`, j.Namespace, j.Worker, j.JobID, j.OpID, j.Holder).Scan(&matches); err != nil {
		return err
	}
	if !matches {
		return ErrPurgeInProgress
	}
	return nil
}

// HoldPurge records an unknown external outcome without making the job runnable.
func (s *Store) HoldPurge(ctx context.Context, j Purge, cause error) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if err := matchPurgeClaim(ctx, t, j); err != nil {
			return err
		}
		msg := "external outcome unknown"
		if cause != nil {
			msg = cause.Error()
		}
		_, err := t.ExecContext(ctx, `UPDATE purges SET state='uncertain', last_error=?, updated_ms=? WHERE ns=? AND worker=? AND job_id=? AND op_id=? AND holder=?`, msg, s.now().UnixMilli(), j.Namespace, j.Worker, j.JobID, j.OpID, j.Holder)
		return err
	})
}
