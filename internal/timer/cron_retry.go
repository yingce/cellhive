package timer

import (
	"context"
	"database/sql"
	"sync"
)

const CronMaxFailures = 6

type cronMemoryKey struct{}
type cronRetry struct {
	timer   Timer
	attempt int
	cutoff  int64
}
type cronMemory struct {
	// Serializes dispatch passes for this resident cell, including runner/waker.
	mu   sync.Mutex
	owed map[string]cronRetry
}

func (s *Store) cronMemory() *cronMemory {
	v, _ := s.cell.Ephemeral.LoadOrStore(cronMemoryKey{}, &cronMemory{owed: make(map[string]cronRetry)})
	return v.(*cronMemory)
}

// retryCron runs under the resident dispatch lock. Only a wake marker survives
// a crash: the expression, original timestamp and counter stay in memory.
func (s *Store) retryCron(ctx context.Context, t Timer, now, expires int64, mem *cronMemory, write Commit) (int64, int, bool, error) {
	prior := mem.owed[t.Token]
	attempt := prior.attempt + 1
	cutoff := prior.cutoff
	if s.CronNext != nil && cutoff == 0 {
		var err error
		cutoff, err = s.CronNext(ctx, t.Scope, now)
		if err != nil {
			return 0, 0, false, err
		}
	}
	retryAt := now + (int64(2000) << attempt)
	terminal := attempt >= CronMaxFailures || (cutoff > 0 && retryAt >= cutoff)
	err := write(ctx, func() error {
		if terminal {
			return s.MarkFired(ctx, t.Token, expires)
		}
		_, err := s.cell.Tx(ctx, func(tx *sql.Tx) error {
			// Retain token only for deduplication, not enough information to replay.
			result, err := tx.ExecContext(ctx, `UPDATE timers SET due_ms=?, occurrence='' WHERE token=?`, retryAt, t.Token)
			if err != nil {
				return err
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if changed != 1 {
				return sql.ErrNoRows
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO timer_retry(token,attempt,retry_after_ms) VALUES(?,-1,?) ON CONFLICT(token) DO UPDATE SET attempt=-1,retry_after_ms=excluded.retry_after_ms`, t.Token, retryAt)
			return err
		})
		if err == nil {
			_ = s.syncIndex(ctx)
		}
		return err
	})
	if err != nil {
		return 0, 0, false, err
	}
	if terminal {
		delete(mem.owed, t.Token)
	} else {
		mem.owed[t.Token] = cronRetry{timer: t, attempt: attempt, cutoff: cutoff}
	}
	return retryAt, attempt, terminal, nil
}
