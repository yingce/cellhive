package timer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"cellhive/internal/cellstore"
)

// Store persists timers in the owning cell's SQLite. Due records and the fired
// TTL table live in one cell database, so the timer set is copied, recovered,
// and fenced exactly like any other cell state.
type Store struct {
	cell *cellstore.Cell
	// Index, when set, keeps the bucket wake index in sync (ADR-099).
	Index Index
}

const schema = `
CREATE TABLE IF NOT EXISTS timers (
  token      TEXT PRIMARY KEY,
  due_ms     INTEGER NOT NULL,
  kind       TEXT NOT NULL,
  scope      TEXT NOT NULL,
  occurrence TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS timers_due ON timers(due_ms);
CREATE TABLE IF NOT EXISTS timer_retry (
  token          TEXT PRIMARY KEY,
  attempt        INTEGER NOT NULL,
  retry_after_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS fired (
  token      TEXT PRIMARY KEY,
  expires_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS fired_expires ON fired(expires_ms);
`

// InsertTx allows a producer sharing the timer cell to commit its state and
// dispatch outbox atomically. The producer must publish the wake index before
// committing, and register the scope after commit (as Store.Upsert does).
func InsertTx(ctx context.Context, tx *sql.Tx, t Timer) error {
	if !t.Kind.Valid() || t.Token == "" {
		return fmt.Errorf("timer: invalid transactional timer")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO timers(token,due_ms,kind,scope,occurrence) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM fired WHERE token=?) ON CONFLICT(token) DO UPDATE SET due_ms=excluded.due_ms,kind=excluded.kind,scope=excluded.scope,occurrence=excluded.occurrence`, t.Token, t.DueAtMs, string(t.Kind), t.Scope, t.Occurrence, t.Token)
	return err
}

// Index mirrors a scope's earliest pending timer into the bucket wake index so
// the fleet waker can find due work without listing every cell (ADR-099).
type Index interface {
	Put(ctx context.Context, scope string, dueMs int64, kind, token string) error
	Delete(ctx context.Context, scope string) error
}

// NewStore opens (migrating if needed) the timer tables in a cell.
func NewStore(ctx context.Context, c *cellstore.Cell) (*Store, error) {
	// Timer stores are opened on every due poll. Re-running the idempotent DDL
	// inside Cell.Tx would still advance its capture txid on every read.
	var objects int
	err := c.DB.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name IN ('timers','timers_due','timer_retry','fired','fired_expires')`).Scan(&objects)
	if err != nil {
		return nil, fmt.Errorf("timer: check schema: %w", err)
	}
	if objects != 5 {
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, schema)
			return err
		}); err != nil {
			return nil, fmt.Errorf("timer: migrate: %w", err)
		}
	}
	return &Store{cell: c}, nil
}

// NextDue returns the earliest pending timer's due time, or 0 when none.
func (s *Store) NextDue(ctx context.Context) (int64, error) {
	var next sql.NullInt64
	if err := s.cell.DB.QueryRowContext(ctx, `SELECT MIN(CASE WHEN r.retry_after_ms > t.due_ms THEN r.retry_after_ms ELSE t.due_ms END)
		FROM timers t LEFT JOIN timer_retry r ON r.token=t.token`).Scan(&next); err != nil {
		return 0, err
	}
	if !next.Valid {
		return 0, nil
	}
	return next.Int64, nil
}

// syncIndex mirrors the scope's earliest due into the wake index.
func (s *Store) syncIndex(ctx context.Context) error {
	if s.Index == nil {
		return nil
	}
	scope := s.cell.Scope.String()
	next, err := s.NextDue(ctx)
	if err != nil {
		return err
	}
	if next == 0 {
		return s.Index.Delete(ctx, scope)
	}
	return s.Index.Put(ctx, scope, next, "", "")
}

// SyncIndex republishes this scope's earliest due from the authoritative timer
// rows (ADR-177). Call it whenever a node (re)claims or opens a timer scope, so
// an index entry lost to a crash or a bucket failure is rebuilt.
func (s *Store) SyncIndex(ctx context.Context) error { return s.syncIndex(ctx) }

// Upsert inserts or updates a timer row, keyed by token.
func (s *Store) Upsert(ctx context.Context, t Timer) error {
	if !t.Kind.Valid() {
		return fmt.Errorf("timer: invalid kind %q", t.Kind)
	}
	if t.Token == "" {
		return fmt.Errorf("timer: empty token")
	}
	// Publish the wake index BEFORE the row commits (ADR-177). The index may
	// then only be ahead of the timers (a false positive the waker clears),
	// never behind them (a wake nobody discovers). The entry carries the earlier
	// of this timer and any already-pending one, so it cannot hide an earlier
	// timer either.
	if s.Index != nil {
		due := t.DueAtMs
		if cur, err := s.NextDue(ctx); err != nil {
			return fmt.Errorf("timer: read earliest due: %w", err)
		} else if cur > 0 && cur < due {
			due = cur
		}
		if err := s.Index.Put(ctx, s.cell.Scope.String(), due, string(t.Kind), t.Token); err != nil {
			return fmt.Errorf("timer: publish wake index: %w", err)
		}
	}
	// Every cell write goes through Cell.Tx so it advances the cell's txid and
	// the capture watermark covers it before the caller is acknowledged (RPO=0).
	_, err := s.cell.Tx(ctx, func(tx *sql.Tx) error {
		return InsertTx(ctx, tx, t)
	})
	// Correct the entry after the commit, and also on failure (the pre-published
	// entry may name a timer that never committed).
	_ = s.syncIndex(ctx)
	return err
}

// RemoveByOccurrence deletes the timer row for an occurrence (used to replace a
// DO object's single alarm, ADR-079).
func (s *Store) RemoveByOccurrence(ctx context.Context, occurrence string) error {
	_, err := s.cell.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM timer_retry WHERE token IN (SELECT token FROM timers WHERE occurrence=?)`, occurrence); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE occurrence=?`, occurrence)
		return err
	})
	if err == nil {
		_ = s.syncIndex(ctx)
	}
	return err
}

// Due returns timers due at or before nowMs, ascending by due time, excluding
// tokens already fired within their TTL. limit <= 0 means no limit.
func (s *Store) Due(ctx context.Context, nowMs int64, limit int) ([]Timer, error) {
	q := `SELECT t.token, t.due_ms, t.kind, t.scope, t.occurrence FROM timers t
	      LEFT JOIN timer_retry r ON r.token=t.token
	      WHERE t.due_ms <= ? AND COALESCE(r.retry_after_ms, 0) <= ?
	        AND t.token NOT IN (SELECT token FROM fired WHERE expires_ms > ?)
	      ORDER BY t.due_ms ASC, t.token ASC`
	args := []any{nowMs, nowMs, nowMs}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.cell.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Timer
	for rows.Next() {
		var t Timer
		var kind string
		if err := rows.Scan(&t.Token, &t.DueAtMs, &kind, &t.Scope, &t.Occurrence); err != nil {
			return nil, err
		}
		t.Kind = Kind(kind)
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkFired records a token as fired until expiresMs and removes its timer row in
// one transaction, so a successful dispatch is not re-delivered within the TTL.
func (s *Store) MarkFired(ctx context.Context, token string, expiresMs int64) error {
	_, err := s.cell.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fired(token, expires_ms) VALUES(?,?)
			ON CONFLICT(token) DO UPDATE SET expires_ms=excluded.expires_ms`, token, expiresMs); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE token = ?`, token); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM timer_retry WHERE token = ?`, token); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	_ = s.syncIndex(ctx)
	return nil
}

// ScheduleRetry keeps the timer's original due time while deferring the next
// attempt. The retry state is durable in the same cell as the timer itself.
func (s *Store) ScheduleRetry(ctx context.Context, token string, nowMs int64) (retryAfterMs int64, attempt int, err error) {
	_, err = s.cell.Tx(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM timers WHERE token=?`, token).Scan(&exists); err != nil {
			return err
		}
		err := tx.QueryRowContext(ctx, `SELECT attempt FROM timer_retry WHERE token=?`, token).Scan(&attempt)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		attempt++
		delay := 5 * time.Second
		for n := 1; n < attempt && delay < 5*time.Minute; n++ {
			delay *= 2
		}
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		retryAfterMs = nowMs + delay.Milliseconds()
		_, err = tx.ExecContext(ctx, `INSERT INTO timer_retry(token,attempt,retry_after_ms) VALUES(?,?,?)
			ON CONFLICT(token) DO UPDATE SET attempt=excluded.attempt,retry_after_ms=excluded.retry_after_ms`, token, attempt, retryAfterMs)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	_ = s.syncIndex(ctx)
	return retryAfterMs, attempt, nil
}

// IsFired reports whether a token was fired and its TTL has not expired.
func (s *Store) IsFired(ctx context.Context, token string, nowMs int64) (bool, error) {
	var n int
	if err := s.cell.DB.QueryRowContext(ctx,
		`SELECT count(1) FROM fired WHERE token = ? AND expires_ms > ?`, token, nowMs).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// Prune deletes expired fired rows and returns how many were removed.
func (s *Store) Prune(ctx context.Context, nowMs int64) (int, error) {
	var n int64
	if _, err := s.cell.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM fired WHERE expires_ms <= ?`, nowMs)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	}); err != nil {
		return 0, err
	}
	return int(n), nil
}

// HasExpiredFired avoids opening a write transaction (and a durability proof)
// on every timer poll when there is nothing to prune.
func (s *Store) HasExpiredFired(ctx context.Context, nowMs int64) (bool, error) {
	var found int
	err := s.cell.DB.QueryRowContext(ctx, `SELECT 1 FROM fired WHERE expires_ms <= ? LIMIT 1`, nowMs).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// Count returns the number of live (unfired) timer rows.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.cell.DB.QueryRowContext(ctx, `SELECT count(1) FROM timers`).Scan(&n)
	return n, err
}
