// Package scheduleoutbox records scheduling changes atomically with cell data.
// It contains no publisher: callers must fence ownership and prove the source
// cell durable before publishing a snapshot, and acknowledge only after success.
package scheduleoutbox

import (
	"context"
	"database/sql"
	"fmt"

	"cellhive/internal/cellstore"
)

type Kind string

const (
	Timer Kind = "timer"
	Queue Kind = "queue"
)

type Snapshot struct {
	Revision int64
	DueMs    int64
	HasDue   bool
	Pending  bool
}

func tables(kind Kind) ([]string, error) {
	switch kind {
	case Timer:
		return []string{"timers", "timer_retry"}, nil
	case Queue:
		return []string{"messages"}, nil
	default:
		return nil, fmt.Errorf("scheduleoutbox: unknown kind %q", kind)
	}
}

// Current is read-only, including when the outbox has never been installed.
func Current(ctx context.Context, db *sql.DB, kind Kind) (bool, error) {
	ts, err := tables(kind)
	if err != nil {
		return false, err
	}
	var n int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schedule_outbox'`).Scan(&n); err != nil || n == 0 {
		return false, err
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM schedule_outbox WHERE kind=?`, kind).Scan(&n); err != nil || n != 1 {
		return false, err
	}
	for _, table := range ts {
		for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
			if err = db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name=?`, "schedule_outbox_"+table+"_"+op).Scan(&n); err != nil || n != 1 {
				return false, err
			}
		}
	}
	return true, nil
}

// Install runs in the schema migration transaction. The initial pending row
// projects existing data as well, so upgrading cannot strand older messages.
func Install(ctx context.Context, tx *sql.Tx, kind Kind) error {
	ts, err := tables(kind)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schedule_outbox(kind TEXT PRIMARY KEY, revision INTEGER NOT NULL, acknowledged INTEGER NOT NULL); INSERT OR IGNORE INTO schedule_outbox(kind,revision,acknowledged) VALUES(?,1,0)`, kind); err != nil {
		return err
	}
	for _, table := range ts {
		for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
			// Identifiers and the literal kind come exclusively from the whitelist above.
			stmt := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS schedule_outbox_%s_%s AFTER %s ON %s BEGIN UPDATE schedule_outbox SET revision=revision+1 WHERE kind='%s'; END`, table, op, op, table, kind)
			if _, err = tx.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

// Read obtains revision and earliest deadline in a single SQLite snapshot.
// Empty schedules are explicit, so deletes can be projected too. A clean
// snapshot is still returned for owner takeover and periodic reconciliation.
func Read(ctx context.Context, db *sql.DB, kind Kind) (Snapshot, error) {
	var dueQuery string
	switch kind {
	case Timer:
		dueQuery = `SELECT MIN(CASE WHEN r.retry_after_ms > t.due_ms THEN r.retry_after_ms ELSE t.due_ms END) FROM timers t LEFT JOIN timer_retry r ON r.token=t.token`
	case Queue:
		dueQuery = `SELECT MIN(MAX(visible_at_ms,lease_until_ms)) FROM messages`
	default:
		return Snapshot{}, fmt.Errorf("scheduleoutbox: unknown kind %q", kind)
	}
	var s Snapshot
	var due sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT revision,revision<>acknowledged,(`+dueQuery+`) FROM schedule_outbox WHERE kind=?`, kind).Scan(&s.Revision, &s.Pending, &due)
	s.DueMs, s.HasDue = due.Int64, due.Valid
	return s, err
}

// AckTx acknowledges exactly the published revision. A newer mutation remains
// pending. Call in Cell.Tx (or use Ack), never in an uncaptured SQL transaction.
func AckTx(ctx context.Context, tx *sql.Tx, kind Kind, revision int64) error {
	if _, err := tables(kind); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE schedule_outbox SET acknowledged=? WHERE kind=? AND revision=? AND acknowledged<>?`, revision, kind, revision, revision)
	return err
}

// Ack captures the acknowledgement using the cell's normal write path. The
// caller must apply the same ownership/durability gate as other cell writes.
func Ack(ctx context.Context, c *cellstore.Cell, kind Kind, revision int64) error {
	_, err := c.Tx(ctx, func(tx *sql.Tx) error { return AckTx(ctx, tx, kind, revision) })
	return err
}
