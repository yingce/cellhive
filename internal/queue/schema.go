package queue

import (
	"cellhive/internal/cellstore"
	"context"
	"database/sql"
	"sync"
)

type schemaState struct {
	mu             sync.Mutex
	cell           *cellstore.Cell
	ready, pending bool
}

// Opening a migrated queue is a read, including after process restart. Avoid
// c.Tx for no-op DDL: c.Tx consumes a replication TXID even without SQL changes.
func schemaCurrent(ctx context.Context, db *sql.DB) (bool, error) {
	for table, required := range map[string][]string{
		"messages":    {"id", "body", "content_type", "visible_at_ms", "attempts", "lease_until_ms", "idempotency_key", "idempotency_fingerprint", "metadata", "producer_request_id", "producer_traceparent", "source_queue", "source_generation", "source_id", "source_attempts", "source_created_ms", "created_ms"},
		"idempotency": {"key", "message_id", "fingerprint", "expires_ms"},
		"queue_meta":  {"key"},
	} {
		rows, err := db.QueryContext(ctx, "SELECT name FROM pragma_table_info(?)", table)
		if err != nil {
			return false, err
		}
		cols := map[string]bool{}
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				rows.Close()
				return false, err
			}
			cols[col] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, err
		}
		for _, col := range required {
			if !cols[col] {
				return false, nil
			}
		}
	}
	var indices, done int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='index' AND name IN ('messages_claim','messages_source','messages_idem','idempotency_expiry','messages_available')`).Scan(&indices); err != nil {
		return false, err
	}
	if indices != 5 {
		return false, nil
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM queue_meta WHERE key='idempotency-v1'`).Scan(&done); err != nil {
		return false, err
	}
	return done == 1, nil
}
