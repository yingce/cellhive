package sqlcapture

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"cellhive/internal/cellstore"
)

const writerUpsertSQL = `INSERT INTO kv(key, value, meta, updated_ms) VALUES(?,?,?,?)
	ON CONFLICT(key) DO UPDATE SET value=excluded.value, meta=excluded.meta, updated_ms=excluded.updated_ms`

// Writer serializes SQLite commits and assigns in-memory durability tickets in
// commit order. The LTX chain persists transaction identity; application SQL
// does not write a separate txid metadata page on every transaction.
//
// Commits are serialized so that ticket order equals WAL commit order, which
// the capture relies on to map tickets to LTX txid ranges. The UPSERT statement
// is prepared once and reused inside each transaction.
type Writer struct {
	cell   *cellstore.Cell
	mu     sync.Mutex
	stmt   *sql.Stmt
	ticket uint64
}

func NewWriter(cell *cellstore.Cell, baseline uint64) *Writer {
	return &Writer{cell: cell, ticket: baseline}
}

// Put commits one UPSERT and returns its durability ticket.
func (w *Writer) Put(ctx context.Context, key string, value, meta []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stmt == nil {
		stmt, err := w.cell.DB.PrepareContext(ctx, writerUpsertSQL)
		if err != nil {
			return 0, err
		}
		w.stmt = stmt
	}
	tx, err := w.cell.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.StmtContext(ctx, w.stmt).ExecContext(ctx, key, value, meta, time.Now().UnixMilli()); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	w.ticket++
	return w.ticket, nil
}

// WithPaused runs fn while no Put can commit. It passes the committed ticket at
// the moment of the pause, so a caller can checkpoint and take a consistent
// watermark without racing a writer.
func (w *Writer) WithPaused(fn func(committed uint64) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fn(w.ticket)
}

func (w *Writer) Current() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ticket
}

// Close releases the prepared statement.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stmt == nil {
		return nil
	}
	err := w.stmt.Close()
	w.stmt = nil
	return err
}
