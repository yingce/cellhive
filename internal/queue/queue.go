// Package queue implements the Queues producer/consumer storage on a per-queue
// cell SQLite (docs/bindings.md): send (with delay and idempotency), claim with a
// lease, ack, and retry.
package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

const schema = `
CREATE TABLE IF NOT EXISTS messages (
  id              TEXT PRIMARY KEY,
  body            BLOB NOT NULL,
  content_type    TEXT NOT NULL,
  visible_at_ms   INTEGER NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  lease_until_ms  INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  created_ms      INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS messages_idem ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS messages_visible ON messages(visible_at_ms);
`

// Store persists queue messages in a cell.
type Store struct {
	cs       *cellstore.Store
	now      func() time.Time
	migrated sync.Map // scope string -> migrated once per process
}

// New creates a queue store.
func New(cs *cellstore.Store) *Store { return &Store{cs: cs, now: time.Now} }

// Scope is the cell scope for a queue.
func Scope(ns, name string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__queue__", ID: name}
}

func (s *Store) cell(ctx context.Context, ns, name string) (*cellstore.Cell, error) {
	if ns == "" || name == "" {
		return nil, fmt.Errorf("queue: ns and name are required")
	}
	scope := Scope(ns, name)
	c, err := s.cs.Cell(ctx, scope)
	if err != nil {
		return nil, err
	}
	if _, ok := s.migrated.Load(scope.String()); ok {
		return c, nil
	}
	// The schema is idempotent; run it once per process so a read API does not
	// advance the capture txid on every call (read traffic -> replication).
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, schema)
		return err
	}); err != nil {
		return nil, err
	}
	s.migrated.Store(scope.String(), struct{}{})
	return c, nil
}

// Message is one queued message.
type Message struct {
	ID           string `json:"id"`
	Body         []byte `json:"body"`
	ContentType  string `json:"content_type"`
	VisibleAtMs  int64  `json:"visible_at_ms"`
	Attempts     int    `json:"attempts"`
	LeaseUntilMs int64  `json:"lease_until_ms"`
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Send enqueues a message, honoring a delay and an idempotency key.
func (s *Store) Send(ctx context.Context, ns, name string, body []byte, contentType string, delaySeconds int, idempotencyKey string) (string, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return "", err
	}
	now := s.now().UnixMilli()
	if idempotencyKey != "" {
		var existing string
		err := c.DB.QueryRowContext(ctx, `SELECT id FROM messages WHERE idempotency_key=?`, idempotencyKey).Scan(&existing)
		if err == nil {
			return existing, nil // dedupe
		}
	}
	id := newID()
	visible := now + int64(delaySeconds)*1000
	var idem any
	if idempotencyKey != "" {
		idem = idempotencyKey
	}
	// Cell.Tx advances the cell txid so the capture watermark covers this write
	// before the caller is acknowledged (RPO=0).
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO messages(id, body, content_type, visible_at_ms, attempts, lease_until_ms, idempotency_key, created_ms)
			 VALUES(?,?,?,?,0,0,?,?)`,
			id, body, contentType, visible, idem, now)
		return err
	}); err != nil {
		// A concurrent send with the same idempotency key won the unique index;
		// dedupe by returning its id instead of surfacing a constraint error.
		if idempotencyKey != "" {
			var existing string
			if qerr := c.DB.QueryRowContext(ctx,
				`SELECT id FROM messages WHERE idempotency_key=?`, idempotencyKey).Scan(&existing); qerr == nil {
				return existing, nil
			}
		}
		return "", err
	}
	return id, nil
}

// Claim returns up to limit visible messages and leases them until now+leaseMs.
func (s *Store) Claim(ctx context.Context, ns, name string, limit int, leaseMs int64) ([]Message, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	if leaseMs <= 0 {
		leaseMs = 30_000
	}
	now := s.now().UnixMilli()
	var out []Message
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			`SELECT id, body, content_type, visible_at_ms, attempts, lease_until_ms FROM messages
			 WHERE visible_at_ms <= ? AND lease_until_ms <= ? ORDER BY visible_at_ms, id LIMIT ?`,
			now, now, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m Message
			if err := rows.Scan(&m.ID, &m.Body, &m.ContentType, &m.VisibleAtMs, &m.Attempts, &m.LeaseUntilMs); err != nil {
				rows.Close()
				return err
			}
			out = append(out, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i := range out {
			lease := now + leaseMs
			if _, err := tx.ExecContext(ctx,
				`UPDATE messages SET lease_until_ms=?, attempts=attempts+1 WHERE id=?`, lease, out[i].ID); err != nil {
				return err
			}
			out[i].LeaseUntilMs = lease
			out[i].Attempts++
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// Ack removes a message.
func (s *Store) Ack(ctx context.Context, ns, name, id string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE id=?`, id)
		return err
	})
	return err
}

// Retry makes a message visible again after delayMs.
func (s *Store) Retry(ctx context.Context, ns, name, id string, delayMs int64) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE messages SET visible_at_ms=?, lease_until_ms=0 WHERE id=?`, s.now().UnixMilli()+delayMs, id)
		return err
	})
	return err
}

// Depth returns the number of pending messages.
func (s *Store) Depth(ctx context.Context, ns, name string) (int, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return 0, err
	}
	var n int
	err = c.DB.QueryRowContext(ctx, `SELECT count(1) FROM messages`).Scan(&n)
	return n, err
}

// Status summarizes a queue's cell without listing message bodies.
type Status struct {
	Depth   int `json:"depth"`   // all messages, regardless of visibility
	Visible int `json:"visible"` // claimable now
	Leased  int `json:"leased"`  // currently leased by a consumer
	// OldestVisibleMs is the earliest visible_at_ms across queued messages (0 =
	// empty). It is a lag/latency indicator: a small value with a non-zero depth
	// means work is overdue.
	OldestVisibleMs int64 `json:"oldest_visible_ms"`
	// MaxAttempts is the highest delivery attempt count in the queue.
	MaxAttempts int `json:"max_attempts"`
}

// Status returns the queue's depth/visible/leased counts (ADR-156).
func (s *Store) Status(ctx context.Context, ns, name string) (Status, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return Status{}, err
	}
	now := s.now().UnixMilli()
	var st Status
	err = c.DB.QueryRowContext(ctx,
		`SELECT count(1),
		        coalesce(sum(CASE WHEN visible_at_ms <= ? AND lease_until_ms <= ? THEN 1 ELSE 0 END), 0),
		        coalesce(sum(CASE WHEN lease_until_ms > ? THEN 1 ELSE 0 END), 0),
		        coalesce(min(visible_at_ms), 0),
		        coalesce(max(attempts), 0)
		   FROM messages`, now, now, now).Scan(&st.Depth, &st.Visible, &st.Leased, &st.OldestVisibleMs, &st.MaxAttempts)
	return st, err
}

// DiskStats returns the queue cell's cheap storage footprint (ADR-157): SQLite
// page accounting plus the real file/WAL bytes.
func (s *Store) DiskStats(ctx context.Context, ns, name string) (cellstore.DiskStats, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return cellstore.DiskStats{}, err
	}
	return c.DiskStats(ctx)
}
