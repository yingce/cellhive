// Package queue implements the Queues producer/consumer storage on a per-queue
// cell SQLite (docs/bindings.md): send (with delay and idempotency), claim with a
// lease, ack, and retry.
package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
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
  idempotency_fingerprint TEXT,
  metadata        TEXT,
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
		if err != nil {
			return err
		}
		for _, stmt := range []string{
			`ALTER TABLE messages ADD COLUMN idempotency_fingerprint TEXT`,
			`ALTER TABLE messages ADD COLUMN metadata TEXT`,
		} {
			if _, e := tx.ExecContext(ctx, stmt); e != nil && !strings.Contains(strings.ToLower(e.Error()), "duplicate column") {
				return e
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	s.migrated.Store(scope.String(), struct{}{})
	return c, nil
}

// Message is one queued message.
type Message struct {
	ID           string          `json:"id"`
	Body         []byte          `json:"body"`
	ContentType  string          `json:"content_type"`
	VisibleAtMs  int64           `json:"visible_at_ms"`
	Attempts     int             `json:"attempts"`
	LeaseUntilMs int64           `json:"lease_until_ms"`
	TimestampMs  int64           `json:"timestamp_ms"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	// IdempotencyKey travels with the message so a dead-letter replay can
	// preserve it (the replayed send dedupes against live messages again).
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Send enqueues one message. SendBatch contains the atomic implementation.
func (s *Store) Send(ctx context.Context, ns, name string, body []byte, contentType string, delaySeconds int, idempotencyKey string) (string, error) {
	ids, err := s.SendBatch(ctx, ns, name, []SendMessage{{Body: body, ContentType: contentType, DelaySeconds: delaySeconds, IdempotencyKey: idempotencyKey}})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// SendMessage is one Cloudflare Queue message in an atomic batch.
type SendMessage struct {
	Body           []byte
	ContentType    string
	DelaySeconds   int
	IdempotencyKey string
	Metadata       json.RawMessage
}

// SendBatch atomically enqueues all messages. Reusing an idempotency key with
// a different payload is a conflict; an identical message returns its old id.
func (s *Store) SendBatch(ctx context.Context, ns, name string, messages []SendMessage) ([]string, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return []string{}, nil
	}
	for _, m := range messages {
		m.ContentType = normalizeContentType(m.ContentType)
		if m.ContentType != "json" && m.ContentType != "text" && m.ContentType != "bytes" && m.ContentType != "v8" {
			return nil, fmt.Errorf("queue: invalid content type %q", m.ContentType)
		}
		if m.DelaySeconds < 0 || m.DelaySeconds > 86400 {
			return nil, fmt.Errorf("queue: delaySeconds must be between 0 and 86400")
		}
		if len(m.Body) > 128*1024 {
			return nil, fmt.Errorf("queue: message exceeds 128 KiB")
		}
	}
	now := s.now().UnixMilli()
	ids := make([]string, len(messages))
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		for i, m := range messages {
			m.ContentType = normalizeContentType(m.ContentType)
			fingerprint := fmt.Sprintf("%x:%s:%d:%s", m.Body, m.ContentType, m.DelaySeconds, string(m.Metadata))
			if m.IdempotencyKey != "" {
				var oldID, oldFP string
				e := tx.QueryRowContext(ctx, `SELECT id, COALESCE(idempotency_fingerprint,'') FROM messages WHERE idempotency_key=?`, m.IdempotencyKey).Scan(&oldID, &oldFP)
				if e == nil {
					if oldFP != "" && oldFP != fingerprint {
						return fmt.Errorf("queue: idempotency key %q conflicts with a different message", m.IdempotencyKey)
					}
					ids[i] = oldID
					continue
				}
				if e != sql.ErrNoRows {
					return e
				}
			}
			id := newID()
			ids[i] = id
			visible := now + int64(m.DelaySeconds)*1000
			var idem, metadata any
			if m.IdempotencyKey != "" {
				idem = m.IdempotencyKey
			}
			if len(m.Metadata) > 0 {
				metadata = string(m.Metadata)
			}
			if _, e := tx.ExecContext(ctx,
				`INSERT INTO messages(id, body, content_type, visible_at_ms, attempts, lease_until_ms, idempotency_key, idempotency_fingerprint, metadata, created_ms)
				 VALUES(?,?,?,?,0,0,?,?,?,?)`, id, m.Body, m.ContentType, visible, idem, fingerprint, metadata, now); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return ids, nil
}

func normalizeContentType(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "application/json":
		return "json"
	case "text/plain":
		return "text"
	case "application/octet-stream":
		return "bytes"
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
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
			`SELECT id, body, content_type, visible_at_ms, attempts, lease_until_ms, COALESCE(idempotency_key,''), created_ms, COALESCE(metadata,'') FROM messages
			 WHERE visible_at_ms <= ? AND lease_until_ms <= ? ORDER BY visible_at_ms, id LIMIT ?`,
			now, now, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m Message
			var metadata string
			if err := rows.Scan(&m.ID, &m.Body, &m.ContentType, &m.VisibleAtMs, &m.Attempts, &m.LeaseUntilMs, &m.IdempotencyKey, &m.TimestampMs, &metadata); err != nil {
				rows.Close()
				return err
			}
			if metadata != "" {
				m.Metadata = json.RawMessage(metadata)
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

// Purge removes all queued messages. It is an operator operation used by the
// Wrangler queues purge compatibility endpoint.
func (s *Store) Purge(ctx context.Context, ns, name string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM messages`)
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
