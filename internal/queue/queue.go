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
  producer_request_id TEXT NOT NULL DEFAULT '',
  producer_traceparent TEXT NOT NULL DEFAULT '',
  source_queue    TEXT NOT NULL DEFAULT '',
  source_generation TEXT NOT NULL DEFAULT '',
  source_id       TEXT NOT NULL DEFAULT '',
  source_attempts INTEGER NOT NULL DEFAULT 0,
  source_created_ms INTEGER NOT NULL DEFAULT 0,
  created_ms      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_visible ON messages(visible_at_ms);
CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, message_id TEXT NOT NULL, fingerprint TEXT NOT NULL, expires_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idempotency_expiry ON idempotency(expires_ms);
CREATE TABLE IF NOT EXISTS queue_meta (key TEXT PRIMARY KEY);
`

// Store persists queue messages in a cell.
type Store struct {
	cs                 *cellstore.Store
	now                func() time.Time
	migrated           sync.Map // scope string -> migrated once per process
	ResourceGeneration func(context.Context, string, string) (string, error)
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
	if previous, ok := s.migrated.Load(scope.String()); ok && previous == c {
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
			`ALTER TABLE messages ADD COLUMN producer_request_id TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE messages ADD COLUMN producer_traceparent TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE messages ADD COLUMN source_queue TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE messages ADD COLUMN source_generation TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE messages ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE messages ADD COLUMN source_attempts INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE messages ADD COLUMN source_created_ms INTEGER NOT NULL DEFAULT 0`,
		} {
			if _, e := tx.ExecContext(ctx, stmt); e != nil && !strings.Contains(strings.ToLower(e.Error()), "duplicate column") {
				return e
			}
		}
		if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS messages_source ON messages(source_queue)`); err != nil {
			return err
		}
		return migrateIdempotency(ctx, tx, s.now().UnixMilli())
	}); err != nil {
		return nil, err
	}
	s.migrated.Store(scope.String(), c)
	return c, nil
}

// Message is one queued message.
type Message struct {
	ID                  string          `json:"id"`
	Body                []byte          `json:"body"`
	ContentType         string          `json:"content_type"`
	VisibleAtMs         int64           `json:"visible_at_ms"`
	Attempts            int             `json:"attempts"`
	LeaseUntilMs        int64           `json:"lease_until_ms"`
	TimestampMs         int64           `json:"timestamp_ms"`
	Metadata            json.RawMessage `json:"metadata,omitempty"`
	ProducerRequestID   string          `json:"producer_request_id,omitempty"`
	ProducerTraceparent string          `json:"producer_traceparent,omitempty"`
	// IdempotencyKey travels with the message so a dead-letter replay can
	// preserve it. Transfers use separate durable receipts so an operator can
	// replay a consumed message without bypassing producer deduplication.
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	SourceQueue      string `json:"source_queue,omitempty"`
	SourceGeneration string `json:"source_generation"`
	ClaimGeneration  string `json:"claim_generation"`
	SourceID         string `json:"source_id,omitempty"`
	SourceAttempts   int    `json:"source_attempts,omitempty"`
	SourceCreatedMs  int64  `json:"source_created_ms,omitempty"`
}

// PeekPage reads messages without claiming them. It never changes attempts,
// leases, visibility, or any other delivery state. The cursor is the last ID
// returned by the previous page; IDs are ordered lexicographically.
func (s *Store) PeekPage(ctx context.Context, ns, name, after string, limit int) ([]Message, string, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT id,body,content_type,visible_at_ms,attempts,lease_until_ms,COALESCE(idempotency_key,''),created_ms,COALESCE(metadata,''),source_queue,source_generation,source_id,source_attempts,source_created_ms,producer_request_id,producer_traceparent FROM messages WHERE id>? ORDER BY id LIMIT ?`, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]Message, 0, limit)
	hasMore := false
	for rows.Next() {
		var m Message
		var metadata string
		if err := rows.Scan(&m.ID, &m.Body, &m.ContentType, &m.VisibleAtMs, &m.Attempts, &m.LeaseUntilMs, &m.IdempotencyKey, &m.TimestampMs, &metadata, &m.SourceQueue, &m.SourceGeneration, &m.SourceID, &m.SourceAttempts, &m.SourceCreatedMs, &m.ProducerRequestID, &m.ProducerTraceparent); err != nil {
			return nil, "", err
		}
		if metadata != "" {
			m.Metadata = json.RawMessage(metadata)
		}
		if len(items) < limit {
			items = append(items, m)
		} else {
			hasMore = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if hasMore && len(items) > 0 {
		next = items[len(items)-1].ID
	}
	return items, next, nil
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
	Body                []byte
	ContentType         string
	DelaySeconds        int
	IdempotencyKey      string
	Metadata            json.RawMessage
	ProducerRequestID   string
	ProducerTraceparent string
	sourceQueue         string
	sourceGeneration    string
	sourceID            string
	sourceAttempts      int
	sourceCreatedMs     int64
}

// SendBatch atomically enqueues all messages. Reusing an idempotency key with
// a different payload is a conflict; an identical message returns its old id.
func (s *Store) SendBatch(ctx context.Context, ns, name string, messages []SendMessage) ([]string, error) {
	return s.sendBatch(ctx, ns, name, messages, "", nil)
}

// Transfer is a privileged DLQ/replay operation. Source message ID is the
// transfer receipt identity; the original producer key remains message data.
func (s *Store) Transfer(ctx context.Context, ns, name string, m Message) (string, error) {
	return s.TransferValidated(ctx, ns, name, m, nil)
}

// TransferValidated checks the source inside the destination write transaction
// so delayed transfers serialize with source-specific DLQ cleanup.
func (s *Store) TransferValidated(ctx context.Context, ns, name string, m Message, validate func(context.Context) error) (string, error) {
	if m.ID == "" {
		return "", fmt.Errorf("queue: transfer source id is required")
	}
	if s.ResourceGeneration != nil && m.SourceQueue != "" {
		next := validate
		validate = func(ctx context.Context) error {
			current, err := s.ResourceGeneration(ctx, ns, m.SourceQueue)
			if err != nil {
				return err
			}
			if current != m.SourceGeneration {
				return cell.ErrResourceDeleted
			}
			if next != nil {
				return next(ctx)
			}
			return nil
		}
	}
	ids, err := s.sendBatch(ctx, ns, name, []SendMessage{{Body: m.Body, ContentType: m.ContentType, Metadata: m.Metadata, IdempotencyKey: m.IdempotencyKey, ProducerRequestID: m.ProducerRequestID, ProducerTraceparent: m.ProducerTraceparent, sourceQueue: m.SourceQueue, sourceGeneration: m.SourceGeneration, sourceID: m.SourceID, sourceAttempts: m.SourceAttempts, sourceCreatedMs: m.SourceCreatedMs}}, m.ID, validate)
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (s *Store) sendBatch(ctx context.Context, ns, name string, messages []SendMessage, transferID string, validate func(context.Context) error) ([]string, error) {
	generation := ""
	var err error
	if s.ResourceGeneration != nil {
		generation, err = s.ResourceGeneration(ctx, ns, name)
		if err != nil {
			return nil, err
		}
	}
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
		if len(m.ProducerRequestID) > 160 || len(m.ProducerTraceparent) > 256 {
			return nil, fmt.Errorf("queue: producer context exceeds limits")
		}
		if _, err := messageFingerprint(m); err != nil {
			return nil, err
		}
	}
	now := s.now().UnixMilli()
	ids := make([]string, len(messages))
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if validate != nil {
			if err := validate(ctx); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM idempotency WHERE expires_ms<=?`, now); err != nil {
			return err
		}
		for i, m := range messages {
			m.ContentType = normalizeContentType(m.ContentType)
			fingerprint, _ := messageFingerprint(m)
			ledgerKey := ""
			if m.IdempotencyKey != "" {
				ledgerKey = "send:" + m.IdempotencyKey
			}
			if transferID != "" {
				ledgerKey = "transfer:" + transferID
				if m.sourceGeneration != "" {
					ledgerKey = "transfer:" + m.sourceGeneration + ":" + transferID
				}
			}
			if ledgerKey != "" {
				var oldID, oldFP string
				e := tx.QueryRowContext(ctx, `SELECT message_id,fingerprint FROM idempotency WHERE key=?`, ledgerKey).Scan(&oldID, &oldFP)
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
			if transferID == "" {
				m.sourceQueue = name
				m.sourceGeneration = generation
				m.sourceID = id
				m.sourceCreatedMs = now
			}
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
				`INSERT INTO messages(id, body, content_type, visible_at_ms, attempts, lease_until_ms, idempotency_key, idempotency_fingerprint, metadata, created_ms,source_queue,source_generation,source_id,source_attempts,source_created_ms,producer_request_id,producer_traceparent)
				 VALUES(?,?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,?)`, id, m.Body, m.ContentType, visible, idem, fingerprint, metadata, now, m.sourceQueue, m.sourceGeneration, m.sourceID, m.sourceAttempts, m.sourceCreatedMs, m.ProducerRequestID, m.ProducerTraceparent); e != nil {
				return e
			}
			if ledgerKey != "" {
				if err := recordIdempotency(ctx, tx, ledgerKey, id, fingerprint, now); err != nil {
					return err
				}
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

// BatchReady reports whether the first available batch should be dispatched.
// Retried messages are dispatched as soon as they become visible; new partial
// batches wait until the oldest visible message reaches maxWaitSeconds.
func (s *Store) BatchReady(ctx context.Context, ns, name string, batchSize, maxWaitSeconds int) (bool, error) {
	if maxWaitSeconds <= 0 {
		return true, nil
	}
	if batchSize <= 0 || batchSize > 100 {
		batchSize = 10
	}
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return false, err
	}
	now := s.now().UnixMilli()
	rows, err := c.DB.QueryContext(ctx, `SELECT visible_at_ms, attempts FROM messages
		WHERE visible_at_ms<=? AND lease_until_ms<=? ORDER BY visible_at_ms,id LIMIT ?`, now, now, batchSize)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	count := 0
	var oldest int64
	for rows.Next() {
		var visible int64
		var attempts int
		if err := rows.Scan(&visible, &attempts); err != nil {
			return false, err
		}
		if count == 0 {
			oldest = visible
		}
		count++
		if attempts > 0 {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return count >= batchSize || (count > 0 && now-oldest >= int64(maxWaitSeconds)*1000), nil
}

// Claim returns up to limit visible messages and leases them until now+leaseMs.
func (s *Store) Claim(ctx context.Context, ns, name string, limit int, leaseMs int64) ([]Message, error) {
	generation := ""
	var err error
	if s.ResourceGeneration != nil {
		generation, err = s.ResourceGeneration(ctx, ns, name)
		if err != nil {
			return nil, err
		}
	}
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
			`SELECT id, body, content_type, visible_at_ms, attempts, lease_until_ms, COALESCE(idempotency_key,''), created_ms, COALESCE(metadata,''),source_queue,source_generation,source_id,source_attempts,source_created_ms,producer_request_id,producer_traceparent FROM messages
			 WHERE visible_at_ms <= ? AND lease_until_ms <= ? ORDER BY visible_at_ms, id LIMIT ?`,
			now, now, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m Message
			var metadata string
			if err := rows.Scan(&m.ID, &m.Body, &m.ContentType, &m.VisibleAtMs, &m.Attempts, &m.LeaseUntilMs, &m.IdempotencyKey, &m.TimestampMs, &metadata, &m.SourceQueue, &m.SourceGeneration, &m.SourceID, &m.SourceAttempts, &m.SourceCreatedMs, &m.ProducerRequestID, &m.ProducerTraceparent); err != nil {
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
			out[i].ClaimGeneration = generation
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
	return s.AckMany(ctx, ns, name, []string{id})
}

// AckMany confirms selected messages atomically; missing IDs are safe no-ops.
func (s *Store) AckMany(ctx context.Context, ns, name string, ids []string) error {
	if len(ids) == 0 || len(ids) > 100 {
		return fmt.Errorf("queue: acknowledge requires 1..100 IDs")
	}
	for _, id := range ids {
		if id == "" || len(id) > 256 {
			return fmt.Errorf("queue: invalid message ID")
		}
	}
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		for _, id := range ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE id=?`, id); err != nil {
				return err
			}
		}
		return nil
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

// RemoveSource removes only dead letters belonging to one source queue. A
// legacy un-attributed DLQ cannot be safely classified; cleanup reports a
// retriable error instead of deleting unrelated messages or claiming success.
func (s *Store) RemoveSource(ctx context.Context, ns, name, source string, generation ...string) error {
	if source == "" || source == name {
		return fmt.Errorf("queue: source cleanup must address an independent DLQ")
	}
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var legacy int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE source_queue=''`).Scan(&legacy); err != nil {
			return err
		}
		if legacy > 0 {
			return fmt.Errorf("queue: DLQ has %d legacy messages without source provenance; preview/ack classification is required before cleanup", legacy)
		}
		var err error
		if len(generation) > 0 {
			_, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE source_queue=? AND source_generation=?`, source, generation[0])
		} else {
			_, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE source_queue=?`, source)
		}
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
