// Package invocation persists Queue/Cron/Workflow execution attempts and their console
// output in a namespace cell. Logical IDs correlate retries; IDs identify one
// immutable attempt, independent of sampled tracing or the ephemeral log ring.
package invocation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type Log struct {
	Level   string `json:"level"`
	Message string `json:"message"`
	AtMs    int64  `json:"at_ms"`
}
type MessageAttempt struct {
	ID        string `json:"id"`
	LogicalID string `json:"logical_id"`
	Attempt   int    `json:"attempt"`
}
type MessageRetry struct {
	ID           string `json:"id"`
	DelaySeconds int    `json:"delay_seconds"`
}
type ProducerLink struct {
	MessageID   string `json:"message_id"`
	RequestID   string `json:"request_id,omitempty"`
	Traceparent string `json:"traceparent,omitempty"`
}
type Record struct {
	ID              string           `json:"invocation_id"`
	RequestID       string           `json:"request_id,omitempty"`
	Traceparent     string           `json:"traceparent,omitempty"`
	LogicalID       string           `json:"logical_id"`
	Namespace       string           `json:"namespace"`
	Worker          string           `json:"worker"`
	Kind            string           `json:"kind"`
	Resource        string           `json:"resource,omitempty"`
	Attempt         int              `json:"attempt"`
	StartedMs       int64            `json:"started_ms"`
	FinishedMs      int64            `json:"finished_ms,omitempty"`
	Status          string           `json:"status"`
	Error           string           `json:"error,omitempty"`
	ErrorStack      string           `json:"error_stack,omitempty"`
	Logs            []Log            `json:"logs"`
	LogsTruncated   bool             `json:"logs_truncated,omitempty"`
	RetryRequested  bool             `json:"retry_requested,omitempty"`
	NoRetry         bool             `json:"no_retry,omitempty"`
	Messages        []MessageAttempt `json:"messages,omitempty"`
	ProducerLinks   []ProducerLink   `json:"producer_links,omitempty"`
	Parents         []string         `json:"parent_invocation_ids,omitempty"`
	Result          json.RawMessage  `json:"result,omitempty"`
	ResultEncoding  string           `json:"result_encoding,omitempty"`
	ResultTruncated bool             `json:"result_truncated,omitempty"`
	Ack             []string         `json:"ack,omitempty"`
	Retry           []MessageRetry   `json:"retry,omitempty"`
}
type Filter struct {
	Namespace, Worker, ID, Kind, Cursor string
	MessageID, MessageLogicalID         string
	FromMs, ToMs                        int64
	Limit                               int
}
type Store struct {
	cs       *cellstore.Store
	migrated sync.Map
	now      func() time.Time
}

func New(cs *cellstore.Store) *Store { return &Store{cs: cs, now: time.Now} }
func Scope(ns string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__invocations__", ID: "main"}
}
func (s *Store) open(ctx context.Context, ns string) (*cellstore.Cell, error) {
	sc := Scope(ns)
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	c, err := s.cs.Cell(ctx, sc)
	if err != nil {
		return nil, err
	}
	if previous, ok := s.migrated.Load(sc.String()); !ok || previous != c {
		_, err = c.Tx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS invocations(id TEXT PRIMARY KEY,worker TEXT NOT NULL,kind TEXT NOT NULL,started_ms INTEGER NOT NULL,record BLOB NOT NULL); CREATE INDEX IF NOT EXISTS invocations_worker_time ON invocations(worker,started_ms,id); CREATE INDEX IF NOT EXISTS invocations_time ON invocations(started_ms,id); CREATE INDEX IF NOT EXISTS invocations_retry ON invocations(json_extract(CAST(record AS TEXT),'$.logical_id'),json_extract(CAST(record AS TEXT),'$.attempt'));
CREATE TABLE IF NOT EXISTS invocation_messages(invocation_id TEXT NOT NULL,message_id TEXT NOT NULL,logical_id TEXT NOT NULL,attempt INTEGER NOT NULL,PRIMARY KEY(invocation_id,message_id));
CREATE INDEX IF NOT EXISTS invocation_message_lineage ON invocation_messages(logical_id,invocation_id);
CREATE INDEX IF NOT EXISTS invocation_message_id ON invocation_messages(message_id,invocation_id);`)
			return err
		})
		if err != nil {
			return nil, err
		}
		s.migrated.Store(sc.String(), c)
	}
	return c, nil
}

func Validate(r Record) error {
	if r.ID == "" || len(r.ID) > 160 || r.LogicalID == "" || len(r.LogicalID) > 512 || r.Worker == "" || len(r.Worker) > 256 || r.StartedMs <= 0 || r.Attempt < 1 {
		return fmt.Errorf("invocation: invalid identity")
	}
	if r.Kind != "queue" && r.Kind != "cron" && r.Kind != "workflow" {
		return fmt.Errorf("invocation: invalid trigger")
	}
	switch r.Status {
	case "running", "success", "failed", "retry", "unknown":
	default:
		return fmt.Errorf("invocation: invalid status")
	}
	if len(r.Logs) > 256 || len(r.Error) > 16384 || len(r.ErrorStack) > 16384 {
		return fmt.Errorf("invocation: record exceeds limits")
	}
	if len(r.RequestID) > 160 || len(r.Traceparent) > 256 {
		return fmt.Errorf("invocation: correlation fields exceed limits")
	}
	if len(r.Messages) > 100 || len(r.ProducerLinks) > 100 || len(r.Parents) > 100 || len(r.Ack) > 100 || len(r.Retry) > 100 || len(r.Result) > 8192 {
		return fmt.Errorf("invocation: result/membership exceeds limits")
	}
	for _, m := range r.Messages {
		if m.ID == "" || len(m.ID) > 128 || m.LogicalID == "" || len(m.LogicalID) > 160 || m.Attempt < 1 {
			return fmt.Errorf("invocation: invalid message lineage")
		}
	}
	for _, link := range r.ProducerLinks {
		if len(link.MessageID) > 128 || len(link.RequestID) > 160 || len(link.Traceparent) > 256 || link.MessageID == "" {
			return fmt.Errorf("invocation: invalid producer link")
		}
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > 128*1024 {
		return fmt.Errorf("invocation: record exceeds byte limit")
	}
	return nil
}
func (s *Store) Put(ctx context.Context, r Record) error {
	if err := Validate(r); err != nil {
		return err
	}
	c, err := s.open(ctx, r.Namespace)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(r)
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var old []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM invocations WHERE id=?`, r.ID).Scan(&old)
		if err == nil {
			var previous Record
			if err := json.Unmarshal(old, &previous); err != nil {
				return err
			}
			if previous.Worker != r.Worker || previous.LogicalID != r.LogicalID || previous.Kind != r.Kind || previous.StartedMs != r.StartedMs || previous.Attempt != r.Attempt {
				return fmt.Errorf("invocation: identity is immutable")
			}
			before, _ := json.Marshal(previous.Messages)
			after, _ := json.Marshal(r.Messages)
			if string(before) != string(after) {
				return fmt.Errorf("invocation: membership is immutable")
			}
			before, _ = json.Marshal(previous.ProducerLinks)
			after, _ = json.Marshal(r.ProducerLinks)
			if string(before) != string(after) {
				return fmt.Errorf("invocation: producer links are immutable")
			}
			if previous.Status != "running" {
				return nil
			}
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO invocations(id,worker,kind,started_ms,record) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record`, r.ID, r.Worker, r.Kind, r.StartedMs, b)
		if err != nil {
			return err
		}
		return s.pruneTx(ctx, tx)
	})
	return err
}

// Start allocates the logical retry attempt and records it atomically before
// tenant code is entered. Repeating the same invocation ID is idempotent.
func (s *Store) Start(ctx context.Context, r Record) (Record, error) {
	if err := Validate(r); err != nil {
		return r, err
	}
	c, err := s.open(ctx, r.Namespace)
	if err != nil {
		return r, err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var old []byte
		err := tx.QueryRowContext(ctx, `SELECT record FROM invocations WHERE id=?`, r.ID).Scan(&old)
		if err == nil {
			var previous Record
			if err := json.Unmarshal(old, &previous); err != nil {
				return err
			}
			if previous.Worker != r.Worker || previous.LogicalID != r.LogicalID || previous.Kind != r.Kind || previous.StartedMs != r.StartedMs {
				return fmt.Errorf("invocation: identity is immutable")
			}
			r = previous
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var next int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(json_extract(CAST(record AS TEXT),'$.attempt')),0)+1 FROM invocations WHERE json_extract(CAST(record AS TEXT),'$.logical_id')=?`, r.LogicalID).Scan(&next); err != nil {
			return err
		}
		if next > r.Attempt {
			r.Attempt = next
		}
		parents := map[string]bool{}
		for _, message := range r.Messages {
			var parent string
			err := tx.QueryRowContext(ctx, `SELECT m.invocation_id FROM invocation_messages m JOIN invocations i ON i.id=m.invocation_id WHERE m.logical_id=? ORDER BY i.started_ms DESC,i.id DESC LIMIT 1`, message.LogicalID).Scan(&parent)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				parents[parent] = true
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO invocation_messages(invocation_id,message_id,logical_id,attempt) VALUES(?,?,?,?)`, r.ID, message.ID, message.LogicalID, message.Attempt); err != nil {
				return err
			}
		}
		r.Parents = nil
		for parent := range parents {
			r.Parents = append(r.Parents, parent)
		}
		sort.Strings(r.Parents)
		b, _ := json.Marshal(r)
		_, err = tx.ExecContext(ctx, `INSERT INTO invocations(id,worker,kind,started_ms,record) VALUES(?,?,?,?,?)`, r.ID, r.Worker, r.Kind, r.StartedMs, b)
		if err != nil {
			return err
		}
		return s.pruneTx(ctx, tx)
	})
	return r, err
}

func (s *Store) pruneTx(ctx context.Context, tx *sql.Tx) error {
	// Keep at most 10,000 attempts per namespace and seven days of history.
	// Cleanup is bounded per mutation so retention cannot stall a busy namespace.
	cutoff := s.now().Add(-7 * 24 * time.Hour).UnixMilli()
	if err := pruneInvocations(ctx, tx, `SELECT id FROM invocations WHERE started_ms<? ORDER BY started_ms,id LIMIT 100`, cutoff); err != nil {
		return err
	}
	return pruneInvocations(ctx, tx, `SELECT id FROM invocations ORDER BY started_ms DESC,id DESC LIMIT 100 OFFSET 10000`)
}

// pruneInvocations deletes the selected invocations and only their message
// memberships. The old implementation ran a NOT IN anti-join over every
// invocation_messages row on every Put/Start, which grows with the whole
// 10,000-invocation retention window.
func pruneInvocations(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(ids) == 0 {
		return nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	values := make([]any, len(ids))
	for i, id := range ids {
		values[i] = id
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM invocation_messages WHERE invocation_id IN (`+ph+`)`, values...); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM invocations WHERE id IN (`+ph+`)`, values...)
	return err
}
func (s *Store) List(ctx context.Context, f Filter) ([]Record, error) {
	c, err := s.open(ctx, f.Namespace)
	if err != nil {
		return nil, err
	}
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 100
	}
	// IDs are UUIDs; a lexical exclusive cursor is stable while previous records
	// are deleted. Time filters apply to the original invocation start.
	rows, err := c.DB.QueryContext(ctx, `SELECT record FROM invocations WHERE id>? AND (?='' OR id=?) AND (?='' OR worker=?) AND (?='' OR kind=?) AND (?=0 OR started_ms>=?) AND (?=0 OR started_ms<=?) AND (?='' OR id IN (SELECT invocation_id FROM invocation_messages WHERE message_id=?)) AND (?='' OR id IN (SELECT invocation_id FROM invocation_messages WHERE logical_id=?)) ORDER BY id LIMIT ?`, f.Cursor, f.ID, f.ID, f.Worker, f.Worker, f.Kind, f.Kind, f.FromMs, f.FromMs, f.ToMs, f.ToMs, f.MessageID, f.MessageID, f.MessageLogicalID, f.MessageLogicalID, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		var r Record
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
