// Package workflow implements a self-built subset of Cloudflare Workflows
// (docs/bindings.md, docs/compatibility-matrix.md): instances, durable steps and
// events persisted in a per-definition cell SQLite (class "__workflow__").
//
// Supported subset: create, run, step.do (memoized, with durable
// retries/backoff), step.sleep/sleepUntil, status/output/error, events,
// pause/resume/terminate/restart, waitForEvent, instance listing and delete.
// Not supported: cross-worker instances; locationHint is accepted but ignored
// (best-effort placement hint).
package workflow

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// Status is an instance's lifecycle state.
type Status string

const (
	StatusQueued     Status = "queued"
	StatusRunning    Status = "running"
	StatusSleeping   Status = "sleeping"
	StatusPaused     Status = "paused"
	StatusComplete   Status = "complete"
	StatusErrored    Status = "errored"
	StatusTerminated Status = "terminated"
)

const schema = `
CREATE TABLE IF NOT EXISTS instances (
  id             TEXT PRIMARY KEY,
  params         BLOB,
  status         TEXT NOT NULL,
  output         BLOB,
  error          TEXT NOT NULL DEFAULT '',
  created_ms     INTEGER NOT NULL,
  updated_ms     INTEGER NOT NULL,
  run_token      TEXT NOT NULL DEFAULT '',
  lease_until_ms INTEGER NOT NULL DEFAULT 0,
  generation     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS steps (
  instance_id TEXT NOT NULL,
  name        TEXT NOT NULL,
  result      BLOB,
  PRIMARY KEY (instance_id, name)
);
CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id TEXT NOT NULL,
  payload     BLOB,
  created_ms  INTEGER NOT NULL,
  consumed    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS step_attempts (
  instance_id TEXT NOT NULL,
  name        TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (instance_id, name)
);
CREATE TABLE IF NOT EXISTS waits (
  instance_id TEXT NOT NULL,
  name        TEXT NOT NULL,
  deadline_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (instance_id, name)
);
CREATE INDEX IF NOT EXISTS events_instance ON events(instance_id, id);
`

// Store persists workflow instances in cells.
type Store struct {
	cs       *cellstore.Store
	now      func() time.Time
	migrated sync.Map // scope string -> migrated once per process
}

// New creates a workflow store.
func New(cs *cellstore.Store) *Store { return &Store{cs: cs, now: time.Now} }

// Scope is the cell scope for a workflow definition's instances (per docs: one
// __workflow__ cell per definition instance set).
func Scope(ns, name string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__workflow__", ID: name}
}

// Instance is one workflow run.
type Instance struct {
	ID        string `json:"id"`
	Status    Status `json:"status"`
	Params    []byte `json:"params,omitempty"`
	Output    []byte `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	CreatedMs int64  `json:"created_ms"`
	UpdatedMs int64  `json:"updated_ms"`
}

func (s *Store) cell(ctx context.Context, ns, name string) (*cellstore.Cell, error) {
	if ns == "" || name == "" {
		return nil, fmt.Errorf("workflow: ns and name are required")
	}
	scope := Scope(ns, name)
	c, err := s.cs.Cell(ctx, scope)
	if err != nil {
		return nil, err
	}
	if _, ok := s.migrated.Load(scope.String()); ok {
		return c, nil
	}
	// The schema and best-effort ALTERs are idempotent; run them once per process
	// so a read API does not advance the capture txid on every call (read traffic
	// -> replication). Each statement goes through Cell.Tx so a cold cell's
	// migration cannot desynchronise capture's txid sequence.
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, schema)
		return err
	}); err != nil {
		return nil, err
	}
	for _, stmt := range []string{
		`ALTER TABLE events ADD COLUMN consumed INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN run_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN lease_until_ms INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN generation INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, stmt)
			return err
		}); err != nil {
			_ = err // already present on cells created by the current schema
		}
	}
	s.migrated.Store(scope.String(), struct{}{})
	return c, nil
}

// ErrStaleRun means a callback carried a run token that no longer owns the
// instance (a newer run or a terminal transition fenced it out).
var ErrStaleRun = errors.New("workflow: stale run token")

// ErrRunActive means another run currently holds the instance lease.
var ErrRunActive = errors.New("workflow: run lease held")

func newRunToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ClaimRun acquires the instance's run lease for a new attempt. ok=false when
// the instance is paused/terminal or another run holds a live lease.
func (s *Store) ClaimRun(ctx context.Context, ns, name, id string, ttlMs int64) (token string, generation uint64, status Status, ok bool, err error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return "", 0, "", false, err
	}
	var cur string
	var lease int64
	var gen uint64
	now := s.now().UnixMilli()
	var leased bool
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT status, lease_until_ms, generation FROM instances WHERE id = ?`, id).Scan(&cur, &lease, &gen); err != nil {
			return err
		}
		switch Status(cur) {
		case StatusPaused, StatusTerminated, StatusComplete, StatusErrored:
			return nil
		}
		if lease > now {
			return ErrRunActive
		}
		token = newRunToken()
		gen++
		if _, err := tx.ExecContext(ctx,
			`UPDATE instances SET run_token = ?, lease_until_ms = ?, generation = ?, status = ?, updated_ms = ? WHERE id = ?`,
			token, now+ttlMs, gen, string(StatusRunning), now, id); err != nil {
			return err
		}
		leased = true
		return nil
	}); err != nil {
		if errors.Is(err, ErrRunActive) {
			return "", gen, Status(cur), false, ErrRunActive
		}
		return "", 0, "", false, err
	}
	if !leased {
		return "", gen, Status(cur), false, nil
	}
	return token, gen, StatusRunning, true, nil
}

// RenewRun extends the lease for the current run token.
func (s *Store) RenewRun(ctx context.Context, ns, name, id, token string, ttlMs int64) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	n, err := execCell(ctx, c, `UPDATE instances SET lease_until_ms = ? WHERE id = ? AND run_token = ?`,
		s.now().UnixMilli()+ttlMs, id, token)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrStaleRun
	}
	return nil
}

// RunStatus validates the run token and returns the current status.
func (s *Store) RunStatus(ctx context.Context, ns, name, id, token string) (Status, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return "", err
	}
	var cur, tok string
	if err := c.DB.QueryRowContext(ctx, `SELECT status, run_token FROM instances WHERE id = ?`, id).Scan(&cur, &tok); err != nil {
		return "", err
	}
	if token != "" && tok != token {
		return "", ErrStaleRun
	}
	return Status(cur), nil
}

// ReleaseRun clears the lease (a run attempt ended: parked/finished/paused).
func (s *Store) ReleaseRun(ctx context.Context, ns, name, id, token string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c, `UPDATE instances SET lease_until_ms = 0, run_token = '' WHERE id = ? AND (run_token = ? OR ? = '')`, id, token, token)
	return err
}

// Prune deletes terminal instances updated before beforeMs, cascading their
// steps/attempts/events/waits (retention). Returns the number removed.
func (s *Store) Prune(ctx context.Context, ns, name string, beforeMs int64) (int, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return 0, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT id FROM instances WHERE status IN (?,?,?) AND updated_ms < ?`,
		string(StatusComplete), string(StatusErrored), string(StatusTerminated), beforeMs)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, id := range ids {
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			for _, tbl := range []string{"steps", "step_attempts", "events", "waits"} {
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE instance_id = ?", id); err != nil {
					return err
				}
			}
			_, err := tx.ExecContext(ctx, `DELETE FROM instances WHERE id = ?`, id)
			return err
		}); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

// Drop deletes every instance (and its steps/attempts/events/waits) of a
// workflow definition (worker/app delete cleanup).
func (s *Store) Drop(ctx context.Context, ns, name string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		for _, tbl := range []string{"steps", "step_attempts", "events", "waits", "instances"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetAttempt returns the persisted retry attempt count/error for a step.
func (s *Store) GetAttempt(ctx context.Context, ns, name, id, step string) (int, string, bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return 0, "", false, err
	}
	var attempts int
	var lastErr string
	err = c.DB.QueryRowContext(ctx, `SELECT attempts, last_error FROM step_attempts WHERE instance_id = ? AND name = ?`, id, step).Scan(&attempts, &lastErr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", false, nil
		}
		return 0, "", false, err
	}
	return attempts, lastErr, true, nil
}

// SetAttempt upserts the retry attempt count/error for a step (durable retries).
func (s *Store) SetAttempt(ctx context.Context, ns, name, id, step string, attempts int, lastErr string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`INSERT INTO step_attempts(instance_id, name, attempts, last_error) VALUES(?,?,?,?)
		 ON CONFLICT(instance_id, name) DO UPDATE SET attempts = excluded.attempts, last_error = excluded.last_error`,
		id, step, attempts, lastErr)
	return err
}

// ClearAttempt removes a step's retry state (on success/restart).
func (s *Store) ClearAttempt(ctx context.Context, ns, name, id, step string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c, `DELETE FROM step_attempts WHERE instance_id = ? AND name = ?`, id, step)
	return err
}

// SetWait records a waitForEvent deadline (0 = no timeout).
func (s *Store) SetWait(ctx context.Context, ns, name, id, step string, deadlineMs int64) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`INSERT INTO waits(instance_id, name, deadline_ms) VALUES(?,?,?)
		 ON CONFLICT(instance_id, name) DO UPDATE SET deadline_ms = excluded.deadline_ms`, id, step, deadlineMs)
	return err
}

// ClearWait removes a waitForEvent entry.
func (s *Store) ClearWait(ctx context.Context, ns, name, id, step string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c, `DELETE FROM waits WHERE instance_id = ? AND name = ?`, id, step)
	return err
}

// GetWait returns a waitForEvent deadline (ok=false when none).
func (s *Store) GetWait(ctx context.Context, ns, name, id, step string) (int64, bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return 0, false, err
	}
	var deadline int64
	err = c.DB.QueryRowContext(ctx, `SELECT deadline_ms FROM waits WHERE instance_id = ? AND name = ?`, id, step).Scan(&deadline)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return deadline, true, nil
}

// ConsumeEvent returns the oldest unconsumed event for an instance (optionally
// matching a JSON "type" field) and marks it consumed.
func (s *Store) ConsumeEvent(ctx context.Context, ns, name, id, evType string) ([]byte, bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, false, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT id, payload FROM events WHERE instance_id = ? AND consumed = 0 ORDER BY id`, id)
	if err != nil {
		return nil, false, err
	}
	type row struct {
		id      int64
		payload []byte
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.payload); err != nil {
			rows.Close()
			return nil, false, err
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	for _, r := range all {
		if evType != "" {
			var probe struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(r.payload, &probe) != nil || probe.Type != evType {
				continue
			}
		}
		if _, err := execCell(ctx, c, `UPDATE events SET consumed = 1 WHERE id = ?`, r.id); err != nil {
			return nil, false, err
		}
		return r.payload, true, nil
	}
	return nil, false, nil
}

// execCell runs one cell write through Cell.Tx so it advances the cell's txid
// and the capture watermark covers it before the caller is acknowledged
// (RPO=0). RowsAffected is returned when the statement reports it.
func execCell(ctx context.Context, c *cellstore.Cell, q string, args ...any) (int64, error) {
	var n int64
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// NewID returns a random instance id.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Create inserts a new instance in the queued state. A non-empty id makes the
// call idempotent: an existing instance with the same id is returned unchanged.
func (s *Store) Create(ctx context.Context, ns, name, id string, params []byte) (Instance, error) {
	if id == "" {
		id = NewID()
	}
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return Instance{}, err
	}
	now := s.now().UnixMilli()
	if _, err := execCell(ctx, c,
		`INSERT INTO instances(id, params, status, created_ms, updated_ms) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO NOTHING`,
		id, params, string(StatusQueued), now, now); err != nil {
		return Instance{}, err
	}
	return s.Get(ctx, ns, name, id)
}

// Get returns one instance.
func (s *Store) Get(ctx context.Context, ns, name, id string) (Instance, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return Instance{}, err
	}
	var in Instance
	var status string
	err = c.DB.QueryRowContext(ctx,
		`SELECT id, params, status, output, error, created_ms, updated_ms FROM instances WHERE id = ?`, id).
		Scan(&in.ID, &in.Params, &status, &in.Output, &in.Error, &in.CreatedMs, &in.UpdatedMs)
	if err != nil {
		return Instance{}, err
	}
	in.Status = Status(status)
	return in, nil
}

// List returns recent instances (operator/diagnostic use).
func (s *Store) List(ctx context.Context, ns, name string, limit int) ([]Instance, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT id, params, status, output, error, created_ms, updated_ms FROM instances ORDER BY created_ms DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var in Instance
		var status string
		if err := rows.Scan(&in.ID, &in.Params, &status, &in.Output, &in.Error, &in.CreatedMs, &in.UpdatedMs); err != nil {
			return nil, err
		}
		in.Status = Status(status)
		out = append(out, in)
	}
	return out, rows.Err()
}

// SetStatus updates an instance's status (and error text when provided).
func (s *Store) SetStatus(ctx context.Context, ns, name, id string, status Status, errText string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`UPDATE instances SET status = ?, error = ?, updated_ms = ? WHERE id = ?`,
		string(status), errText, s.now().UnixMilli(), id)
	return err
}

// Delete removes an instance and all of its persisted state (step results,
// attempts, events, waits). It reports whether the instance existed; deleting a
// missing instance is not an error (idempotent).
func (s *Store) Delete(ctx context.Context, ns, name, id string) (bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return false, err
	}
	var removed bool
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM instances WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		removed = true
		for _, tbl := range []string{"steps", "step_attempts", "events", "waits"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE instance_id = ?", id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return false, err
	}
	return removed, nil
}

// SetOutput stores the final output and marks the instance complete.
func (s *Store) SetOutput(ctx context.Context, ns, name, id string, output []byte) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`UPDATE instances SET status = ?, output = ?, updated_ms = ?, run_token = '', lease_until_ms = 0 WHERE id = ?`,
		string(StatusComplete), output, s.now().UnixMilli(), id)
	return err
}

// Pause/Resume/Terminate change instance status without touching steps.
func (s *Store) Pause(ctx context.Context, ns, name, id string) error {
	if err := s.SetStatus(ctx, ns, name, id, StatusPaused, ""); err != nil {
		return err
	}
	return s.ReleaseRun(ctx, ns, name, id, "")
}

func (s *Store) Resume(ctx context.Context, ns, name, id string) error {
	return s.SetStatus(ctx, ns, name, id, StatusQueued, "")
}

func (s *Store) Terminate(ctx context.Context, ns, name, id string) error {
	if err := s.SetStatus(ctx, ns, name, id, StatusTerminated, ""); err != nil {
		return err
	}
	return s.ReleaseRun(ctx, ns, name, id, "")
}

// Restart clears all memoized steps and re-queues the instance from the start.
func (s *Store) Restart(ctx context.Context, ns, name, id string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM steps WHERE instance_id = ?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM step_attempts WHERE instance_id = ?`, id); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE instances SET status = ?, output = NULL, error = '', updated_ms = ?, run_token = '', lease_until_ms = 0 WHERE id = ?`,
			string(StatusQueued), now, id)
		return err
	})
	return err
}

// PutStep memoizes a step result (idempotent per (instance, step)).
func (s *Store) PutStep(ctx context.Context, ns, name, id, step string, result []byte) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`INSERT INTO steps(instance_id, name, result) VALUES(?,?,?)
		 ON CONFLICT(instance_id, name) DO NOTHING`, id, step, result)
	return err
}

// GetStep returns a memoized step result.
func (s *Store) GetStep(ctx context.Context, ns, name, id, step string) ([]byte, bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, false, err
	}
	var result []byte
	err = c.DB.QueryRowContext(ctx,
		`SELECT result FROM steps WHERE instance_id = ? AND name = ?`, id, step).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return result, true, nil
}

// AppendEvent records an event delivered to an instance.
func (s *Store) AppendEvent(ctx context.Context, ns, name, id string, payload []byte) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execCell(ctx, c,
		`INSERT INTO events(instance_id, payload, created_ms) VALUES(?,?,?)`, id, payload, s.now().UnixMilli())
	return err
}

// Events returns all events for an instance in order.
func (s *Store) Events(ctx context.Context, ns, name, id string) ([][]byte, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT payload FROM events WHERE instance_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var p []byte
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
