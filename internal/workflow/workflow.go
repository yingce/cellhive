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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
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
  generation     INTEGER NOT NULL DEFAULT 0,
  target_worker  TEXT NOT NULL DEFAULT '',
  target_bundle  TEXT NOT NULL DEFAULT '',
  target_version INTEGER NOT NULL DEFAULT 0,
  target_class   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS instances_retention ON instances(status, updated_ms);
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
	migrated sync.Map // scope string -> cell handle whose schema was migrated
	Index    timer.Index
	Register func(string)
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
	ID            string `json:"id"`
	Status        Status `json:"status"`
	Params        []byte `json:"params,omitempty"`
	Output        []byte `json:"output,omitempty"`
	Error         string `json:"error,omitempty"`
	CreatedMs     int64  `json:"created_ms"`
	UpdatedMs     int64  `json:"updated_ms"`
	TargetWorker  string `json:"target_worker,omitempty"`
	TargetBundle  string `json:"target_bundle,omitempty"`
	TargetVersion int    `json:"target_version,omitempty"`
	TargetClass   string `json:"target_class,omitempty"`
}

// CompletedStep is a persisted memoized result. The store does not record
// Cloudflare's per-attempt timing or in-progress step history.
type CompletedStep struct {
	Name   string `json:"name"`
	Result []byte `json:"result"`
}

func (s *Store) CompletedSteps(ctx context.Context, ns, name, id string) ([]CompletedStep, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT name, result FROM steps WHERE instance_id=? ORDER BY rowid`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	steps := make([]CompletedStep, 0)
	for rows.Next() {
		var step CompletedStep
		if err := rows.Scan(&step.Name, &step.Result); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

// Target pins the Worker version/class used by a workflow instance.
type Target struct {
	Worker, BundleSHA, ClassName string
	Version                      int
}

// Policy is the Cloudflare Workflow definition policy persisted by the
// control-plane compatibility layer. Zero values preserve the historical
// CellHive behavior.
type Policy struct {
	Concurrency        int      `json:"concurrency,omitempty"`
	Schedules          []string `json:"schedules,omitempty"`
	SuccessRetentionMs int64    `json:"success_retention_ms,omitempty"`
	ErrorRetentionMs   int64    `json:"error_retention_ms,omitempty"`
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
	if previous, ok := s.migrated.Load(scope.String()); ok && previous == c {
		return c, nil
	}
	// The schema and best-effort ALTERs are idempotent; run them once per cell handle
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
		`ALTER TABLE instances ADD COLUMN target_worker TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN target_bundle TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN target_version INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN target_class TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, stmt)
			return err
		}); err != nil && !isDuplicateColumn(err) {
			// A real migration failure (permissions, disk, corruption) must fail
			// the operation and must NOT be cached as migrated, or every later
			// call would run against a half-migrated cell.
			return nil, fmt.Errorf("workflow: migrate %s: %w", scope.String(), err)
		}
	}
	if _, err := timer.NewStore(ctx, c); err != nil {
		s.migrated.Delete(scope.String())
		return nil, err
	}
	s.migrated.Store(scope.String(), c)
	return c, nil
}

// isDuplicateColumn reports the expected "duplicate column name" error from an
// ADD COLUMN for a column the current schema already created. SQLite reports it
// as a plain SQLITE_ERROR (no distinct code), so match the message.
func isDuplicateColumn(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
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
	return s.ClaimRunLimited(ctx, ns, name, id, ttlMs, 0)
}

// ClaimRunLimited atomically enforces a definition-wide concurrency limit when
// limit is positive, while retaining the per-instance lease fence.
func (s *Store) ClaimRunLimited(ctx context.Context, ns, name, id string, ttlMs int64, limit int) (token string, generation uint64, status Status, ok bool, err error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return "", 0, "", false, err
	}
	var cur string
	var lease int64
	var gen uint64
	now := s.now().UnixMilli()
	var leased bool
	if _, err := s.writeWake(ctx, c, func(tx *sql.Tx) error {
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
		if limit > 0 {
			var active int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM instances WHERE status = ? AND lease_until_ms > ? AND id <> ?`, string(StatusRunning), now, id).Scan(&active); err != nil {
				return err
			}
			if active >= limit {
				return nil
			}
		}
		token = newRunToken()
		gen++
		if _, err := tx.ExecContext(ctx,
			`UPDATE instances SET run_token = ?, lease_until_ms = ?, generation = ?, status = ?, updated_ms = ? WHERE id = ?`,
			token, now+ttlMs, gen, string(StatusRunning), now, id); err != nil {
			return err
		}
		// A crashed attempt is retried after its lease expires, without relying
		// on an in-memory goroutine or another external event.
		if err := s.queueTx(ctx, tx, c, id, now+ttlMs); err != nil {
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
	removed := 0
	// Select and delete in one write transaction. Selecting first outside the
	// transaction (and then deleting by id) could remove an instance that a
	// concurrent Resume/Restart had just moved back to queued.
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			`SELECT id FROM instances WHERE status IN (?,?,?) AND updated_ms < ?`,
			string(StatusComplete), string(StatusErrored), string(StatusTerminated), beforeMs)
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
		for _, id := range ids {
			for _, tbl := range []string{"steps", "step_attempts", "events", "waits"} {
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE instance_id = ?", id); err != nil {
					return err
				}
			}
			res, err := tx.ExecContext(ctx,
				`DELETE FROM instances WHERE id=? AND status IN (?,?,?) AND updated_ms < ?`,
				id, string(StatusComplete), string(StatusErrored), string(StatusTerminated), beforeMs)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n > 0 {
				removed++
			}
		}
		return nil
	})
	return removed, err
}

// PruneRetention applies separate success/error retention windows. A zero
// window leaves that terminal status untouched. Deletes are batched so one
// large backlog cannot hold the definition's write lock for an unbounded time.
func (s *Store) PruneRetention(ctx context.Context, ns, name string, successBefore, errorBefore int64) (int, error) {
	const (
		batchSize  = 500
		maxBatches = 20
	)
	total := 0
	for i := 0; i < maxBatches; i++ {
		n, err := s.pruneRetentionBatch(ctx, ns, name, successBefore, errorBefore, batchSize)
		total += n
		if err != nil || n < batchSize {
			return total, err
		}
	}
	return total, nil
}

func (s *Store) pruneRetentionBatch(ctx context.Context, ns, name string, successBefore, errorBefore int64, limit int) (int, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return 0, err
	}
	removed := 0
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM instances WHERE (status=? AND ? > 0 AND updated_ms < ?) OR (status=? AND ? > 0 AND updated_ms < ?) LIMIT ?`, string(StatusComplete), successBefore, successBefore, string(StatusErrored), errorBefore, errorBefore, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, id := range ids {
			for _, tbl := range []string{"steps", "step_attempts", "events", "waits"} {
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE instance_id = ?", id); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM instances WHERE id = ?`, id); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
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
func (s *Store) SetAttempt(ctx context.Context, ns, name, id, step string, attempts int, lastErr string, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run,
		`INSERT INTO step_attempts(instance_id, name, attempts, last_error) VALUES(?,?,?,?)
		 ON CONFLICT(instance_id, name) DO UPDATE SET attempts = excluded.attempts, last_error = excluded.last_error`,
		id, step, attempts, lastErr)
	return err
}

// ClearAttempt removes a step's retry state (on success/restart).
func (s *Store) ClearAttempt(ctx context.Context, ns, name, id, step string, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run, `DELETE FROM step_attempts WHERE instance_id = ? AND name = ?`, id, step)
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
func (s *Store) ClearWait(ctx context.Context, ns, name, id, step string, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run, `DELETE FROM waits WHERE instance_id = ? AND name = ?`, id, step)
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
	return s.ConsumeEventForStep(ctx, ns, name, id, "", evType, "")
}

// ConsumeEventForStep commits consumption and its replayable step result together.
// An empty step is the legacy, non-replayable internal consumption API.
func (s *Store) ConsumeEventForStep(ctx context.Context, ns, name, id, step, evType, token string) ([]byte, bool, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, false, err
	}
	var payload []byte
	var found bool
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		if err := checkRunTx(ctx, tx, id, []string{token}); err != nil {
			return err
		}
		if step != "" {
			err := tx.QueryRowContext(ctx, `SELECT result FROM steps WHERE instance_id=? AND name=?`, id, step).Scan(&payload)
			if err == nil {
				found = true
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		var eventID int64
		err := tx.QueryRowContext(ctx, `SELECT id,payload FROM events WHERE instance_id=? AND consumed=0 AND (?='' OR CASE WHEN json_valid(CAST(payload AS TEXT)) THEN json_extract(CAST(payload AS TEXT),'$.type') END=?) ORDER BY id LIMIT 1`, id, evType, evType).Scan(&eventID, &payload)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE events SET consumed=1 WHERE id=?`, eventID); err != nil {
			return err
		}
		if step != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO steps(instance_id,name,result) VALUES(?,?,?)`, id, step, payload); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM waits WHERE instance_id=? AND name=?`, id, step); err != nil {
				return err
			}
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return payload, found, nil
}

// checkRunTx is deliberately inside the same write transaction as the mutation.
func checkRunTx(ctx context.Context, tx *sql.Tx, id string, run []string) error {
	if len(run) == 0 || run[0] == "" {
		return nil
	}
	var current string
	err := tx.QueryRowContext(ctx, `SELECT run_token FROM instances WHERE id=?`, id).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleRun
	}
	if err != nil {
		return err
	}
	if current != run[0] {
		return ErrStaleRun
	}
	return nil
}

func execRunCell(ctx context.Context, c *cellstore.Cell, id string, run []string, q string, args ...any) (int64, error) {
	var n int64
	_, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if err := checkRunTx(ctx, tx, id, run); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n, err
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
	return s.CreateWithTarget(ctx, ns, name, id, params, Target{})
}

func (s *Store) CreateWithTarget(ctx context.Context, ns, name, id string, params []byte, target Target) (Instance, error) {
	if id == "" {
		id = NewID()
	}
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return Instance{}, err
	}
	now := s.now().UnixMilli()
	if _, err := s.writeWake(ctx, c, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO instances(id, params, status, created_ms, updated_ms, target_worker, target_bundle, target_version, target_class) VALUES(?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO NOTHING`,
			id, params, string(StatusQueued), now, now, target.Worker, target.BundleSHA, target.Version, target.ClassName)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		return s.queueTx(ctx, tx, c, id, now)
	}); err != nil {
		return Instance{}, err
	}
	return s.Get(ctx, ns, name, id)
}

// CreateBatch inserts every instance in one cell transaction. A failure leaves
// the batch unchanged; non-empty IDs retain Create's idempotent semantics.
func (s *Store) CreateBatch(ctx context.Context, ns, name string, instances []Instance) ([]Instance, error) {
	return s.CreateBatchWithTarget(ctx, ns, name, instances, Target{})
}

func (s *Store) CreateBatchWithTarget(ctx context.Context, ns, name string, instances []Instance, target Target) ([]Instance, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	now := s.now().UnixMilli()
	ids := make([]string, len(instances))
	if _, err := s.writeWake(ctx, c, func(tx *sql.Tx) error {
		for i, in := range instances {
			id := in.ID
			if id == "" {
				id = NewID()
			}
			ids[i] = id
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO instances(id, params, status, created_ms, updated_ms, target_worker, target_bundle, target_version, target_class) VALUES(?,?,?,?,?,?,?,?,?)
				 ON CONFLICT(id) DO NOTHING`,
				id, in.Params, string(StatusQueued), now, now, target.Worker, target.BundleSHA, target.Version, target.ClassName); err != nil {
				return err
			}
			if err := s.queueTx(ctx, tx, c, id, now); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(ids))
	for _, id := range ids {
		in, err := s.Get(ctx, ns, name, id)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, nil
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
		`SELECT id, params, status, output, error, created_ms, updated_ms, target_worker, target_bundle, target_version, target_class FROM instances WHERE id = ?`, id).
		Scan(&in.ID, &in.Params, &status, &in.Output, &in.Error, &in.CreatedMs, &in.UpdatedMs, &in.TargetWorker, &in.TargetBundle, &in.TargetVersion, &in.TargetClass)
	if err != nil {
		return Instance{}, err
	}
	in.Status = Status(status)
	return in, nil
}

// List returns recent instances (operator/diagnostic use).
func (s *Store) List(ctx context.Context, ns, name string, limit int) ([]Instance, error) {
	return s.ListPage(ctx, ns, name, limit, 0, "", 0, 0)
}

// ListPage reads a stable newest-first page from the workflow cell.
func (s *Store) ListPage(ctx context.Context, ns, name string, limit, offset int, status Status, fromMs, toMs int64) ([]Instance, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		return nil, fmt.Errorf("negative workflow page offset")
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT id, params, status, output, error, created_ms, updated_ms, target_worker, target_bundle, target_version, target_class FROM instances
		 WHERE (?='' OR status=?) AND (?=0 OR created_ms>=?) AND (?=0 OR created_ms<=?)
		 ORDER BY created_ms DESC, id DESC LIMIT ? OFFSET ?`, string(status), string(status), fromMs, fromMs, toMs, toMs, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var in Instance
		var status string
		if err := rows.Scan(&in.ID, &in.Params, &status, &in.Output, &in.Error, &in.CreatedMs, &in.UpdatedMs, &in.TargetWorker, &in.TargetBundle, &in.TargetVersion, &in.TargetClass); err != nil {
			return nil, err
		}
		in.Status = Status(status)
		out = append(out, in)
	}
	return out, rows.Err()
}

// SetStatus updates an instance's status (and error text when provided).
func (s *Store) SetStatus(ctx context.Context, ns, name, id string, status Status, errText string, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run,
		`UPDATE instances SET status = ?, error = ?, updated_ms = ?, run_token = CASE WHEN ? THEN '' ELSE run_token END, lease_until_ms = CASE WHEN ? THEN 0 ELSE lease_until_ms END WHERE id = ?`,
		string(status), errText, s.now().UnixMilli(), status != StatusRunning, status != StatusRunning, id)
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
func (s *Store) SetOutput(ctx context.Context, ns, name, id string, output []byte, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run,
		`UPDATE instances SET status = ?, output = ?, updated_ms = ?, run_token = '', lease_until_ms = 0 WHERE id = ?`,
		string(StatusComplete), output, s.now().UnixMilli(), id)
	return err
}

// Pause/Resume/Terminate change instance status without touching steps.
func (s *Store) Pause(ctx context.Context, ns, name, id string) error {
	return s.SetStatus(ctx, ns, name, id, StatusPaused, "")
}

func (s *Store) Resume(ctx context.Context, ns, name, id string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = s.writeWake(ctx, c, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE instances SET status=?,updated_ms=? WHERE id=?`, string(StatusQueued), s.now().UnixMilli(), id); err != nil {
			return err
		}
		return s.queueTx(ctx, tx, c, id, s.now().UnixMilli())
	})
	return err
}

func (s *Store) Terminate(ctx context.Context, ns, name, id string) error {
	return s.SetStatus(ctx, ns, name, id, StatusTerminated, "")
}

// Restart clears all memoized steps and re-queues the instance from the start.
func (s *Store) Restart(ctx context.Context, ns, name, id string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	_, err = s.writeWake(ctx, c, func(tx *sql.Tx) error {
		for _, tbl := range []string{"steps", "step_attempts", "events", "waits"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+tbl+" WHERE instance_id = ?", id); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE instances SET status = ?, output = NULL, error = '', updated_ms = ?, run_token = '', lease_until_ms = 0 WHERE id = ?`,
			string(StatusQueued), now, id)
		if err != nil {
			return err
		}
		return s.queueTx(ctx, tx, c, id, now)
	})
	return err
}

// PutStep memoizes a step result (idempotent per (instance, step)).
func (s *Store) PutStep(ctx context.Context, ns, name, id, step string, result []byte, run ...string) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = execRunCell(ctx, c, id, run,
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
	_, err = s.writeWake(ctx, c, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(instance_id,payload,created_ms) VALUES(?,?,?)`, id, payload, s.now().UnixMilli()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE instances SET status=? WHERE id=? AND status=?`, string(StatusQueued), id, string(StatusSleeping)); err != nil {
			return err
		}
		return s.queueTx(ctx, tx, c, id, s.now().UnixMilli())
	})
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
