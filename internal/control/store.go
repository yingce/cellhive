package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

const (
	// ControlClass is the class of the control cell.
	ControlClass = "__control__"
	// GlobalNamespace owns the single control cell (ADR-117). App namespaces are
	// a column, not a scope: the control plane is one database.
	GlobalNamespace = "__platform__"
	// SystemNamespace is reserved and cannot be an app.
	SystemNamespace = "__system__"
)

var (
	// ErrReserved means the namespace is reserved.
	ErrReserved = errors.New("control: reserved namespace")
	// ErrNotFound means the control object does not exist.
	ErrNotFound = errors.New("control: not found")
	// ErrNoVersion means the worker version does not exist.
	ErrNoVersion = errors.New("control: no such version")
	// ErrNoPrevious means there is no previous version to roll back to.
	ErrNoPrevious = errors.New("control: no previous version")
	// ErrNoEnvelope means no secret root key is configured.
	ErrNoEnvelope = errors.New("control: secret root key not configured")
	// ErrIdempotencyConflict means a key was reused for a different deploy.
	ErrIdempotencyConflict = errors.New("control: idempotency key reused with different deploy parameters")
)

// Store is the control-plane metadata store. All control metadata lives in one
// cell (ADR-117) and is stored in relational tables: apps, workers, versions,
// routes, resources, secrets, audit, do_classes, delete_locks, gc_marks and
// deploy_idempotency. Version payloads that are open-ended (bindings, vars,
// consumers, crons, assets) stay as JSON columns.
type Store struct {
	cs  *cellstore.Store
	env *Envelope
	now func() time.Time

	mu         sync.Mutex
	migrated   map[*cellstore.Cell]bool
	resourceMu sync.Mutex
	// Set only by fault tests to stop after all deploy rows are
	// written but before Cell.Tx commits them.
	onDeployBeforeCommit func()
	// Lifecycle hooks run after the metadata transaction but before its caller
	// acknowledges it. Coordination failures must be returned to the caller.
	OnResourceActivated func(context.Context, string, string, string) error
	OnResourceRevoked   func(context.Context, string, string, string) error
	ResourceGeneration  func(context.Context, cell.Scope) (string, error)
}

// SetDeployBeforeCommitForTest installs the process-death barrier used by fault
// tests. Production callers leave it unset.
func (s *Store) SetDeployBeforeCommitForTest(hook func()) {
	s.onDeployBeforeCommit = hook
}

// New builds a control store. env may be nil when secrets are unused.
func New(cs *cellstore.Store, env *Envelope) *Store {
	return &Store{cs: cs, env: env, now: time.Now, migrated: map[*cellstore.Cell]bool{}}
}

// Scope is the single control cell scope (ADR-117).
func Scope() cell.Scope {
	return cell.Scope{Namespace: GlobalNamespace, Class: ControlClass, ID: "main"}
}

// ScopeFor is retained for callers that pass a namespace. Since ADR-117 the
// control plane is one cell, so the namespace is a key dimension, not a scope.
func ScopeFor(string) cell.Scope { return Scope() }

// GlobalScope is an alias of Scope (ADR-117).
func GlobalScope() cell.Scope { return Scope() }

func validateNS(ns string) error {
	if ns == "" || ns == GlobalNamespace || ns == SystemNamespace {
		return ErrReserved
	}
	if strings.ContainsAny(ns, "/\\") {
		return fmt.Errorf("control: invalid namespace %q", ns)
	}
	return nil
}

// validateScopeComponent applies the cell.Scope component rules (ADR-002: a
// cell is <ns>/<class>/<id>, so each component is one path element).
func validateScopeComponent(what, v string) error {
	if v == "" {
		return fmt.Errorf("control: %s is required", what)
	}
	// A component becomes a directory/file name (cells/<ns>/<class>/<id>.db) and
	// part of the "<ns>/<class>/<id>" protocol string.
	if v == "." || v == ".." || strings.Contains(v, "..") {
		return fmt.Errorf("control: invalid %s %q: must not contain a path traversal component", what, v)
	}
	if strings.ContainsAny(v, "/\\") {
		return fmt.Errorf("control: invalid %s %q: must be a single path component", what, v)
	}
	return nil
}

// validateResourceName rejects resource names that cannot address a cell: the
// name is the default cell id, the registry key and the binding/token name, so a
// separator would register a resource that every data path, stats and even
// delete reject afterwards (ADR-269). Use --scope to decouple the name from the
// physical cell id when a different id is needed.
func validateResourceName(name string) error {
	return validateScopeComponent("resource name", name)
}

// validateResourceScope accepts both registry scope shapes: a full cell scope
// ("<ns>/__<kind>__/<id>") or a bare cell id ("sessions", the pre-scope form).
// Either shape must address a cell; a malformed scope otherwise leaves the
// resource unreadable and undeletable. An empty scope is allowed for resources
// that only carry config (e.g. Hyperdrive) or are declared without an id; the
// data path rejects those when an id is required (ADR-097). Namespace/class
// boundaries remain enforced by the data path, not here.
func validateResourceScope(scope string) error {
	if scope == "" {
		return nil
	}
	if strings.Contains(scope, "/") {
		if _, err := cell.ParseScope(scope); err != nil {
			return fmt.Errorf("control: invalid resource scope %q: %w", scope, err)
		}
		return nil
	}
	return validateScopeComponent("resource scope", scope)
}

const schema = `
CREATE TABLE IF NOT EXISTS apps (
  ns         TEXT PRIMARY KEY,
  created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS workers (
  ns         TEXT NOT NULL,
  name       TEXT NOT NULL,
  active     INTEGER NOT NULL DEFAULT 0,
  previous   INTEGER NOT NULL DEFAULT 0,
  storage_id TEXT NOT NULL DEFAULT '',
  host_label TEXT NOT NULL DEFAULT '',
  created_ms INTEGER NOT NULL DEFAULT 0,
  deleted_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE TABLE IF NOT EXISTS versions (
  ns             TEXT NOT NULL,
  worker         TEXT NOT NULL,
  number         INTEGER NOT NULL,
  bundle_sha     TEXT NOT NULL,
  assets_sha     TEXT NOT NULL DEFAULT '',
  storage_id     TEXT NOT NULL DEFAULT '',
  session_policy TEXT NOT NULL DEFAULT '',
  bindings       TEXT NOT NULL DEFAULT '[]',
  vars           TEXT NOT NULL DEFAULT '{}',
  consumers      TEXT NOT NULL DEFAULT '[]',
  crons          TEXT NOT NULL DEFAULT '[]',
  assets         TEXT,
  limits         TEXT NOT NULL DEFAULT '{}',
  created_ms     INTEGER NOT NULL,
  actor          TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number)
);
CREATE TABLE IF NOT EXISTS deployment_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ns         TEXT NOT NULL,
  worker     TEXT NOT NULL,
  version    INTEGER NOT NULL,
  actor      TEXT NOT NULL DEFAULT '',
  created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS deployment_events_worker ON deployment_events(ns, worker, id DESC);
CREATE TABLE IF NOT EXISTS version_bundle_refs (
  ns     TEXT NOT NULL,
  worker TEXT NOT NULL,
  number INTEGER NOT NULL,
  sha    TEXT NOT NULL,
  PRIMARY KEY (ns, worker, number, sha)
);
CREATE TABLE IF NOT EXISTS routes (
  host   TEXT NOT NULL,
  path   TEXT NOT NULL DEFAULT '',
  ns     TEXT NOT NULL,
  worker TEXT NOT NULL,
  PRIMARY KEY (host, path)
);
CREATE TABLE IF NOT EXISTS resources (
  ns         TEXT NOT NULL,
  kind       TEXT NOT NULL,
  name       TEXT NOT NULL,
  scope      TEXT NOT NULL DEFAULT '',
  created_ms INTEGER NOT NULL,
  -- Sealed resource config (e.g. a Hyperdrive origin connection string): only
  -- the wrapped DEK + ciphertext are stored (ADR-129), never plaintext.
  cfg_dek    BLOB,
  cfg_nonce  BLOB,
  cfg_ct     BLOB,
  PRIMARY KEY (ns, kind, name)
);
CREATE TABLE IF NOT EXISTS secrets (
  ns          TEXT NOT NULL,
  worker      TEXT NOT NULL,
  key         TEXT NOT NULL,
  wrapped_dek BLOB,
  value_nonce BLOB,
  ciphertext  BLOB,
  updated_ms  INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, key)
);
CREATE TABLE IF NOT EXISTS version_secrets (
  ns          TEXT NOT NULL,
  worker      TEXT NOT NULL,
  number      INTEGER NOT NULL,
  key         TEXT NOT NULL,
  wrapped_dek BLOB NOT NULL,
  ciphertext  BLOB NOT NULL,
  updated_ms  INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, number, key)
);
CREATE TABLE IF NOT EXISTS audit (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  ns     TEXT NOT NULL,
  at_ms  INTEGER NOT NULL,
  actor  TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_ns ON audit(ns, at_ms);
CREATE TABLE IF NOT EXISTS do_classes (
  ns            TEXT NOT NULL,
  worker        TEXT NOT NULL,
  code_class    TEXT NOT NULL,
  storage_class TEXT NOT NULL DEFAULT '',
  deleted       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, worker, code_class)
);
CREATE TABLE IF NOT EXISTS delete_locks (
  ns     TEXT NOT NULL,
  worker TEXT NOT NULL,
  actor  TEXT NOT NULL DEFAULT '',
  at_ms  INTEGER NOT NULL,
  exp_ms INTEGER NOT NULL,
  PRIMARY KEY (ns, worker)
);
CREATE TABLE IF NOT EXISTS gc_marks (
  kind      TEXT NOT NULL,
  id        TEXT NOT NULL,
  marked_ms INTEGER NOT NULL,
  PRIMARY KEY (kind, id)
);
CREATE TABLE IF NOT EXISTS deploy_idempotency (
  ns      TEXT NOT NULL,
  worker  TEXT NOT NULL,
  key     TEXT NOT NULL,
  version INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, key)
);
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(k, v) VALUES ('rev', '0');
`

// schemaV2 holds the objects added by ADR-131 (hosts/verification, normalized
// bindings, purge jobs, per-namespace revision). Columns added to pre-existing
// tables are applied by migrateColumns so an existing control cell upgrades in
// place (no rebuild required).
const schemaV2 = `
CREATE TABLE IF NOT EXISTS hosts (
  host       TEXT PRIMARY KEY,
  ns         TEXT NOT NULL,
  worker     TEXT NOT NULL DEFAULT '',         -- builtin: the worker; custom: ''
  kind       TEXT NOT NULL DEFAULT 'custom',   -- builtin|custom
  created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS hosts_ns ON hosts(ns);

CREATE TABLE IF NOT EXISTS bindings (
  ns          TEXT NOT NULL,
  worker      TEXT NOT NULL,
  number      INTEGER NOT NULL,
  name        TEXT NOT NULL,
  type        TEXT NOT NULL,
  id          TEXT NOT NULL DEFAULT '',
  class_name  TEXT NOT NULL DEFAULT '',
  entrypoint  TEXT NOT NULL DEFAULT '',
  pin_sha     TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number, name)
);
CREATE INDEX IF NOT EXISTS bindings_lookup ON bindings(ns, type, name);
CREATE INDEX IF NOT EXISTS bindings_pin    ON bindings(ns, id, pin_sha);

CREATE TABLE IF NOT EXISTS purges (
  ns           TEXT NOT NULL,
  worker       TEXT NOT NULL DEFAULT '',
  state        TEXT NOT NULL DEFAULT 'pending',
  requested_ms INTEGER NOT NULL,
  updated_ms   INTEGER NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker)
);
CREATE TABLE IF NOT EXISTS resource_cleanups (
  ns TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  job_id TEXT NOT NULL, data_scope TEXT NOT NULL,
  source_generation TEXT NOT NULL DEFAULT '',
  dead_letters TEXT NOT NULL DEFAULT '[]',
  state TEXT NOT NULL DEFAULT 'pending', requested_ms INTEGER NOT NULL,
  updated_ms INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '', next_retry_ms INTEGER NOT NULL DEFAULT 0,
  lease_token TEXT NOT NULL DEFAULT '', lease_until_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(ns,kind,name)
);
CREATE TABLE IF NOT EXISTS queue_transfer_destinations (
  ns TEXT NOT NULL, source_queue TEXT NOT NULL, source_generation TEXT NOT NULL,
  destination_queue TEXT NOT NULL, PRIMARY KEY(ns,source_queue,source_generation,destination_queue)
);
CREATE TABLE IF NOT EXISTS resource_retirements (
  ns TEXT NOT NULL, data_scope TEXT PRIMARY KEY, job_id TEXT NOT NULL,
  epoch_floor INTEGER NOT NULL, gc_cursor TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '', updated_ms INTEGER NOT NULL
);

-- Target-side allowlist for service bindings (ADR-144): (ns, worker) may be
-- bound from callers in caller_ns. Same-namespace bindings do not need a row.
CREATE TABLE IF NOT EXISTS service_acls (
  ns          TEXT NOT NULL,
  worker      TEXT NOT NULL,
  caller_ns   TEXT NOT NULL,
  created_ms  INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, caller_ns)
);
CREATE INDEX IF NOT EXISTS service_acls_caller ON service_acls(caller_ns, ns, worker);

CREATE TABLE IF NOT EXISTS rollback_idempotency (
  ns TEXT NOT NULL, worker TEXT NOT NULL, key TEXT NOT NULL,
  active INTEGER NOT NULL, previous INTEGER NOT NULL, created_ms INTEGER NOT NULL,
  PRIMARY KEY(ns, worker, key)
);

-- Revoked resources (ADR-156): a tombstone that overrides version-derived
-- binding declarations, so a revoke takes effect even though immutable version
-- metadata still mentions the binding. Re-registering clears it.
CREATE TABLE IF NOT EXISTS revoked_resources (
  ns          TEXT NOT NULL,
  kind        TEXT NOT NULL,
  name        TEXT NOT NULL,
  revoked_ms  INTEGER NOT NULL,
  PRIMARY KEY (ns, kind, name)
);

CREATE INDEX IF NOT EXISTS routes_ns        ON routes(ns, host, path);
CREATE INDEX IF NOT EXISTS routes_ns_worker ON routes(ns, worker);
CREATE INDEX IF NOT EXISTS versions_assets  ON versions(assets_sha) WHERE assets_sha <> '';
CREATE INDEX IF NOT EXISTS bundle_refs_sha  ON version_bundle_refs(sha);
CREATE INDEX IF NOT EXISTS resources_kind   ON resources(kind, ns, name);
CREATE INDEX IF NOT EXISTS audit_actor      ON audit(actor, at_ms);
CREATE UNIQUE INDEX IF NOT EXISTS workers_host_label ON workers(host_label) WHERE host_label <> '';
`

// migrateColumns adds columns introduced after a cell was first created. SQLite
// cannot add a column that already exists, so the current set is read first.
func migrateColumns(ctx context.Context, db *sql.DB) error {
	cols := map[string][]string{
		"apps":               {"deleted_ms INTEGER NOT NULL DEFAULT 0"},
		"workers":            {"host_label TEXT NOT NULL DEFAULT ''", "created_ms INTEGER NOT NULL DEFAULT 0", "deleted_ms INTEGER NOT NULL DEFAULT 0"},
		"versions":           {"compat_date TEXT NOT NULL DEFAULT ''", "compat_flags TEXT NOT NULL DEFAULT '[]'", "limits TEXT NOT NULL DEFAULT '{}'", "log_policy TEXT NOT NULL DEFAULT '{}'", "secret_snapshot INTEGER NOT NULL DEFAULT 0"},
		"deploy_idempotency": {"created_ms INTEGER NOT NULL DEFAULT 0", "fingerprint TEXT NOT NULL DEFAULT ''"},
		"audit":              {"actor_kind TEXT NOT NULL DEFAULT ''", "on_behalf_of TEXT NOT NULL DEFAULT ''", "request_id TEXT NOT NULL DEFAULT ''"},
	}
	for table, defs := range cols {
		have := map[string]bool{}
		rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var cid int
			var name, typ string
			var notnull, pk int
			var dflt any
			if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
				rows.Close()
				return err
			}
			have[name] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, def := range defs {
			name := strings.Fields(def)[0]
			if have[name] {
				continue
			}
			if _, err := db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+def); err != nil {
				return fmt.Errorf("control: migrate %s.%s: %w", table, name, err)
			}
		}
	}
	return nil
}

// cell returns the single control cell, creating/migrating it on first use.
func (s *Store) cell(ctx context.Context) (*cellstore.Cell, error) {
	// A cellstore eviction can close the cell between Cell() and our lazy
	// migration (the control cell used to be evictable). Re-fetch and retry
	// once; a pinned control cell makes this a rare fallback (ADR-272).
	for attempt := 0; attempt < 2; attempt++ {
		c, err := s.cs.Cell(ctx, Scope())
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		done := s.migrated[c]
		s.mu.Unlock()
		if done {
			if !c.Closed() {
				return c, nil
			}
			continue
		}
		if c.Closed() {
			continue
		}
		if err := s.migrateCell(ctx, c); err != nil {
			if c.Closed() {
				continue
			}
			return nil, err
		}
		if c.Closed() {
			continue
		}
		s.mu.Lock()
		s.migrated[c] = true
		s.mu.Unlock()
		return c, nil
	}
	return nil, fmt.Errorf("control: cell closed while migrating")
}

// migrateCell installs the control schema on a freshly opened cell. It is
// idempotent and safe to retry on a new Cell instance.
func (s *Store) migrateCell(ctx context.Context, c *cellstore.Cell) error {
	// Install the idempotent schema WITHOUT advancing the cell txid: a control
	// cell reopen (restart/takeover) must not leave the local cell one txid
	// ahead of the durable chain, which would force a full capture snapshot on
	// the next write (ADR-272). Schema DDL is idempotent and reruns on crash.
	if _, err := c.DB.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("control: migrate: %w", err)
	}
	// Columns added to legacy tables must exist before schemaV2's indexes.
	if err := migrateColumns(ctx, c.DB); err != nil {
		return err
	}
	if _, err := c.DB.ExecContext(ctx, schemaV2); err != nil {
		return fmt.Errorf("control: migrate: %w", err)
	}
	// Historical secret values cannot be reconstructed. Pin the values known
	// at upgrade time once, before any new version or secret write occurs.
	var pending int
	if err := c.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM versions WHERE secret_snapshot=0)`).Scan(&pending); err != nil {
		return fmt.Errorf("control: inspect secret snapshots: %w", err)
	}
	if pending != 0 {
		if err := tx(ctx, c, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, `INSERT OR IGNORE INTO version_secrets
				(ns,worker,number,key,wrapped_dek,ciphertext,updated_ms)
				SELECT v.ns,v.worker,v.number,s.key,s.wrapped_dek,s.ciphertext,s.updated_ms
				FROM versions v JOIN secrets s ON s.ns=v.ns AND s.worker=v.worker
				WHERE v.secret_snapshot=0`); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, `UPDATE versions SET secret_snapshot=1 WHERE secret_snapshot=0`)
			return err
		}); err != nil {
			return fmt.Errorf("control: migrate secret snapshots: %w", err)
		}
	}
	return nil
}

// tx runs fn in one control transaction and bumps the revision.
// tx runs one control write in a cell transaction. It uses Cell.Tx (which
// advances the cell txid) so capture's durability watermark covers the write
// before a caller is acknowledged (RPO=0), and bumps the projection revision in
// the same transaction.
func tx(ctx context.Context, c *cellstore.Cell, fn func(tx *sql.Tx) error) error {
	_, err := c.Tx(ctx, func(t *sql.Tx) error {
		if err := fn(t); err != nil {
			return err
		}
		_, err := t.ExecContext(ctx, `UPDATE meta SET v = CAST(v AS INTEGER) + 1 WHERE k = 'rev'`)
		return err
	})
	return err
}

// Rev returns the control store's revision; it changes on every control write.
func (s *Store) Rev(ctx context.Context) (int64, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return 0, err
	}
	var v int64
	if err := c.DB.QueryRowContext(ctx, `SELECT CAST(v AS INTEGER) FROM meta WHERE k='rev'`).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func marshal(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func jsonOr(v any, def string) string {
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" {
		return def
	}
	return string(b)
}

// Actor carries request attribution for the audit trail (ADR-131). The admin
// middleware puts it on the request context; store writes pick it up so the
// audit row records who acted (JWT sub), how (user/service/static) and on whose
// behalf, without threading it through every signature.
type Actor struct {
	Sub        string
	Kind       string // user|service|static|system
	OnBehalfOf string
	RequestID  string
}

type actorCtxKey struct{}

// WithActor attaches request attribution to ctx.
func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, actorCtxKey{}, a)
}

// ActorFrom returns the attribution attached to ctx, if any.
func ActorFrom(ctx context.Context) (Actor, bool) {
	a, ok := ctx.Value(actorCtxKey{}).(Actor)
	return a, ok
}

// txAudit records one control-plane write. When the context carries an Actor the
// row is attributed to it; the legacy actor argument is the fallback.
func txAudit(ctx context.Context, t *sql.Tx, ns, actor, action, target string) error {
	var kind, behalf, reqID string
	if a, ok := ActorFrom(ctx); ok {
		if a.Sub != "" {
			actor = a.Sub
		}
		kind, behalf, reqID = a.Kind, a.OnBehalfOf, a.RequestID
	}
	_, err := t.ExecContext(ctx,
		`INSERT INTO audit(ns, at_ms, actor, actor_kind, on_behalf_of, request_id, action, target)
		 VALUES (?,?,?,?,?,?,?,?)`,
		ns, time.Now().UnixMilli(), actor, kind, behalf, reqID, action, target)
	return err
}

// --- apps -------------------------------------------------------------------

// CreateApp registers an app namespace (idempotent).
func (s *Store) CreateApp(ctx context.Context, ns, actor string) (App, error) {
	if err := validateNS(ns); err != nil {
		return App{}, err
	}
	app := App{Namespace: ns, CreatedMs: s.now().UnixMilli()}
	c, err := s.cell(ctx)
	if err != nil {
		return App{}, err
	}
	err = tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx,
			`INSERT INTO apps(ns, created_ms) VALUES (?,?) ON CONFLICT(ns) DO NOTHING`,
			ns, app.CreatedMs); err != nil {
			return err
		}
		// Creating an app resurrects a soft-deleted one and cancels its purge.
		if _, err := t.ExecContext(ctx, `UPDATE apps SET deleted_ms=0 WHERE ns=?`, ns); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM purges WHERE ns=? AND worker=''`, ns); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "app.create", ns)
	})
	return app, err
}

// AppActive reports whether ns has an app row that is not soft-deleted. It is a
// read-only check: a deploy uses it to skip the idempotent CreateApp write (and
// its durability proof) when the app is already live (ADR-271).
func (s *Store) AppActive(ctx context.Context, ns string) (bool, error) {
	if err := validateNS(ns); err != nil {
		return false, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return false, err
	}
	var deletedMs int64
	err = c.DB.QueryRowContext(ctx, `SELECT deleted_ms FROM apps WHERE ns=?`, ns).Scan(&deletedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return deletedMs == 0, nil
}

// GetApp returns an app record.
func (s *Store) GetApp(ctx context.Context, ns string) (App, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return App{}, err
	}
	var app App
	err = c.DB.QueryRowContext(ctx, `SELECT ns, created_ms FROM apps WHERE ns=?`, ns).
		Scan(&app.Namespace, &app.CreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return App{}, ErrNotFound
	}
	return app, err
}

// Apps returns the registered apps, sorted by namespace.
func (s *Store) Apps(ctx context.Context) ([]App, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT ns, created_ms FROM apps WHERE deleted_ms=0 ORDER BY ns`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []App
	for rows.Next() {
		var a App
		if err := rows.Scan(&a.Namespace, &a.CreatedMs); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteApp soft-deletes an app (ADR-131): stop routing immediately, then let the
// purge worker remove the control rows (and, once wired, ask the owners to drop
// data cells). Idempotent.
func (s *Store) DeleteApp(ctx context.Context, ns, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `UPDATE apps SET deleted_ms=? WHERE ns=?`, now, ns); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `UPDATE workers SET deleted_ms=? WHERE ns=?`, now, ns); err != nil {
			return err
		}
		// Stop traffic at once; the purge worker removes the rest.
		if _, err := t.ExecContext(ctx, `DELETE FROM routes WHERE ns=?`, ns); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM hosts WHERE ns=? AND kind='builtin'`, ns); err != nil {
			return err
		}
		if err := enqueuePurgeTx(ctx, t, ns, "", now); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "app.delete", ns)
	})
}

// PurgeAppRows hard-deletes an app's control metadata (called by the purge
// worker once the purge job is claimed).
func (s *Store) PurgeAppRows(ctx context.Context, ns string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var deletedMs int64
		err := t.QueryRowContext(ctx, `SELECT deleted_ms FROM apps WHERE ns=?`, ns).Scan(&deletedMs)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil // already purged: idempotent
		case err != nil:
			return err
		case deletedMs == 0:
			return nil // resurrected since the job was queued
		}
		for _, stmt := range []string{
			`DELETE FROM apps WHERE ns=?`,
			`DELETE FROM workers WHERE ns=?`,
			`DELETE FROM versions WHERE ns=?`,
			`DELETE FROM bindings WHERE ns=?`,
			`DELETE FROM version_bundle_refs WHERE ns=?`,
			`DELETE FROM routes WHERE ns=?`,
			`DELETE FROM hosts WHERE ns=?`,
			`DELETE FROM resources WHERE ns=?`,
			`DELETE FROM resource_cleanups WHERE ns=?`,
			`DELETE FROM queue_transfer_destinations WHERE ns=?`,
			`DELETE FROM secrets WHERE ns=?`,
			`DELETE FROM version_secrets WHERE ns=?`,
			`DELETE FROM do_classes WHERE ns=?`,
			`DELETE FROM delete_locks WHERE ns=?`,
			`DELETE FROM deploy_idempotency WHERE ns=?`,
			`DELETE FROM rollback_idempotency WHERE ns=?`,
		} {
			if _, err := t.ExecContext(ctx, stmt, ns); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- resources --------------------------------------------------------------

// CreateResource registers a resource (no auto-provisioning).
func (s *Store) CreateResource(ctx context.Context, ns, kind, name, scope, actor string) (Resource, error) {
	return s.CreateResourceWithConfig(ctx, ns, kind, name, scope, nil, actor)
}

// CreateResourceWithConfig registers a resource, optionally with a sealed
// config blob (e.g. a Hyperdrive origin connection string). A non-empty config
// requires the envelope (root key); plaintext is never stored (ADR-129).
func (s *Store) CreateResourceWithConfig(ctx context.Context, ns, kind, name, scope string, config []byte, actor string) (Resource, error) {
	s.resourceMu.Lock()
	defer s.resourceMu.Unlock()
	if err := validateNS(ns); err != nil {
		return Resource{}, err
	}
	if kind == "" || name == "" {
		return Resource{}, fmt.Errorf("control: resource kind and name are required")
	}
	if err := validateResourceName(name); err != nil {
		return Resource{}, err
	}
	if err := validateResourceScope(scope); err != nil {
		return Resource{}, err
	}
	var dek, ct []byte
	if len(config) > 0 {
		if s.env == nil {
			return Resource{}, ErrNoEnvelope
		}
		var err error
		if dek, ct, err = s.env.Seal(config); err != nil {
			return Resource{}, err
		}
	}
	r := Resource{Kind: kind, Name: name, Scope: scope, CreatedMs: s.now().UnixMilli()}
	c, err := s.cell(ctx)
	if err != nil {
		return Resource{}, err
	}
	err = tx(ctx, c, func(t *sql.Tx) error {
		// Registering clears any revoke tombstone (ADR-156).
		if kind == "kv" || kind == "queue" {
			physical, err := ResourceDataScope(ns, kind, name, scope)
			physicalKey := ""
			if err == nil {
				physicalKey = physical.String()
			}
			var count int
			if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_cleanups WHERE state<>'complete' AND ns=? AND ((kind=? AND name=?) OR (data_scope=? AND ?<>''))`, ns, kind, name, physicalKey, physicalKey).Scan(&count); err != nil {
				return err
			}
			if count > 0 {
				return ErrCleanupPending
			}
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM revoked_resources WHERE ns=? AND kind=? AND name=?`, ns, kind, name); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx,
			`INSERT INTO resources(ns, kind, name, scope, created_ms, cfg_dek, cfg_nonce, cfg_ct)
			 VALUES (?,?,?,?,?,?,?,?)
			 ON CONFLICT(ns, kind, name) DO UPDATE SET scope=excluded.scope,
			   cfg_dek=excluded.cfg_dek, cfg_nonce=excluded.cfg_nonce, cfg_ct=excluded.cfg_ct`,
			ns, kind, name, scope, r.CreatedMs, dek, nil, ct); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "resource.create", kind+"/"+name)
	})
	if err == nil && s.OnResourceActivated != nil {
		err = s.OnResourceActivated(ctx, ns, kind, name)
	}
	return r, err
}

// GetResource returns one registered resource.
func (s *Store) GetResource(ctx context.Context, ns, kind, name string) (Resource, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return Resource{}, err
	}
	r := Resource{Kind: kind, Name: name}
	err = c.DB.QueryRowContext(ctx,
		`SELECT scope, created_ms FROM resources WHERE ns=? AND kind=? AND name=?`,
		ns, kind, name).Scan(&r.Scope, &r.CreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Resource{}, ErrNotFound
	}
	return r, err
}

// ResourceConfig returns a decrypted resource config blob (e.g. a Hyperdrive
// origin connection string). ErrNotFound when the resource or its config is
// absent; ErrNoEnvelope when the root key is not configured (ADR-129).
func (s *Store) ResourceConfig(ctx context.Context, ns, kind, name string) ([]byte, error) {
	if s.env == nil {
		return nil, ErrNoEnvelope
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	var dek, ct []byte
	err = c.DB.QueryRowContext(ctx,
		`SELECT cfg_dek, cfg_ct FROM resources WHERE ns=? AND kind=? AND name=?`,
		ns, kind, name).Scan(&dek, &ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(dek) == 0 || len(ct) == 0 {
		return nil, ErrNotFound
	}
	return s.env.Open(dek, ct)
}

// ResourceRef is a compact resource reference.
type ResourceRef struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Scope     string `json:"scope"`
}

// Resources lists an app's resources, optionally filtered by kind.
func (s *Store) Resources(ctx context.Context, ns, kind string) ([]ResourceRef, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	q := `SELECT ns, kind, name, scope FROM resources WHERE ns=?`
	args := []any{ns}
	if kind != "" {
		q += ` AND kind=?`
		args = append(args, kind)
	}
	q += ` ORDER BY kind, name`
	return s.scanResources(ctx, c, q, args...)
}

// ResourcesByKind lists every app's resources of one kind (queue consumer loop).
func (s *Store) ResourcesByKind(ctx context.Context, kind string) ([]ResourceRef, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	return s.scanResources(ctx, c,
		`SELECT ns, kind, name, scope FROM resources WHERE kind=? ORDER BY ns, name`, kind)
}

func (s *Store) scanResources(ctx context.Context, c *cellstore.Cell, q string, args ...any) ([]ResourceRef, error) {
	rows, err := c.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResourceRef
	for rows.Next() {
		var r ResourceRef
		if err := rows.Scan(&r.Namespace, &r.Kind, &r.Name, &r.Scope); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// HasBinding reports whether a binding declaration exists.
func (s *Store) HasBinding(ctx context.Context, ns, kind, name string) (bool, error) {
	_, ok, err := s.Binding(ctx, ns, kind, name)
	return ok, err
}

// Binding resolves a binding declaration to its id/scope: a registered
// resource, or (ADR-061) a binding declared by a worker's active version.
func (s *Store) Binding(ctx context.Context, ns, kind, name string) (string, bool, error) {
	if err := validateNS(ns); err != nil {
		return "", false, nil
	}
	c, err := s.cell(ctx)
	if err != nil {
		return "", false, err
	}
	// A revoked resource never resolves, even if an immutable version still
	// declares the binding (ADR-156).
	var revoked int
	if rerr := c.DB.QueryRowContext(ctx,
		`SELECT 1 FROM revoked_resources WHERE ns=? AND kind=? AND name=?`, ns, kind, name).Scan(&revoked); rerr == nil {
		return "", false, nil
	} else if !errors.Is(rerr, sql.ErrNoRows) {
		return "", false, rerr
	}
	var scope string
	err = c.DB.QueryRowContext(ctx,
		`SELECT scope FROM resources WHERE ns=? AND kind=? AND name=?`, ns, kind, name).Scan(&scope)
	switch {
	case err == nil:
		return scope, true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", false, err
	}
	// Declared by a deployed worker version: the normalized bindings table first
	// (ADR-131), falling back to the JSON column for pre-migration versions.
	var declared string
	err = c.DB.QueryRowContext(ctx,
		`SELECT b.id FROM bindings b JOIN workers w
		   ON b.ns=w.ns AND b.worker=w.name AND b.number=w.active
		 WHERE b.ns=? AND b.type=? AND b.name=?`, ns, kind, name).Scan(&declared)
	switch {
	case err == nil:
		return declared, true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", false, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT v.bindings FROM versions v JOIN workers w
		   ON v.ns=w.ns AND v.worker=w.name AND v.number=w.active
		 WHERE w.ns=?`, ns)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return "", false, err
		}
		var bindings []Binding
		if err := unmarshal([]byte(raw), &bindings); err != nil {
			continue
		}
		for _, b := range bindings {
			if b.Type == kind && b.Name == name {
				return b.ID, true, nil
			}
		}
	}
	return "", false, rows.Err()
}

// --- workers / versions ------------------------------------------------------

func scanVersion(scan func(dest ...any) error) (Version, error) {
	var v Version
	var bindings, vars, consumers, crons string
	var assets sql.NullString
	var flags, limits string
	var number int
	if err := scan(&number, &v.BundleSHA, &v.AssetsSHA, &v.StorageID, &v.SessionPolicy,
		&bindings, &vars, &consumers, &crons, &assets, &v.CompatDate, &flags, &limits, &v.CreatedMs, &v.Actor); err != nil {
		return Version{}, err
	}
	if flags != "" {
		_ = unmarshal([]byte(flags), &v.CompatFlags)
	}
	if limits != "" && limits != "{}" {
		var l WorkerLimits
		if err := unmarshal([]byte(limits), &l); err != nil {
			return Version{}, err
		}
		v.Limits = &l
	}
	v.Number = number
	if err := unmarshal([]byte(bindings), &v.Bindings); err != nil {
		return Version{}, err
	}
	if err := unmarshal([]byte(vars), &v.Vars); err != nil {
		return Version{}, err
	}
	if err := unmarshal([]byte(consumers), &v.Consumers); err != nil {
		return Version{}, err
	}
	if err := unmarshal([]byte(crons), &v.Crons); err != nil {
		return Version{}, err
	}
	if assets.Valid && assets.String != "" && assets.String != "null" {
		var a AssetsConfig
		if err := unmarshal([]byte(assets.String), &a); err != nil {
			return Version{}, err
		}
		v.Assets = &a
	}
	return v, nil
}

const versionCols = `number, bundle_sha, assets_sha, storage_id, session_policy, bindings, vars, consumers, crons, assets, compat_date, compat_flags, limits, created_ms, actor`

// versionColsAliased is versionCols qualified with the versions-table alias "v"
// for queries that join workers (both tables have storage_id).
const versionColsAliased = `v.number, v.bundle_sha, v.assets_sha, v.storage_id, v.session_policy, v.bindings, v.vars, v.consumers, v.crons, v.assets, v.compat_date, v.compat_flags, v.limits, v.created_ms, v.actor`

func (s *Store) getVersion(ctx context.Context, c *cellstore.Cell, ns, worker string, number int) (Version, bool, error) {
	row := c.DB.QueryRowContext(ctx,
		`SELECT `+versionCols+` FROM versions WHERE ns=? AND worker=? AND number=?`, ns, worker, number)
	v, err := scanVersion(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Version{}, false, nil
	}
	if err != nil {
		return Version{}, false, err
	}
	return v, true, nil
}

// activeVersion returns a worker's active version row.
func (s *Store) activeVersion(ctx context.Context, c *cellstore.Cell, ns, worker string) (Version, bool, error) {
	row := c.DB.QueryRowContext(ctx,
		`SELECT `+versionColsAliased+` FROM versions v JOIN workers w
		   ON v.ns=w.ns AND v.worker=w.name AND v.number=w.active
		 WHERE w.ns=? AND w.name=?`, ns, worker)
	v, err := scanVersion(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Version{}, false, nil
	}
	if err != nil {
		return Version{}, false, err
	}
	return v, true, nil
}

// DOStorageID returns (allocating and persisting on first use) a worker's
// stable Durable Object storage id (ADR-081).
func (s *Store) DOStorageID(ctx context.Context, ns, worker string) (string, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return "", err
	}
	var id string
	err = tx(ctx, c, func(t *sql.Tx) error {
		err := t.QueryRowContext(ctx, `SELECT storage_id FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if id != "" {
			return nil
		}
		id = newStorageID()
		_, err = t.ExecContext(ctx,
			`INSERT INTO workers(ns, name, active, previous, storage_id) VALUES (?,?,0,0,?)
			 ON CONFLICT(ns, name) DO UPDATE SET storage_id=excluded.storage_id`,
			ns, worker, id)
		return err
	})
	return id, err
}

func newStorageID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "ds_" + hex.EncodeToString(b[:])
}

// Deploy allocates the next immutable version and makes it active in one
// transaction.
func (s *Store) Deploy(ctx context.Context, ns, worker string, spec DeploySpec, actor string) (Version, error) {
	fingerprint := ""
	if spec.IdempotencyKey != "" {
		copy := spec
		copy.IdempotencyKey = ""
		encoded, err := json.Marshal(copy)
		if err != nil {
			return Version{}, err
		}
		sum := sha256.Sum256(encoded)
		fingerprint = hex.EncodeToString(sum[:])
	}
	if err := validateNS(ns); err != nil {
		return Version{}, err
	}
	if worker == "" || spec.BundleSHA == "" {
		return Version{}, fmt.Errorf("control: worker and bundle sha are required")
	}
	if spec.Limits != nil && spec.Limits.SubRequests != nil && *spec.Limits.SubRequests <= 0 {
		return Version{}, fmt.Errorf("control: limits.subrequests must be positive")
	}
	if spec.Limits != nil && spec.Limits.CpuMs != nil && *spec.Limits.CpuMs <= 0 {
		return Version{}, fmt.Errorf("control: limits.cpuMs must be positive")
	}
	for _, consumer := range spec.Consumers {
		if consumer.MaxRetries < 0 || consumer.RetryDelaySeconds < 0 || consumer.MaxBatchTimeoutSeconds < 0 || consumer.MaxBatchTimeoutSeconds > 60 || consumer.MaxBatchSize < 0 || consumer.MaxBatchSize > 100 || consumer.MaxConcurrency < 0 {
			return Version{}, fmt.Errorf("control: invalid queue consumer settings for %s", consumer.Queue)
		}
	}
	c, err := s.cell(ctx)
	if err != nil {
		return Version{}, err
	}
	var out Version
	err = tx(ctx, c, func(t *sql.Tx) error {
		// Idempotent deploy: return the version created by the first call.
		if spec.IdempotencyKey != "" {
			var prev int
			var previousFingerprint string
			err := t.QueryRowContext(ctx,
				`SELECT version, fingerprint FROM deploy_idempotency WHERE ns=? AND worker=? AND key=?`,
				ns, worker, spec.IdempotencyKey).Scan(&prev, &previousFingerprint)
			if err == nil {
				if previousFingerprint != "" && previousFingerprint != fingerprint {
					return ErrIdempotencyConflict
				}
				v, ok, err := s.getVersion(ctx, c, ns, worker, prev)
				if err != nil {
					return err
				}
				if ok {
					out = v
					return nil
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// Delete lock blocks a deploy in progress.
		var expMs int64
		err := t.QueryRowContext(ctx, `SELECT exp_ms FROM delete_locks WHERE ns=? AND worker=?`, ns, worker).Scan(&expMs)
		if err == nil && expMs > s.now().UnixMilli() {
			return fmt.Errorf("control: worker %s has a delete in progress", worker)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var active, previous int
		var storageID string
		err = t.QueryRowContext(ctx, `SELECT active, previous, storage_id FROM workers WHERE ns=? AND name=?`, ns, worker).
			Scan(&active, &previous, &storageID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if storageID == "" {
			storageID = newStorageID()
		}
		var maxNumber int
		if err := t.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(number),0) FROM versions WHERE ns=? AND worker=?`, ns, worker).Scan(&maxNumber); err != nil {
			return err
		}
		next := maxNumber + 1
		bindings, err := s.pinServiceBindings(ctx, t, ns, worker, spec.Bindings)
		if err != nil {
			return err
		}
		v := Version{
			Number: next, BundleSHA: spec.BundleSHA, AssetsSHA: spec.AssetsSHA,
			Bindings: bindings, Vars: spec.Vars, Consumers: spec.Consumers,
			Crons: spec.Crons, Assets: spec.Assets, StorageID: storageID,
			CompatDate: spec.CompatDate, CompatFlags: spec.CompatFlags, Limits: spec.Limits,
			CreatedMs: s.now().UnixMilli(), Actor: actor, SessionPolicy: spec.SessionPolicy,
		}
		var assets any
		if v.Assets != nil {
			b, _ := json.Marshal(v.Assets)
			assets = string(b)
		}
		limits := jsonOr(v.Limits, "{}")
		if _, err := t.ExecContext(ctx,
			`INSERT INTO versions(ns, worker, number, bundle_sha, assets_sha, storage_id, session_policy,
				                      bindings, vars, consumers, crons, assets, compat_date, compat_flags, limits, created_ms, actor, secret_snapshot)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)`,
			ns, worker, v.Number, v.BundleSHA, v.AssetsSHA, v.StorageID, v.SessionPolicy,
			jsonOr(v.Bindings, "[]"), jsonOr(v.Vars, "{}"), jsonOr(v.Consumers, "[]"),
			jsonOr(v.Crons, "[]"), assets, v.CompatDate, jsonOr(v.CompatFlags, "[]"), limits, v.CreatedMs, v.Actor); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx,
			`INSERT INTO workers(ns, name, active, previous, storage_id) VALUES (?,?,?,?,?)
			 ON CONFLICT(ns, name) DO UPDATE SET active=excluded.active, previous=excluded.previous, storage_id=excluded.storage_id`,
			ns, worker, next, active, storageID); err != nil {
			return err
		}
		if err := appendDeploymentEvent(ctx, t, ns, worker, next, actor, v.CreatedMs); err != nil {
			return err
		}
		// Bundle refs: the version's own sha plus any pinned service targets.
		refs := map[string]bool{v.BundleSHA: true}
		for _, b := range v.Bindings {
			if b.Type == "service" && b.Version != "" {
				refs[b.Version] = true
			}
		}
		for sha := range refs {
			if _, err := t.ExecContext(ctx,
				`INSERT OR IGNORE INTO version_bundle_refs(ns, worker, number, sha) VALUES (?,?,?,?)`,
				ns, worker, next, sha); err != nil {
				return err
			}
		}
		if spec.IdempotencyKey != "" {
			if _, err := t.ExecContext(ctx,
				`INSERT OR REPLACE INTO deploy_idempotency(ns, worker, key, version, created_ms, fingerprint) VALUES (?,?,?,?,?,?)`,
				ns, worker, spec.IdempotencyKey, next, v.CreatedMs, fingerprint); err != nil {
				return err
			}
		}
		if err := syncBindingsTx(ctx, t, ns, worker, next, v.Bindings); err != nil {
			return err
		}
		if err := snapshotCurrentSecretsTx(ctx, t, ns, worker, next); err != nil {
			return err
		}
		// A deploy resurrects a soft-deleted worker AND its app: the projection
		// filters soft-deleted apps, so without this the fresh version would be
		// invisible; and a pending purge job would delete it later (ADR-134
		// follow-up).
		if _, err := t.ExecContext(ctx, `UPDATE workers SET deleted_ms=0 WHERE ns=? AND name=?`, ns, worker); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `UPDATE apps SET deleted_ms=0 WHERE ns=?`, ns); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM purges WHERE ns=? AND (worker=? OR worker='')`, ns, worker); err != nil {
			return err
		}
		out = v
		_ = previous
		if err := txAudit(ctx, t, ns, actor, "deploy", fmt.Sprintf("%s/%s@%d", ns, worker, next)); err != nil {
			return err
		}
		if s.onDeployBeforeCommit != nil {
			s.onDeployBeforeCommit()
		}
		return nil
	})
	return out, err
}

// pinServiceBindings pins each unpinned service binding to the target worker's
// active bundle (ADR-104), resolved in the same namespace.
func (s *Store) pinServiceBindings(ctx context.Context, t *sql.Tx, ns, worker string, bindings []Binding) ([]Binding, error) {
	if len(bindings) == 0 {
		return bindings, nil
	}
	out := make([]Binding, len(bindings))
	copy(out, bindings)
	for i := range out {
		b := &out[i]
		if b.Type != "service" || b.Version != "" {
			continue
		}
		target := b.ID
		if target == "" {
			target = b.Name
		}
		if target == "" {
			continue
		}
		var sha string
		err := t.QueryRowContext(ctx,
			`SELECT v.bundle_sha FROM versions v JOIN workers w
			   ON v.ns=w.ns AND v.worker=w.name AND v.number=w.active
			 WHERE w.ns=? AND w.name=?`, ns, target).Scan(&sha)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		b.Version = sha
	}
	return out, nil
}

// Releases returns a worker's release log, newest first.
func (s *Store) Releases(ctx context.Context, ns, worker string) ([]Release, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	var active int
	_ = c.DB.QueryRowContext(ctx, `SELECT active FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&active)
	rows, err := c.DB.QueryContext(ctx,
		`SELECT number, bundle_sha, actor, created_ms, crons FROM versions WHERE ns=? AND worker=? ORDER BY number DESC`,
		ns, worker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Release
	for rows.Next() {
		var r Release
		var crons string
		if err := rows.Scan(&r.Version, &r.BundleSHA, &r.Actors, &r.CreatedMs, &crons); err != nil {
			return nil, err
		}
		r.Active = r.Version == active
		if crons != "" {
			_ = json.Unmarshal([]byte(crons), &r.Crons)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// appendDeploymentEvent runs inside the activation transaction.
func appendDeploymentEvent(ctx context.Context, t *sql.Tx, ns, worker string, version int, actor string, createdMs int64) error {
	_, err := t.ExecContext(ctx, `INSERT INTO deployment_events(ns, worker, version, actor, created_ms) VALUES (?,?,?,?,?)`, ns, worker, version, actor, createdMs)
	return err
}

// DeploymentEvents returns committed activations, newest first. A pre-upgrade
// worker has no fabricated history; the current active pointer is still read
// separately by callers that need a status view.
func (s *Store) DeploymentEvents(ctx context.Context, ns, worker string, limit int) ([]DeploymentEvent, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT id, version, actor, created_ms FROM deployment_events WHERE ns=? AND worker=? ORDER BY id DESC LIMIT ?`, ns, worker, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentEvent
	for rows.Next() {
		var item DeploymentEvent
		if err := rows.Scan(&item.ID, &item.Version, &item.Actor, &item.CreatedMs); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// Promote switches a worker's active version, remembering the previous one.
func (s *Store) Promote(ctx context.Context, ns, worker string, version int, actor string) (Worker, error) {
	if err := validateNS(ns); err != nil {
		return Worker{}, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return Worker{}, err
	}
	var out Worker
	err = tx(ctx, c, func(t *sql.Tx) error {
		var n int
		err := t.QueryRowContext(ctx, `SELECT number FROM versions WHERE ns=? AND worker=? AND number=?`, ns, worker, version).Scan(&n)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoVersion
		}
		if err != nil {
			return err
		}
		var active, previous int
		err = t.QueryRowContext(ctx, `SELECT active, previous FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&active, &previous)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if active == version {
			out = Worker{Name: worker, Active: active, Previous: previous}
			return nil
		}
		previous, active = active, version
		if _, err := t.ExecContext(ctx, `UPDATE workers SET active=?, previous=? WHERE ns=? AND name=?`,
			active, previous, ns, worker); err != nil {
			return err
		}
		if err := restoreVersionSecretsTx(ctx, t, ns, worker, active); err != nil {
			return err
		}
		if err := appendDeploymentEvent(ctx, t, ns, worker, active, actor, s.now().UnixMilli()); err != nil {
			return err
		}
		out = Worker{Name: worker, Active: active, Previous: previous}
		return txAudit(ctx, t, ns, actor, "promote", fmt.Sprintf("%s/%s@%d", ns, worker, version))
	})
	return out, err
}

// Rollback switches back to the previous version.
func (s *Store) Rollback(ctx context.Context, ns, worker, actor string) (Worker, error) {
	return s.RollbackWithKey(ctx, ns, worker, "", actor)
}

// RollbackWithKey makes an unknown-outcome retry safe. A repeated key returns
// the first committed pointer result without toggling previous a second time.
func (s *Store) RollbackWithKey(ctx context.Context, ns, worker, key, actor string) (Worker, error) {
	if err := validateNS(ns); err != nil {
		return Worker{}, err
	}
	if len(key) > 256 {
		return Worker{}, fmt.Errorf("control: rollback idempotency key exceeds 256 bytes")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return Worker{}, err
	}
	var out Worker
	err = tx(ctx, c, func(t *sql.Tx) error {
		if key != "" {
			var active, previous int
			err := t.QueryRowContext(ctx, `SELECT active, previous FROM rollback_idempotency WHERE ns=? AND worker=? AND key=?`, ns, worker, key).Scan(&active, &previous)
			if err == nil {
				out = Worker{Name: worker, Active: active, Previous: previous}
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		var active, previous int
		err := t.QueryRowContext(ctx, `SELECT active, previous FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&active, &previous)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if previous == 0 {
			return ErrNoPrevious
		}
		active, previous = previous, active
		if _, err := t.ExecContext(ctx, `UPDATE workers SET active=?, previous=? WHERE ns=? AND name=?`,
			active, previous, ns, worker); err != nil {
			return err
		}
		if err := restoreVersionSecretsTx(ctx, t, ns, worker, active); err != nil {
			return err
		}
		if err := appendDeploymentEvent(ctx, t, ns, worker, active, actor, s.now().UnixMilli()); err != nil {
			return err
		}
		if key != "" {
			if _, err := t.ExecContext(ctx, `INSERT INTO rollback_idempotency(ns,worker,key,active,previous,created_ms) VALUES (?,?,?,?,?,?)`, ns, worker, key, active, previous, s.now().UnixMilli()); err != nil {
				return err
			}
		}
		out = Worker{Name: worker, Active: active, Previous: previous}
		return txAudit(ctx, t, ns, actor, "rollback", fmt.Sprintf("%s/%s@%d", ns, worker, active))
	})
	return out, err
}

// DeleteWorker soft-deletes a worker (ADR-131): stop its routes at once, then
// let the purge worker remove the metadata. A later deploy resurrects it.
func (s *Store) DeleteWorker(ctx context.Context, ns, worker, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	return tx(ctx, c, func(t *sql.Tx) error {
		res, err := t.ExecContext(ctx, `UPDATE workers SET deleted_ms=? WHERE ns=? AND name=?`, now, ns, worker)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM routes WHERE ns=? AND worker=?`, ns, worker); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM hosts WHERE ns=? AND kind='builtin' AND worker=?`, ns, worker); err != nil {
			return err
		}
		if err := enqueuePurgeTx(ctx, t, ns, worker, now); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "worker.delete", ns+"/"+worker)
	})
}

// PurgeWorkerRows hard-deletes one worker's control metadata.
func (s *Store) PurgeWorkerRows(ctx context.Context, ns, worker string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var deletedMs int64
		err := t.QueryRowContext(ctx, `SELECT deleted_ms FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&deletedMs)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil // already purged (or never existed): idempotent
		case err != nil:
			return err
		case deletedMs == 0:
			return nil // resurrected by a deploy since the job was queued
		}
		for _, stmt := range []string{
			`DELETE FROM versions WHERE ns=? AND worker=?`,
			`DELETE FROM version_secrets WHERE ns=? AND worker=?`,
			`DELETE FROM bindings WHERE ns=? AND worker=?`,
			`DELETE FROM version_bundle_refs WHERE ns=? AND worker=?`,
			`DELETE FROM workers WHERE ns=? AND name=?`,
			`DELETE FROM do_classes WHERE ns=? AND worker=?`,
			`DELETE FROM delete_locks WHERE ns=? AND worker=?`,
			`DELETE FROM deploy_idempotency WHERE ns=? AND worker=?`,
			`DELETE FROM rollback_idempotency WHERE ns=? AND worker=?`,
			`DELETE FROM secrets WHERE ns=? AND worker=?`,
			`DELETE FROM routes WHERE ns=? AND worker=?`,
			`DELETE FROM hosts WHERE ns=? AND kind='builtin' AND worker=?`,
		} {
			if _, err := t.ExecContext(ctx, stmt, ns, worker); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- routes -----------------------------------------------------------------

// PutRoute adds or replaces a host (+ optional path) route for a worker.
func (s *Store) PutRoute(ctx context.Context, ns string, rt Route, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	if rt.Host == "" || rt.Worker == "" {
		return fmt.Errorf("control: route host and worker are required")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	host := NormalizeHost(rt.Host)
	return tx(ctx, c, func(t *sql.Tx) error {
		// Ownership: the host must be registered (ADR-131/133). Registration is
		// authorization; built-in hosts are system-generated.
		var hostNS string
		err := t.QueryRowContext(ctx, `SELECT ns FROM hosts WHERE host=?`, host).Scan(&hostNS)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrHostUnknown
		case err != nil:
			return err
		case hostNS != ns:
			return ErrHostTaken
		}
		res, err := t.ExecContext(ctx,
			`INSERT INTO routes(host, path, ns, worker) VALUES (?,?,?,?)
			 ON CONFLICT(host, path) DO UPDATE SET worker=excluded.worker
			   WHERE routes.ns = excluded.ns`,
			host, rt.Path, ns, rt.Worker)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrHostTaken
		}
		return txAudit(ctx, t, ns, actor, "route.put", ns+"/"+rt.Host)
	})
}

// DeleteRoute removes a specific host/path route when path is non-nil, or all
// routes for the host when path is nil. An explicit empty path selects the root route.
func (s *Store) DeleteRoute(ctx context.Context, ns, host string, path *string, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		key := ns + "/" + host
		if path == nil {
			if _, err := t.ExecContext(ctx, `DELETE FROM routes WHERE ns=? AND host=?`, ns, NormalizeHost(host)); err != nil {
				return err
			}
		} else {
			if _, err := t.ExecContext(ctx, `DELETE FROM routes WHERE ns=? AND host=? AND path=?`, ns, NormalizeHost(host), *path); err != nil {
				return err
			}
			key += *path
		}
		return txAudit(ctx, t, ns, actor, "route.delete", key)
	})
}

// --- secrets ----------------------------------------------------------------

// PutSecret stores an envelope-encrypted secret.
func (s *Store) PutSecret(ctx context.Context, ns, worker, key string, value []byte, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	if s.env == nil {
		return ErrNoEnvelope
	}
	if worker == "" || key == "" {
		return fmt.Errorf("control: worker and key are required")
	}
	wrapped, ct, err := s.env.Seal(value)
	if err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx,
			`INSERT INTO secrets(ns, worker, key, wrapped_dek, value_nonce, ciphertext, updated_ms)
			 VALUES (?,?,?,?,?,?,?)
			 ON CONFLICT(ns, worker, key) DO UPDATE SET
			   wrapped_dek=excluded.wrapped_dek, value_nonce=excluded.value_nonce,
			   ciphertext=excluded.ciphertext, updated_ms=excluded.updated_ms`,
			ns, worker, key, wrapped, nil, ct, s.now().UnixMilli()); err != nil {
			return err
		}
		if err := txAudit(ctx, t, ns, actor, "secret.put", ns+"/"+worker+"/"+key); err != nil {
			return err
		}
		return s.publishSecretChangeTx(ctx, t, ns, worker, actor)
	})
}

// PatchSecrets applies up to 100 puts/deletes in one control-cell transaction.
// A nil value removes the key, matching Wrangler secret bulk's merge patch.
func (s *Store) PatchSecrets(ctx context.Context, ns, worker string, changes map[string]*string, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	if s.env == nil {
		return ErrNoEnvelope
	}
	if worker == "" || len(changes) == 0 || len(changes) > 100 {
		return fmt.Errorf("control: secret bulk requires worker and 1 to 100 entries")
	}
	type sealed struct{ wrapped, ciphertext []byte }
	sealedValues := make(map[string]sealed, len(changes))
	keys := make([]string, 0, len(changes))
	for key, value := range changes {
		if key == "" {
			return fmt.Errorf("control: secret key is required")
		}
		keys = append(keys, key)
		if value != nil {
			wrapped, ciphertext, err := s.env.Seal([]byte(*value))
			if err != nil {
				return err
			}
			sealedValues[key] = sealed{wrapped, ciphertext}
		}
	}
	sort.Strings(keys)
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		changed := false
		for _, key := range keys {
			if changes[key] == nil {
				result, err := t.ExecContext(ctx, `DELETE FROM secrets WHERE ns=? AND worker=? AND key=?`, ns, worker, key)
				if err != nil {
					return err
				}
				if n, _ := result.RowsAffected(); n > 0 {
					changed = true
				}
				if err := txAudit(ctx, t, ns, actor, "secret.delete", ns+"/"+worker+"/"+key); err != nil {
					return err
				}
				continue
			}
			v := sealedValues[key]
			changed = true
			if _, err := t.ExecContext(ctx, `INSERT INTO secrets(ns, worker, key, wrapped_dek, value_nonce, ciphertext, updated_ms)
				VALUES (?,?,?,?,?,?,?) ON CONFLICT(ns, worker, key) DO UPDATE SET
				wrapped_dek=excluded.wrapped_dek, value_nonce=excluded.value_nonce,
				ciphertext=excluded.ciphertext, updated_ms=excluded.updated_ms`,
				ns, worker, key, v.wrapped, nil, v.ciphertext, s.now().UnixMilli()); err != nil {
				return err
			}
			if err := txAudit(ctx, t, ns, actor, "secret.put", ns+"/"+worker+"/"+key); err != nil {
				return err
			}
		}
		if !changed {
			return nil
		}
		return s.publishSecretChangeTx(ctx, t, ns, worker, actor)
	})
}

// GetSecret returns a decrypted secret value.
func (s *Store) GetSecret(ctx context.Context, ns, worker, key string) ([]byte, error) {
	if s.env == nil {
		return nil, ErrNoEnvelope
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	var sec Secret
	err = c.DB.QueryRowContext(ctx,
		`SELECT worker, key, wrapped_dek, value_nonce, ciphertext, updated_ms
		   FROM secrets WHERE ns=? AND worker=? AND key=?`, ns, worker, key).
		Scan(&sec.Worker, &sec.Key, &sec.WrappedDEK, &sec.ValueNonce, &sec.Ciphertext, &sec.UpdatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.env.Open(sec.WrappedDEK, sec.Ciphertext)
}

// --- delete locks -----------------------------------------------------------

// AcquireDeleteLock takes a per-worker delete lock unless a live one exists.
func (s *Store) AcquireDeleteLock(ctx context.Context, ns, worker, actor string, ttl time.Duration) (bool, error) {
	if err := validateNS(ns); err != nil {
		return false, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return false, err
	}
	now := s.now().UnixMilli()
	contended := false
	err = tx(ctx, c, func(t *sql.Tx) error {
		var expMs int64
		err := t.QueryRowContext(ctx, `SELECT exp_ms FROM delete_locks WHERE ns=? AND worker=?`, ns, worker).Scan(&expMs)
		if err == nil && expMs > now {
			contended = true // a live delete already holds it
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = t.ExecContext(ctx,
			`INSERT INTO delete_locks(ns, worker, actor, at_ms, exp_ms) VALUES (?,?,?,?,?)
			 ON CONFLICT(ns, worker) DO UPDATE SET actor=excluded.actor, at_ms=excluded.at_ms, exp_ms=excluded.exp_ms`,
			ns, worker, actor, now, now+ttl.Milliseconds())
		return err
	})
	return contended, err
}

// ReleaseDeleteLock releases a delete lock.
func (s *Store) ReleaseDeleteLock(ctx context.Context, ns, worker string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `DELETE FROM delete_locks WHERE ns=? AND worker=?`, ns, worker); err != nil {
			return err
		}
		return nil
	})
}

// --- audit ------------------------------------------------------------------

// Audit returns an app's audit records, oldest first.
func (s *Store) Audit(ctx context.Context, ns string) ([]Audit, error) {
	return s.auditRows(ctx, ns, 0, 0)
}

// AuditQuery returns at most limit records at or after sinceMs (newest kept).
func (s *Store) AuditQuery(ctx context.Context, ns string, limit int, sinceMs int64) ([]Audit, error) {
	return s.auditRows(ctx, ns, limit, sinceMs)
}

func (s *Store) auditRows(ctx context.Context, ns string, limit int, sinceMs int64) ([]Audit, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT at_ms, actor, action, target FROM audit WHERE ns=? AND at_ms>=? ORDER BY at_ms ASC, id ASC`,
		ns, sinceMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Audit
	for rows.Next() {
		var a Audit
		if err := rows.Scan(&a.AtMs, &a.Actor, &a.Action, &a.Target); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}

// PruneAudit deletes an app's audit records older than beforeMs.
func (s *Store) PruneAudit(ctx context.Context, ns string, beforeMs int64) (int, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	err = tx(ctx, c, func(t *sql.Tx) error {
		res, err := t.ExecContext(ctx, `DELETE FROM audit WHERE ns=? AND at_ms < ?`, ns, beforeMs)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		n = int(affected)
		return nil
	})
	return n, err
}

// --- DO classes -------------------------------------------------------------

// DOClassState returns the code-class -> storage-class aliases and the deleted
// classes for a worker.
func (s *Store) DOClassState(ctx context.Context, ns, worker string) (map[string]string, []string, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT code_class, storage_class, deleted FROM do_classes WHERE ns=? AND worker=? ORDER BY code_class`,
		ns, worker)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	aliases := map[string]string{}
	var deleted []string
	for rows.Next() {
		var code, storage string
		var del int
		if err := rows.Scan(&code, &storage, &del); err != nil {
			return nil, nil, err
		}
		if storage != "" {
			aliases[code] = storage
		}
		if del != 0 {
			deleted = append(deleted, code)
		}
	}
	return aliases, deleted, rows.Err()
}

// RenameClass records a frozen alias codeClass -> storageClass (ADR-082).
func (s *Store) RenameClass(ctx context.Context, ns, worker, from, to, actor string) error {
	if from == "" || to == "" {
		return fmt.Errorf("control: rename needs from and to")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var exists int
		err := t.QueryRowContext(ctx, `SELECT 1 FROM do_classes WHERE ns=? AND worker=? AND code_class=?`, ns, worker, to).Scan(&exists)
		if err == nil {
			return fmt.Errorf("control: class %s already exists", to)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var storage string
		err = t.QueryRowContext(ctx, `SELECT storage_class FROM do_classes WHERE ns=? AND worker=? AND code_class=?`, ns, worker, from).Scan(&storage)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if storage == "" {
			storage = from // first rename of a fresh class
		}
		if _, err := t.ExecContext(ctx,
			`INSERT INTO do_classes(ns, worker, code_class, storage_class, deleted) VALUES (?,?,?,?,0)
			 ON CONFLICT(ns, worker, code_class) DO UPDATE SET storage_class=excluded.storage_class, deleted=0`,
			ns, worker, to, storage); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM do_classes WHERE ns=? AND worker=? AND code_class=?`, ns, worker, from); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "do.class.rename", ns+"/"+worker+"/"+from+"->"+to)
	})
}

// DeleteClass marks a class deleted (ADR-082).
func (s *Store) DeleteClass(ctx context.Context, ns, worker, class, actor string) error {
	if class == "" {
		return fmt.Errorf("control: delete needs a class")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx,
			`INSERT INTO do_classes(ns, worker, code_class, storage_class, deleted) VALUES (?,?,?,?,1)
			 ON CONFLICT(ns, worker, code_class) DO UPDATE SET deleted=1`,
			ns, worker, class, ""); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "do.class.delete", ns+"/"+worker+"/"+class)
	})
}

// --- projection / views ------------------------------------------------------

// Projection builds the routing projection (ADR-031) across all apps.
func (s *Store) Projection(ctx context.Context) (Projection, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return Projection{}, err
	}
	apps, err := s.Apps(ctx)
	if err != nil {
		return Projection{}, err
	}
	proj := Projection{AtMs: s.now().UnixMilli()}
	for _, app := range apps {
		pa := ProjectionApp{Namespace: app.Namespace}
		routes, err := c.DB.QueryContext(ctx,
			`SELECT host, path, worker FROM routes WHERE ns=? ORDER BY host, path`, app.Namespace)
		if err != nil {
			return Projection{}, err
		}
		for routes.Next() {
			var rt Route
			if err := routes.Scan(&rt.Host, &rt.Path, &rt.Worker); err != nil {
				routes.Close()
				return Projection{}, err
			}
			pa.Routes = append(pa.Routes, rt)
		}
		routes.Close()
		if err := routes.Err(); err != nil {
			return Projection{}, err
		}
		workers, err := c.DB.QueryContext(ctx, `SELECT name, active FROM workers WHERE ns=? AND deleted_ms=0 ORDER BY name`, app.Namespace)
		if err != nil {
			return Projection{}, err
		}
		type wrow struct {
			name   string
			active int
		}
		var list []wrow
		for workers.Next() {
			var w wrow
			if err := workers.Scan(&w.name, &w.active); err != nil {
				workers.Close()
				return Projection{}, err
			}
			list = append(list, w)
		}
		workers.Close()
		if err := workers.Err(); err != nil {
			return Projection{}, err
		}
		for _, w := range list {
			v, ok, err := s.getVersion(ctx, c, app.Namespace, w.name, w.active)
			if err != nil {
				return Projection{}, err
			}
			if !ok {
				continue
			}
			aliases, deleted, err := s.DOClassState(ctx, app.Namespace, w.name)
			if err != nil {
				return Projection{}, err
			}
			pa.Workers = append(pa.Workers, ProjectionWorker{
				Worker: w.name, Active: w.active, Version: v,
				ClassStorage: aliases, DeletedClasses: deleted,
			})
		}
		proj.Apps = append(proj.Apps, pa)
	}
	rev, err := s.Rev(ctx)
	if err != nil {
		return Projection{}, err
	}
	proj.ETag = fmt.Sprintf(`"r%d"`, rev)
	proj.AtMs = s.now().UnixMilli()
	return proj, nil
}

// WorkerEnv returns the immutable execution environment of a worker's active
// version by point reads (ADR-115).
func (s *Store) WorkerEnv(ctx context.Context, ns, worker string) (WorkerView, bool, error) {
	return s.VersionEnv(ctx, ns, worker, 0)
}

// VersionEnv returns the immutable execution environment of a specific version
// by point reads (number <= 0 = active). Dispatch paths that carry a version
// resolve the matching env, so the loaded env cannot drift from the code the
// caller asked for (ADR-128).
func (s *Store) VersionEnv(ctx context.Context, ns, worker string, number int) (WorkerView, bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return WorkerView{}, false, err
	}
	var v Version
	var ok bool
	if number <= 0 {
		v, ok, err = s.activeVersion(ctx, c, ns, worker)
	} else {
		v, ok, err = s.getVersion(ctx, c, ns, worker, number)
	}
	if err != nil || !ok {
		return WorkerView{}, false, err
	}
	aliases, deleted, err := s.DOClassState(ctx, ns, worker)
	if err != nil {
		return WorkerView{}, false, err
	}
	var hostLabel string
	_ = c.DB.QueryRowContext(ctx, `SELECT host_label FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&hostLabel)
	return WorkerView{
		Namespace: ns, Worker: worker, Version: v.Number, BundleSHA: v.BundleSHA,
		AssetsSHA: v.AssetsSHA, Assets: v.Assets, Bindings: v.Bindings, Vars: v.Vars,
		StorageID: v.StorageID, CompatDate: v.CompatDate, CompatFlags: v.CompatFlags, Limits: v.Limits,
		HostLabel: hostLabel, ClassStorage: aliases, DeletedClasses: deleted,
	}, true, nil
}

// VersionEnvBySHA returns the environment of the newest version carrying the
// given bundle sha. Service bindings pin a sha at deploy time (ADR-104) and the
// runtime resolves it back to the immutable env here (ADR-134).
func (s *Store) VersionEnvBySHA(ctx context.Context, ns, worker, sha string) (WorkerView, bool, error) {
	if sha == "" {
		return WorkerView{}, false, nil
	}
	c, err := s.cell(ctx)
	if err != nil {
		return WorkerView{}, false, err
	}
	row := c.DB.QueryRowContext(ctx,
		`SELECT `+versionColsAliased+` FROM versions v
		  WHERE v.ns=? AND v.worker=? AND v.bundle_sha=? ORDER BY v.number DESC LIMIT 1`, ns, worker, sha)
	v, err := scanVersion(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkerView{}, false, nil
	}
	if err != nil {
		return WorkerView{}, false, err
	}
	aliases, deleted, err := s.DOClassState(ctx, ns, worker)
	if err != nil {
		return WorkerView{}, false, err
	}
	var hostLabel string
	_ = c.DB.QueryRowContext(ctx, `SELECT host_label FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&hostLabel)
	return WorkerView{
		Namespace: ns, Worker: worker, Version: v.Number, BundleSHA: v.BundleSHA,
		AssetsSHA: v.AssetsSHA, Assets: v.Assets, Bindings: v.Bindings, Vars: v.Vars,
		StorageID: v.StorageID, CompatDate: v.CompatDate, CompatFlags: v.CompatFlags, Limits: v.Limits,
		HostLabel: hostLabel, ClassStorage: aliases, DeletedClasses: deleted,
	}, true, nil
}

// --- GC inputs ---------------------------------------------------------------

// BundleRefs returns every bundle SHA referenced by a version or a pinned
// service binding (ADR-110).
func (s *Store) BundleRefs(ctx context.Context) (map[string]bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT DISTINCT sha FROM version_bundle_refs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := map[string]bool{}
	for rows.Next() {
		var sha string
		if err := rows.Scan(&sha); err != nil {
			return nil, err
		}
		if sha != "" {
			refs[sha] = true
		}
	}
	return refs, rows.Err()
}

// AssetRefs returns every asset-version id ("<ns>/<worker>/<token>") referenced
// by a worker version's AssetsSHA (ADR-111).
func (s *Store) AssetRefs(ctx context.Context) (map[string]bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT DISTINCT ns, worker, assets_sha FROM versions WHERE assets_sha <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := map[string]bool{}
	for rows.Next() {
		var ns, worker, sha string
		if err := rows.Scan(&ns, &worker, &sha); err != nil {
			return nil, err
		}
		refs[ns+"/"+worker+"/"+sha] = true
	}
	return refs, rows.Err()
}

// gcMarkKey validates a GC mark's kind and id.
func gcMarkKey(kind, id string) error {
	switch kind {
	case "bundle", "asset":
	default:
		return fmt.Errorf("control: unknown gc kind %q", kind)
	}
	if id == "" || strings.ContainsAny(id, "\\") || strings.Contains(id, "..") {
		return fmt.Errorf("control: bad gc id %q", id)
	}
	return nil
}

// GCMark returns when an item was first marked as a GC candidate.
func (s *Store) GCMark(ctx context.Context, kind, id string) (int64, bool, error) {
	if err := gcMarkKey(kind, id); err != nil {
		return 0, false, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return 0, false, err
	}
	var ms int64
	err = c.DB.QueryRowContext(ctx, `SELECT marked_ms FROM gc_marks WHERE kind=? AND id=?`, kind, id).Scan(&ms)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return ms, err == nil, err
}

// SetGCMark records an item as a GC candidate.
func (s *Store) SetGCMark(ctx context.Context, kind, id string, ms int64) error {
	if err := gcMarkKey(kind, id); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		_, err := t.ExecContext(ctx,
			`INSERT INTO gc_marks(kind, id, marked_ms) VALUES (?,?,?)
			 ON CONFLICT(kind, id) DO UPDATE SET marked_ms=excluded.marked_ms`, kind, id, ms)
		return err
	})
}

// ClearGCMark clears an item's GC candidate mark.
func (s *Store) ClearGCMark(ctx context.Context, kind, id string) error {
	if err := gcMarkKey(kind, id); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		_, err := t.ExecContext(ctx, `DELETE FROM gc_marks WHERE kind=? AND id=?`, kind, id)
		return err
	})
}

// GCRefs adapts the control store to objgc.Refs for one kind ("bundle"/"asset").
type GCRefs struct {
	S    *Store
	Kind string
}

// Referenced returns the referenced id set for the kind.
func (r GCRefs) Referenced(ctx context.Context) (map[string]bool, error) {
	switch r.Kind {
	case "bundle":
		return r.S.BundleRefs(ctx)
	case "asset":
		return r.S.AssetRefs(ctx)
	}
	return nil, fmt.Errorf("control: unknown gc kind %q", r.Kind)
}

// Mark reports an item's GC candidate time.
func (r GCRefs) Mark(ctx context.Context, id string) (int64, bool, error) {
	return r.S.GCMark(ctx, r.Kind, id)
}

// SetMark records an item as a GC candidate.
func (r GCRefs) SetMark(ctx context.Context, id string, ms int64) error {
	return r.S.SetGCMark(ctx, r.Kind, id, ms)
}

// ClearMark clears an item's GC candidate mark.
func (r GCRefs) ClearMark(ctx context.Context, id string) error {
	return r.S.ClearGCMark(ctx, r.Kind, id)
}

// sortStrings is a small helper kept for deterministic outputs.
func sortStrings(v []string) { sort.Strings(v) }

// SecretMeta is a secret key without its value (list API).
type SecretMeta struct {
	Worker    string `json:"worker"`
	Key       string `json:"key"`
	UpdatedMs int64  `json:"updated_ms"`
}

// DeleteSecret removes a secret (missing key is not an error).
func (s *Store) DeleteSecret(ctx context.Context, ns, worker, key, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		result, err := t.ExecContext(ctx,
			`DELETE FROM secrets WHERE ns=? AND worker=? AND key=?`, ns, worker, key)
		if err != nil {
			return err
		}
		if err := txAudit(ctx, t, ns, actor, "secret.delete", ns+"/"+worker+"/"+key); err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			return nil
		}
		return s.publishSecretChangeTx(ctx, t, ns, worker, actor)
	})
}

// ListSecrets returns a worker's secret keys (never the values).
func (s *Store) ListSecrets(ctx context.Context, ns, worker string) ([]SecretMeta, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT worker, key, updated_ms FROM secrets WHERE ns=? AND worker=? ORDER BY key`, ns, worker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SecretMeta
	for rows.Next() {
		var m SecretMeta
		if err := rows.Scan(&m.Worker, &m.Key, &m.UpdatedMs); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ResourceReferencedBy lists the workers whose versions declare a binding for
// (ns, kind, name) — used to refuse revoking a resource that is still in use
// (ADR-156). The match is on the binding name (the deploy gate's identity) and
// the resource's scope for id-shaped declarations.
func (s *Store) ResourceReferencedBy(ctx context.Context, ns, kind, name, scope string) ([]string, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT DISTINCT worker FROM bindings WHERE ns=? AND type=? AND (name=? OR id=?) ORDER BY worker`,
		ns, kind, name, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DeleteResource revokes registration and, for KV/Queue, atomically snapshots a
// durable cleanup job. Lifecycle coordination is completed before acknowledging;
// the asynchronous owner-routed job subsequently removes data. Repeats preserve
// the same job and are safe even when the original acknowledgement was lost.
func (s *Store) DeleteResource(ctx context.Context, ns, kind, name, actor string) error {
	s.resourceMu.Lock()
	defer s.resourceMu.Unlock()
	if err := validateNS(ns); err != nil {
		return err
	}
	if kind == "" || name == "" {
		return fmt.Errorf("control: resource kind and name are required")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	err = tx(ctx, c, func(t *sql.Tx) error {
		dataScope, generation := "", ""
		cleanupScope := false
		deadLetters := []string{}
		if kind == "kv" || kind == "queue" {
			var ref string
			lookup := t.QueryRowContext(ctx, `SELECT scope FROM resources WHERE ns=? AND kind=? AND name=?`, ns, kind, name).Scan(&ref)
			if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
				return lookup
			}
			if errors.Is(lookup, sql.ErrNoRows) {
				var jobs int
				if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_cleanups WHERE ns=? AND kind=? AND name=?`, ns, kind, name).Scan(&jobs); err != nil {
					return err
				}
				if jobs > 0 {
					return nil
				}
				// Active-version derived declarations are snapshotted under the
				// same transaction, rather than through a separate connection.
				lookup = t.QueryRowContext(ctx, `SELECT b.id FROM bindings b JOIN workers w ON b.ns=w.ns AND b.worker=w.name AND b.number=w.active WHERE b.ns=? AND b.type=? AND b.name=? LIMIT 1`, ns, kind, name).Scan(&ref)
				if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
					return lookup
				}
			}
			physical, perr := ResourceDataScope(ns, kind, name, ref)
			// A malformed registration predating the name/scope validation
			// (ADR-269) never addressed a physical cell: revoke the registry
			// entry and tombstone without a data-cleanup job instead of making
			// the resource permanently undeletable.
			if perr == nil {
				dataScope = physical.String()
				cleanupScope = true
				rows, err := t.QueryContext(ctx, `SELECT name,scope FROM resources WHERE ns=? AND kind=? AND name<>?`, ns, kind, name)
				if err != nil {
					return err
				}
				for rows.Next() {
					var otherName, otherRef string
					if err := rows.Scan(&otherName, &otherRef); err != nil {
						rows.Close()
						return err
					}
					other, err := ResourceDataScope(ns, kind, otherName, otherRef)
					if err == nil && other.String() == dataScope {
						rows.Close()
						return ErrResourceScopeShared
					}
				}
				if err := rows.Err(); err != nil {
					rows.Close()
					return err
				}
				rows.Close()
				if s.ResourceGeneration != nil {
					generation, err = s.ResourceGeneration(ctx, physical)
					if err != nil {
						return err
					}
				}
			}
		}
		if kind == "queue" {
			rows, err := t.QueryContext(ctx, `SELECT consumers FROM versions WHERE ns=?`, ns)
			if err != nil {
				return err
			}
			seen := map[string]bool{}
			for rows.Next() {
				var raw string
				if err := rows.Scan(&raw); err != nil {
					rows.Close()
					return err
				}
				var consumers []Consumer
				if err := json.Unmarshal([]byte(raw), &consumers); err != nil {
					rows.Close()
					return err
				}
				for _, consumer := range consumers {
					dlq := consumer.DeadLetterQueue
					if consumer.Queue == name && dlq != "" && dlq != name && !seen[dlq] {
						seen[dlq] = true
						deadLetters = append(deadLetters, dlq)
					}
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			edges, err := t.QueryContext(ctx, `SELECT destination_queue FROM queue_transfer_destinations WHERE ns=? AND source_queue=? AND source_generation=?`, ns, name, generation)
			if err != nil {
				return err
			}
			for edges.Next() {
				var dlq string
				if err := edges.Scan(&dlq); err != nil {
					edges.Close()
					return err
				}
				if dlq != name && !seen[dlq] {
					seen[dlq] = true
					deadLetters = append(deadLetters, dlq)
				}
			}
			if err := edges.Err(); err != nil {
				edges.Close()
				return err
			}
			edges.Close()
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM resources WHERE ns=? AND kind=? AND name=?`, ns, kind, name); err != nil {
			return err
		}
		// Record a tombstone (immutable versions still declare the binding) and
		// drop the derived index rows so resolution fails closed and a queue
		// consumer stops (ADR-156). Re-registering clears the tombstone.
		if _, err := t.ExecContext(ctx,
			`INSERT INTO revoked_resources(ns, kind, name, revoked_ms) VALUES(?,?,?,?)
			 ON CONFLICT(ns, kind, name) DO UPDATE SET revoked_ms=excluded.revoked_ms`,
			ns, kind, name, s.now().UnixMilli()); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM bindings WHERE ns=? AND type=? AND name=?`, ns, kind, name); err != nil {
			return err
		}
		if (kind == "kv" || kind == "queue") && cleanupScope {
			dlqs, _ := json.Marshal(deadLetters)
			if _, err := t.ExecContext(ctx, `INSERT INTO resource_cleanups(ns,kind,name,job_id,data_scope,source_generation,dead_letters,state,requested_ms,updated_ms) VALUES(?,?,?,?,?,?,?,'pending',?,?) ON CONFLICT(ns,kind,name) DO UPDATE SET job_id=excluded.job_id,data_scope=excluded.data_scope,source_generation=excluded.source_generation,dead_letters=excluded.dead_letters,state='pending',requested_ms=excluded.requested_ms,updated_ms=excluded.updated_ms,attempts=0,last_error='',next_retry_ms=0,lease_token='',lease_until_ms=0`, ns, kind, name, newStorageID(), dataScope, generation, string(dlqs), s.now().UnixMilli(), s.now().UnixMilli()); err != nil {
				return err
			}
		}
		return txAudit(ctx, t, ns, actor, "resource.delete", kind+"/"+name)
	})
	if err == nil && s.OnResourceRevoked != nil {
		err = s.OnResourceRevoked(ctx, ns, kind, name)
	}
	return err
}
