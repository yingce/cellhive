// Package cellstore provides per-cell SQLite storage.
//
// Each cell owns exactly one SQLite database file under
// <DataDir>/cells/<ns>/<class>/<id>.db. The store enforces WAL journaling so
// that an external supervisor can observe the WAL (see internal/wal).
package cellstore

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("cellstore: not found")

// Store opens cells under a data directory.
type Store struct {
	DataDir string

	// Hydrate, when set, is called once before a cell is opened for the first
	// time on this host if its SQLite file does not yet exist. It should
	// populate dest with the cell's durable state (e.g. by restoring the bucket
	// replica), which is how a fresh owner recovers cells after a crash or
	// takeover. A nil hook leaves new cells empty.
	Hydrate func(ctx context.Context, sc cell.Scope, dest string) error

	// Paged, when set, takes precedence over Hydrate for a cold cell: it returns
	// a page source for the cell's pinned replication cut, in which case the
	// local file is created sparse and SQLite faults pages from the source on
	// first use (ADR-160). Returning ok=false falls back to Hydrate (small
	// chains, or no compacted L1 page index yet).
	Paged func(ctx context.Context, sc cell.Scope, dest string) (pagedvfs.Source, bool, error)

	// PagedHydrateMBPS is the background fill rate for a paged cell (0 =
	// disabled, keep it sparse and fault every cold page). One hydrator runs per
	// node at a time.
	PagedHydrateMBPS int

	// Eviction (disabled when both are zero, the default). MaxOpenCells caps the
	// number of cached cells; IdleTTL closes a cell after that long without use.
	// Drop, when set, runs before a cell is closed so the owner can stop the
	// cell's capture loop first. Eviction releases only the local handle: the
	// cell's ownership record is untouched, and the next request reopens it.
	MaxOpenCells int
	IdleTTL      time.Duration
	Drop         func(ctx context.Context, sc cell.Scope) error

	openMu sync.Mutex // serializes cold opens so one file is opened once

	evicted atomic.Uint64
	sweeps  atomic.Uint64

	mu       sync.Mutex
	cond     *sync.Cond
	cells    map[string]*Cell
	lru      *list.List // front = most recently used; values are *lruEnt
	byPath   map[string]*list.Element
	busy     map[string]int  // in-flight requests per gate key (a cell scope)
	evicting map[string]bool // gate key currently being evicted

	// diskMu caches the cold-path DiskFiles walk for metrics (ADR-122).
	diskMu    sync.Mutex
	diskFiles int
	diskBytes int64
	diskAt    time.Time
}

type lruEnt struct {
	path string
	last time.Time
}

// New creates a Store rooted at dataDir.
func New(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		DataDir:  dataDir,
		cells:    map[string]*Cell{},
		lru:      list.New(),
		byPath:   map[string]*list.Element{},
		busy:     map[string]int{},
		evicting: map[string]bool{},
	}
	s.cond = sync.NewCond(&s.mu)
	return s, nil
}

// BeginRequest marks a request in flight for a gate key (a cell scope string)
// and returns its end function. It blocks while that key is being evicted, so a
// request never races the close of the cell it uses. An empty key is ignored.
func (s *Store) BeginRequest(key string) (end func()) {
	if key == "" {
		return func() {}
	}
	s.mu.Lock()
	for s.evicting[key] {
		s.cond.Wait()
	}
	s.busy[key]++
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		if s.busy[key] > 0 {
			s.busy[key]--
		}
		if s.busy[key] == 0 {
			s.cond.Broadcast()
		}
		s.mu.Unlock()
	}
}

func (s *Store) touchLocked(p string) {
	if e, ok := s.byPath[p]; ok {
		e.Value.(*lruEnt).last = time.Now()
		s.lru.MoveToFront(e)
	}
}

// Cell returns a shared, cached Cell for the scope, opening and migrating it
// only once. The returned Cell must NOT be closed by the caller; it is released
// by Store.Close or by an eviction sweep.
func (s *Store) Cell(ctx context.Context, sc cell.Scope) (*Cell, error) {
	p, err := s.Path(sc)
	if err != nil {
		return nil, err
	}
	key := sc.String()
	s.mu.Lock()
	for s.evicting[key] {
		s.cond.Wait() // an eviction is closing/deleting this cell
	}
	if c, ok := s.cells[p]; ok {
		s.touchLocked(p)
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	s.openMu.Lock()
	defer s.openMu.Unlock()
	// Another goroutine may have opened it while we waited.
	s.mu.Lock()
	if c, ok := s.cells[p]; ok {
		s.touchLocked(p)
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	_, statErr := os.Stat(p)
	missing := errors.Is(statErr, os.ErrNotExist)
	markerPS, markerCommit, hasMarker := readPagedMarker(p)
	if missing && hasMarker {
		// A marker without its file is leftover state: drop it.
		removePagedMarker(p)
		hasMarker = false
	}
	// A sparse paged cache is not a database: it must be served by the paged VFS,
	// never by the base VFS. Treat it like a cold cell and re-register it.
	cold := missing || hasMarker
	paged := false
	if cold && s.Paged != nil {
		src, ok, perr := s.Paged(ctx, sc, p)
		if perr != nil {
			return nil, fmt.Errorf("cellstore: paged %s: %w", sc, perr)
		}
		if ok {
			reuse := hasMarker && !missing && markerPS == src.PageSize() && markerCommit == src.Commit()
			// Write the marker before the file can become sparse, so a crash in
			// between cannot leave a sparse file that looks complete.
			if err := writePagedMarker(p, src.PageSize(), src.Commit()); err != nil {
				return nil, fmt.Errorf("cellstore: paged marker %s: %w", sc, err)
			}
			if reuse {
				if err := pagedvfs.Register(p, src); err != nil {
					removePagedMarker(p)
					return nil, fmt.Errorf("cellstore: paged register %s: %w", sc, err)
				}
			} else if err := pagedvfs.Prepare(p, src); err != nil {
				removePagedMarker(p)
				return nil, fmt.Errorf("cellstore: paged prepare %s: %w", sc, err)
			}
			paged = true
			missing = false // the sparse file now exists
		}
	}
	if cold && hasMarker && !paged {
		// A partial cache we cannot serve: discard it and rebuild the file from
		// the durable replica rather than reading zeros.
		if err := removeCellFileSet(p); err != nil {
			return nil, fmt.Errorf("cellstore: discard paged cache %s: %w", sc, err)
		}
		missing = true
	}
	if missing && s.Hydrate != nil {
		if err := s.Hydrate(ctx, sc, p); err != nil {
			return nil, fmt.Errorf("cellstore: hydrate %s: %w", sc, err)
		}
	}
	if !paged && missing && s.Hydrate == nil {
		if _, _, ok := readPagedMarker(p); ok {
			return nil, fmt.Errorf("%w: %s", ErrPagedCacheUnusable, p)
		}
	}
	c, err := openCell(ctx, sc, p, paged)
	if err != nil {
		if paged {
			pagedvfs.Release(p)
		}
		return nil, err
	}
	if paged {
		c.paged = true
		c.hydrateStop = pagedvfs.HydrateRate(p, s.PagedHydrateMBPS, func(complete bool) {
			if complete {
				removePagedMarker(p)
			}
		})
	}
	s.mu.Lock()
	s.cells[p] = c
	s.byPath[p] = s.lru.PushFront(&lruEnt{path: p, last: time.Now()})
	s.mu.Unlock()
	return c, nil
}

// Start runs the eviction sweeper until ctx is cancelled. It is a no-op when
// eviction is disabled.
func (s *Store) Start(ctx context.Context) {
	if s.IdleTTL <= 0 && s.MaxOpenCells <= 0 {
		return
	}
	interval := 30 * time.Second
	if s.IdleTTL > 0 {
		if interval = s.IdleTTL / 2; interval < time.Second {
			interval = time.Second
		}
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.Sweep(ctx)
			}
		}
	}()
}

// Sweep evicts idle cells and cells over MaxOpenCells, least-recently-used
// first. It skips a cell whose namespace has an in-flight request and returns
// the number evicted.
func (s *Store) Sweep(ctx context.Context) int {
	s.sweeps.Add(1)
	evicted := 0
	for {
		if !s.shouldEvict() {
			return evicted
		}
		if !s.evictOne(ctx) {
			return evicted
		}
		evicted++
	}
}

func (s *Store) shouldEvict() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaxOpenCells > 0 && len(s.cells) > s.MaxOpenCells {
		return true
	}
	if s.IdleTTL > 0 && s.lru.Len() > 0 {
		ent := s.lru.Back().Value.(*lruEnt)
		if _, ok := s.cells[ent.path]; ok && time.Since(ent.last) >= s.IdleTTL {
			return true
		}
	}
	return false
}

// evictOne picks the least-recently-used evictable cell, removes it, then stops
// its capture (Drop) and closes it outside the lock.
// Forget closes a locally cached cell (if open) and deletes its local SQLite
// files. It is used when this node no longer owns the cell (ADR-115): the next
// open re-hydrates from the bucket, so a non-owner can never keep serving a
// stale local copy. Calls made through BeginRequest(key) are respected: we wait
// for in-flight requests to drain before closing.
func (s *Store) Forget(ctx context.Context, sc cell.Scope) error {
	return s.forget(ctx, sc, nil)
}

// ForgetWithVerify evicts a cell only when verify (run under the eviction gate,
// with every in-flight request drained) returns nil. The disk janitor uses it to
// re-check that the bucket manifest still covers the cell's current txid, so a
// write acked after the eligibility check cannot be deleted.
func (s *Store) ForgetWithVerify(ctx context.Context, sc cell.Scope, verify func() error) error {
	return s.forget(ctx, sc, verify)
}

func (s *Store) forget(ctx context.Context, sc cell.Scope, verify func() error) error {
	p, err := s.Path(sc)
	if err != nil {
		return err
	}
	key := sc.String()
	s.mu.Lock()
	// Wait for in-flight requests and other evictions: after this loop the cell
	// cannot be reached through BeginRequest/Cell until the gate is released.
	for s.evicting[key] || s.busy[key] > 0 {
		s.cond.Wait()
	}
	s.evicting[key] = true
	s.mu.Unlock()
	if verify != nil {
		if err := verify(); err != nil {
			s.mu.Lock()
			delete(s.evicting, key)
			s.cond.Broadcast()
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Lock()
	c := s.cells[p]
	if c != nil {
		delete(s.cells, p)
		if e := s.byPath[p]; e != nil {
			s.lru.Remove(e)
			delete(s.byPath, p)
		}
	}
	s.mu.Unlock()

	if c != nil {
		if s.Drop != nil {
			_ = s.Drop(ctx, sc) // stop the capture loop before closing
		}
		_ = c.Close()
	}
	for _, path := range []string{p, p + "-wal", p + "-shm", pagedMarkerPath(p)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.mu.Lock()
			delete(s.evicting, key)
			s.cond.Broadcast()
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Lock()
	delete(s.evicting, key)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.evicted.Add(1)
	return nil
}

func (s *Store) evictOne(ctx context.Context) bool {
	s.mu.Lock()
	var c *Cell
	for e := s.lru.Back(); e != nil; e = e.Prev() {
		ent := e.Value.(*lruEnt)
		cc := s.cells[ent.path]
		if cc == nil {
			s.lru.Remove(e)
			delete(s.byPath, ent.path)
			continue
		}
		key := cc.Scope.String()
		if s.busy[key] > 0 || s.evicting[key] {
			continue
		}
		s.evicting[key] = true
		delete(s.cells, ent.path)
		s.lru.Remove(e)
		delete(s.byPath, ent.path)
		c = cc
		break
	}
	s.mu.Unlock()
	if c == nil {
		return false
	}
	if s.Drop != nil {
		_ = s.Drop(ctx, c.Scope)
	}
	_ = c.Close()
	s.mu.Lock()
	delete(s.evicting, c.Scope.String())
	s.cond.Broadcast()
	s.mu.Unlock()
	s.evicted.Add(1)
	return true
}

// Stats reports cache size and eviction counters for metrics/diagnostics.
func (s *Store) Stats() (openCells int, evicted, sweeps uint64) {
	s.mu.Lock()
	openCells = len(s.cells)
	s.mu.Unlock()
	return openCells, s.evicted.Load(), s.sweeps.Load()
}

// IsOpen reports whether a cell handle is currently resident (cached) here. A
// closed (evicted) cell is idle and safe for rebalance to move.
func (s *Store) IsOpen(sc cell.Scope) bool {
	p, err := s.Path(sc)
	if err != nil {
		return false
	}
	s.mu.Lock()
	_, ok := s.cells[p]
	s.mu.Unlock()
	return ok
}

// DeleteNamespace closes and removes every cached cell under a namespace's
// directory (app delete cleanup).
func (s *Store) DeleteNamespace(ctx context.Context, ns string) error {
	if ns == "" || strings.ContainsAny(ns, "/\\") {
		return fmt.Errorf("cellstore: invalid namespace")
	}
	dir := filepath.Join(s.DataDir, "cells", ns)
	prefix := dir + string(filepath.Separator)
	// Evict every cached scope of the namespace through the regular drain path so
	// in-flight requests finish and the capture loop is stopped (Drop) before the
	// files disappear.
	s.mu.Lock()
	var scopes []cell.Scope
	for p, c := range s.cells {
		if strings.HasPrefix(p, prefix) {
			scopes = append(scopes, c.Scope)
		}
	}
	s.mu.Unlock()
	for _, sc := range scopes {
		if err := s.forget(ctx, sc, nil); err != nil {
			return err
		}
	}
	return os.RemoveAll(dir)
}

// ForgetPrefix evicts every cached cell whose scope string starts with prefix,
// through the regular drain path (in-flight requests finish, capture stops via
// Drop) — used by the purge hook to drop a deleted worker's local DO cells
// (ADR-142). Returns the number of cells forgotten.
func (s *Store) ForgetPrefix(ctx context.Context, prefix string) (int, error) {
	if prefix == "" {
		return 0, fmt.Errorf("cellstore: empty prefix")
	}
	s.mu.Lock()
	var scopes []cell.Scope
	for _, c := range s.cells {
		if strings.HasPrefix(c.Scope.String(), prefix) {
			scopes = append(scopes, c.Scope)
		}
	}
	s.mu.Unlock()
	n := 0
	for _, sc := range scopes {
		if err := s.forget(ctx, sc, nil); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Close closes every cached Cell. The Store must not be used afterwards.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for p, c := range s.cells {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.cells, p)
	}
	s.lru.Init()
	s.byPath = map[string]*list.Element{}
	return firstErr
}

// LocalScopes walks the cell store and returns every scope that has a database
// file on this node. It is a cold path used by the wake-index repair loop
// (ADR-177) and diagnostics; entries are sorted so a rotating cursor is stable.
func (s *Store) LocalScopes() ([]cell.Scope, error) {
	root := filepath.Join(s.DataDir, "cells")
	nsDirs, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []cell.Scope
	for _, ns := range nsDirs {
		if !ns.IsDir() {
			continue
		}
		classDirs, cerr := os.ReadDir(filepath.Join(root, ns.Name()))
		if cerr != nil {
			continue
		}
		for _, cl := range classDirs {
			if !cl.IsDir() {
				continue
			}
			files, ferr := os.ReadDir(filepath.Join(root, ns.Name(), cl.Name()))
			if ferr != nil {
				continue
			}
			for _, f := range files {
				name := f.Name()
				if f.IsDir() || !strings.HasSuffix(name, ".db") {
					continue
				}
				sc := cell.Scope{Namespace: ns.Name(), Class: cl.Name(), ID: strings.TrimSuffix(name, ".db")}
				if sc.Validate() == nil {
					out = append(out, sc)
				}
			}
		}
	}
	return out, nil
}

// PendingTimerMin reads a cell's earliest pending timer without migrating or
// writing the schema. has is false when the cell has no timers table; min is 0
// when the table exists but holds no pending row. A sparse (paged) cell that is
// not mounted through the paged VFS is skipped rather than read as zeros.
func PendingTimerMin(ctx context.Context, path string) (min int64, has bool, err error) {
	if _, _, ok := readPagedMarker(path); ok && !pagedvfs.IsPaged(path) {
		return 0, false, nil
	}
	dsn := OpenDSN(path)
	if pagedvfs.IsPaged(path) {
		dsn += "&vfs=" + pagedvfs.VFSName
	}
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		return 0, false, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var name string
	if qerr := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='timers'`).Scan(&name); qerr != nil {
		if errors.Is(qerr, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, qerr
	}
	var m sql.NullInt64
	if qerr := db.QueryRowContext(ctx, `SELECT MIN(due_ms) FROM timers`).Scan(&m); qerr != nil {
		return 0, false, qerr
	}
	if !m.Valid {
		return 0, true, nil
	}
	return m.Int64, true, nil
}

// Path returns the SQLite file path for a scope.
func (s *Store) Path(sc cell.Scope) (string, error) {
	if err := sc.Validate(); err != nil {
		return "", err
	}
	dir := filepath.Join(s.DataDir, "cells", sc.Namespace, sc.Class)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(dir, sc.ID+".db"), nil
}

// Open opens (creating if needed) a fresh, caller-owned SQLite handle for a
// scope. The caller must Close it. For hot paths, prefer the cached Store.Cell.
func (s *Store) Open(ctx context.Context, sc cell.Scope) (*Cell, error) {
	p, err := s.Path(sc)
	if err != nil {
		return nil, err
	}
	return openCell(ctx, sc, p, false)
}

// openCell opens and migrates a cell database at an explicit path. paged opens
// the main database through the paged VFS (ADR-160), which faults pages on use.
func openCell(ctx context.Context, sc cell.Scope, p string, paged bool) (*Cell, error) {
	// A path registered with the paged VFS must always open through it, however
	// the caller reached us: reading a sparse file with the base VFS would
	// return zeros.
	if !paged {
		paged = pagedvfs.IsPaged(p)
	}
	if !paged {
		if _, _, ok := readPagedMarker(p); ok {
			return nil, fmt.Errorf("%w: %s", ErrPagedCacheUnusable, p)
		}
	}
	dsn := OpenDSN(p)
	if paged {
		dsn += "&vfs=" + pagedvfs.VFSName
	}
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(3) // writer + read-lock + headroom for checkpoint/Conn
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	c := &Cell{DB: db, Scope: sc, Path: p, paged: paged}
	if err := c.loadTxID(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return c, nil
}

// loadTxID reads the persisted txid into the in-memory mirror. Callers hold
// writeMu (or run before the Cell is shared).
func (c *Cell) loadTxID(ctx context.Context) error {
	if c.txidOK {
		return nil
	}
	var text string
	if err := c.DB.QueryRowContext(ctx, `SELECT v FROM cell_meta WHERE k='txid'`).Scan(&text); err != nil {
		return err
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("cellstore: bad txid %q: %w", text, err)
	}
	c.txid, c.txidOK = n, true
	return nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS kv (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  meta  BLOB,
  updated_ms INTEGER NOT NULL,
  expires_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cell_meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
INSERT OR IGNORE INTO cell_meta(k, v) VALUES ('txid', '0');
INSERT OR IGNORE INTO cell_meta(k, v) VALUES ('created_ms', CAST(strftime('%s','now') AS TEXT) || '000');
`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return err
	}
	return ensureKVExpires(ctx, db)
}

// ensureKVExpires adds the expires_ms column to a pre-existing kv table (the
// CREATE TABLE IF NOT EXISTS above only creates it for fresh databases) and
// always ensures the partial expiry index.
//
// The index must be created here rather than in the schema block: legacy tables
// lack the column, so an index in the schema would fail before the ALTER runs.
// Without it, NextExpiry/DeleteExpired (the TTL sweeper) scan the whole table
// (ADR-157 regression fix).
func ensureKVExpires(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(kv)`)
	if err != nil {
		return err
	}
	hasExpires := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "expires_ms" {
			hasExpires = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if !hasExpires {
		if _, err := db.ExecContext(ctx, `ALTER TABLE kv ADD COLUMN expires_ms INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	_, err = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS kv_expires ON kv(expires_ms) WHERE expires_ms > 0`)
	return err
}

// OpenAt opens an existing SQLite database file directly (e.g. a restored
// snapshot). The scope is advisory and may be left zero.
func OpenAt(ctx context.Context, path string) (*Cell, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	// OpenAt callers (restored snapshots) read more than they write; the mirror
	// is loaded lazily on the first Tx if it is not set here.

	return &Cell{DB: db, Path: path}, nil
}

// Cell is a single cell's SQLite database handle.
//
// It keeps a second connection free for a long-running read transaction (the
// WAL read lock) so a controlled TRUNCATE checkpoint can be taken without
// losing frames the capture has not consumed. See AcquireReadLock.
type Cell struct {
	DB     *sql.DB
	Scope  cell.Scope
	Path   string
	closed bool

	// writeMu serializes write transactions in-process. SQLite allows one
	// writer, so without it concurrent writers collide and retry on
	// SQLITE_BUSY, which lowers throughput as concurrency rises. It also guards
	// txid, the in-memory mirror of cell_meta.txid that Tx advances without a
	// read-back. The mirror is safe because exactly one process owns a cell at a
	// time and writeMu serializes the transactions within it; it is reloaded from
	// cell_meta on open, so a restart or a takeover continues the sequence.
	writeMu sync.Mutex
	txid    uint64
	txidOK  bool

	readConn *sql.Conn
	readTx   *sql.Tx

	// paged marks a cell opened through the paged VFS; hydrateStop cancels its
	// background fill. Close releases both (ADR-160).
	paged       bool
	hydrateStop func()
}

// Close closes the cell.
func (c *Cell) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	_ = c.ReleaseReadLock(context.Background())
	err := c.DB.Close()
	if c.hydrateStop != nil {
		c.hydrateStop()
		c.hydrateStop = nil
	}
	if c.paged {
		// The local file is a cache and must be re-registered on the next open;
		// keep the bytes (eviction snapshot) but drop the VFS registration.
		pagedvfs.Release(c.Path)
		c.paged = false
	}
	return err
}

// AcquireReadLock opens a long-running read transaction on a dedicated
// connection. It pins the WAL read mark, so an automatic PASSIVE checkpoint can
// backfill but never reset the WAL under the capture. Idempotent.
func (c *Cell) AcquireReadLock(ctx context.Context) error {
	if c.readTx != nil {
		return nil
	}
	conn, err := c.DB.Conn(ctx)
	if err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return err
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(1) FROM kv`).Scan(&n); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		return err
	}
	c.readConn, c.readTx = conn, tx
	return nil
}

// ReleaseReadLock ends the read-lock transaction. Idempotent.
func (c *Cell) ReleaseReadLock(_ context.Context) error {
	if c.readTx == nil {
		return nil
	}
	err := c.readTx.Rollback()
	_ = c.readConn.Close()
	c.readTx, c.readConn = nil, nil
	return err
}

// CheckpointTruncate performs a safe TRUNCATE checkpoint: it releases the read
// lock so SQLite can reset the WAL, runs the checkpoint, then re-acquires the
// lock. The capture must re-baseline afterwards (the WAL salt rotates) and emit
// a full snapshot, which is what makes the truncation safe.
func (c *Cell) CheckpointTruncate(ctx context.Context) error {
	// Release the read lock if the caller is holding one; do not re-acquire it,
	// so a quiesced caller can truncate without pinning a connection. A short
	// busy timeout bounds the stall if another connection holds a read mark.
	_ = c.ReleaseReadLock(ctx)
	conn, err := c.DB.Conn(ctx)
	if err != nil {
		return err
	}
	_, _ = conn.ExecContext(ctx, "PRAGMA busy_timeout=500")
	_, err = conn.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	_ = conn.Close()
	return err
}

// SafeCheckpoint pauses writers, truncates the WAL, and returns the committed
// txid watermark. It is the checkpointer hook the capture uses to bound per-cell
// WAL growth safely (capture re-baselines with a full snapshot afterwards).
func (c *Cell) SafeCheckpoint(ctx context.Context) (uint64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.CheckpointTruncate(ctx); err != nil {
		return 0, err
	}
	if err := c.loadTxID(ctx); err != nil {
		return 0, err
	}
	return c.txid, nil
}

// Put writes a key/value with optional metadata.
func (c *Cell) Put(ctx context.Context, key string, value, meta []byte) error {
	_, err := c.PutTx(ctx, key, value, meta)
	return err
}

// Tx runs fn in one SQLite transaction and advances the cell txid in the same
// transaction, returning the committed txid. Every captured write must use it:
// capture derives one txid per WAL transaction, so a logical write that commits
// more than once (or bumps the txid in a separate commit) would desynchronise the
// txid watermark and let Wait() release an ack before the write is durable.
func (c *Cell) Tx(ctx context.Context, fn func(tx *sql.Tx) error) (uint64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return 0, err
	}
	if err := c.loadTxID(ctx); err != nil {
		return 0, err
	}
	next := c.txid + 1
	if _, err := tx.ExecContext(ctx, `UPDATE cell_meta SET v=? WHERE k='txid'`, strconv.FormatUint(next, 10)); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	c.txid = next
	return next, nil
}

// txNoBump runs fn in one transaction WITHOUT advancing the txid. It exists so
// benchmarks can isolate the txid bookkeeping cost; production writes must use
// Tx so capture's durability watermark covers them.
func (c *Cell) txNoBump(ctx context.Context, fn func(tx *sql.Tx) error) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// PutTx writes a key/value and advances the cell txid in one SQLite
// transaction, returning the committed txid.
func (c *Cell) PutTx(ctx context.Context, key string, value, meta []byte) (uint64, error) {
	return c.PutTxOpts(ctx, key, value, meta, 0)
}

// PutTxOpts writes a key/value with optional metadata and an absolute expiry
// (unix millis; 0 = never expires) in one transaction, returning the txid.
func (c *Cell) PutTxOpts(ctx context.Context, key string, value, meta []byte, expiresMs int64) (uint64, error) {
	now := time.Now().UnixMilli()
	if expiresMs > 0 && expiresMs <= now {
		expiresMs = now // already expired: still stored, filtered out on read
	}
	return c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO kv(key, value, meta, updated_ms, expires_ms) VALUES(?,?,?,?,?)
			 ON CONFLICT(key) DO UPDATE SET value=excluded.value, meta=excluded.meta, updated_ms=excluded.updated_ms, expires_ms=excluded.expires_ms`,
			key, value, meta, now, expiresMs)
		return err
	})
}

// PutOpts is PutTxOpts without returning the txid.
func (c *Cell) PutOpts(ctx context.Context, key string, value, meta []byte, expiresMs int64) error {
	_, err := c.PutTxOpts(ctx, key, value, meta, expiresMs)
	return err
}

// Get reads a key. Missing -> ErrNotFound.
func (c *Cell) Get(ctx context.Context, key string) (value, meta []byte, err error) {
	err = c.DB.QueryRowContext(ctx, `SELECT value, meta FROM kv WHERE key=? AND (expires_ms=0 OR expires_ms>?)`, key, time.Now().UnixMilli()).Scan(&value, &meta)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	return value, meta, err
}

// Delete removes a key and advances the txid in one transaction.
func (c *Cell) Delete(ctx context.Context, key string) error {
	_, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM kv WHERE key=?`, key)
		return err
	})
	return err
}

// List returns up to limit keys with the given prefix that sort after `after`,
// in ascending order (bounded KV-list primitive). limit <= 0 defaults to 100 and
// is capped at 1000.
func (c *Cell) List(ctx context.Context, prefix, after string, limit int) ([]string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	lo := prefix
	if after != "" {
		lo = after + "\x00"
	}
	q := `SELECT key FROM kv WHERE key >= ? AND (expires_ms=0 OR expires_ms>?)`
	args := []any{lo, time.Now().UnixMilli()}
	if prefix != "" {
		q += ` AND key < ?`
		args = append(args, prefix+"\xff")
	}
	q += ` ORDER BY key ASC LIMIT ?`
	args = append(args, limit)
	rows, err := c.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ListMeta is List with per-key metadata and expiry (for KV list with metadata).
func (c *Cell) ListMeta(ctx context.Context, prefix, after string, limit int) ([]KVEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	lo := prefix
	if after != "" {
		lo = after + "\x00"
	}
	q := `SELECT key, meta, expires_ms FROM kv WHERE key >= ? AND (expires_ms=0 OR expires_ms>?)`
	args := []any{lo, time.Now().UnixMilli()}
	if prefix != "" {
		q += ` AND key < ?`
		args = append(args, prefix+"\xff")
	}
	q += ` ORDER BY key ASC LIMIT ?`
	args = append(args, limit)
	rows, err := c.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KVEntry
	for rows.Next() {
		var e KVEntry
		if err := rows.Scan(&e.Key, &e.Meta, &e.ExpiresMs); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// KVEntry is a KV row's key, metadata and absolute expiry (0 = never).
type KVEntry struct {
	Key       string
	Meta      []byte
	ExpiresMs int64
}

// NextExpiry returns the earliest future expiry across the cell's KV rows, or 0
// when none is set.
func (c *Cell) NextExpiry(ctx context.Context) (int64, error) {
	var next sql.NullInt64
	if err := c.DB.QueryRowContext(ctx, `SELECT MIN(expires_ms) FROM kv WHERE expires_ms>0`).Scan(&next); err != nil {
		return 0, err
	}
	if !next.Valid {
		return 0, nil
	}
	return next.Int64, nil
}

// DeleteExpired removes KV rows whose expiry has passed, in one captured
// transaction, and returns how many were deleted plus the next future expiry
// (0 = none). It is a no-op (no write) when nothing is due.
func (c *Cell) DeleteExpired(ctx context.Context, nowMs int64) (int, int64, error) {
	next, err := c.NextExpiry(ctx)
	if err != nil {
		return 0, 0, err
	}
	var due int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM kv WHERE expires_ms>0 AND expires_ms<=?`, nowMs).Scan(&due); err != nil {
		return 0, 0, err
	}
	if due == 0 {
		if next > 0 && next <= nowMs {
			next = 0
		}
		return 0, next, nil
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		_, derr := tx.ExecContext(ctx, `DELETE FROM kv WHERE expires_ms>0 AND expires_ms<=?`, nowMs)
		return derr
	})
	if err != nil {
		return 0, 0, err
	}
	nx, err := c.NextExpiry(ctx)
	return due, nx, err
}

// Scan iterates keys with the given prefix (operator use; not for hot paths).
func (c *Cell) Scan(ctx context.Context, prefix string, fn func(key string, value, meta []byte) error) error {
	rows, err := c.DB.QueryContext(ctx, `SELECT key, value, meta FROM kv WHERE key LIKE ? ORDER BY key`, prefix+"%")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v, m []byte
		if err := rows.Scan(&k, &v, &m); err != nil {
			return err
		}
		if err := fn(k, v, m); err != nil {
			return err
		}
	}
	return rows.Err()
}

// TxID returns the current monotonic transaction id from the in-memory mirror,
// loaded from cell_meta once per process. The mirror equals the persisted value
// after every committed Tx, so this is a cheap read on the write hot path.
func (c *Cell) TxID(ctx context.Context) (uint64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.loadTxID(ctx); err != nil {
		return 0, err
	}
	return c.txid, nil
}

// Snapshot writes a consistent copy of the database to dest using VACUUM INTO.
func (c *Cell) Snapshot(ctx context.Context, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	_ = os.Remove(dest)
	// VACUUM INTO does not accept bound parameters; escape single quotes.
	esc := strings.ReplaceAll(dest, "'", "''")
	_, err := c.DB.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", esc))
	return err
}

// WALPath returns the path of the write-ahead log file.
func (c *Cell) WALPath() string { return c.Path + "-wal" }

// SnapshotPages reads the database file as a full-page image
// (pageSize, commit, pages). It is the source for a KindSnapshot segment.
//
// The caller must have checkpointed first (CheckpointTruncate): after a
// checkpoint the WAL is backfilled, so the file is the current, consistent
// image and — unlike VACUUM INTO — its page numbers still match the WAL frames
// that later deltas carry.
func (c *Cell) SnapshotPages(ctx context.Context) (uint32, uint32, map[uint32][]byte, error) {
	if c.paged {
		// Capture reads the file directly (os.ReadFile); every page must be
		// materialized first or the snapshot would encode sparse zeros.
		if err := pagedvfs.HydrateAll(ctx, c.Path); err != nil {
			return 0, 0, nil, err
		}
		// The file is now a complete image; it no longer needs the paged VFS.
		removePagedMarker(c.Path)
	}
	return ReadDBPages(c.Path)
}

// ReadDBPages reads a SQLite database file as a full-page image
// (pageSize, commit, pages). The caller must ensure the file is a consistent
// image (e.g. after a checkpoint); page numbers match the WAL frames that
// later deltas carry. Used by capture and by the workerd actor supervisor.
func ReadDBPages(path string) (uint32, uint32, map[uint32][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(data) < 100 {
		return 0, 0, nil, fmt.Errorf("cellstore: snapshot too small")
	}
	pageSize := uint32(binary.BigEndian.Uint16(data[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize == 0 || len(data)%int(pageSize) != 0 {
		return 0, 0, nil, fmt.Errorf("cellstore: invalid page size %d for %d bytes", pageSize, len(data))
	}
	commit := uint32(len(data) / int(pageSize))
	pages := make(map[uint32][]byte, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		off := (int(pgno) - 1) * int(pageSize)
		page := make([]byte, pageSize)
		copy(page, data[off:off+int(pageSize)])
		pages[pgno] = page
	}
	return pageSize, commit, pages, nil
}

// Checkpoint forces a WAL checkpoint (used by tests and shutdown).
// TRUNCATE resets the WAL file and rotates its salt.
func (c *Cell) Checkpoint(ctx context.Context) error {
	_, err := c.DB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
