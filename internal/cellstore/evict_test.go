package cellstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// cellsMustExist reports whether the cache still holds the scope's cell.
func (s *Store) cellsMustExist(sc cell.Scope) (*Cell, bool) {
	p, err := s.Path(sc)
	if err != nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cells[p]
	return c, ok
}

func forceIdle(t *testing.T, s *Store, sc cell.Scope, age time.Duration) {
	t.Helper()
	p, err := s.Path(sc)
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	s.mu.Lock()
	if e, ok := s.byPath[p]; ok {
		e.Value.(*lruEnt).last = time.Now().Add(-age)
	} else {
		t.Fatalf("no lru entry for %s", p)
	}
	s.mu.Unlock()
}

func TestEvictIdleAndReopenContinuesTxID(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.IdleTTL = time.Hour
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}

	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if _, err := c.PutTx(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	forceIdle(t, s, sc, 2*time.Hour)

	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	p, _ := s.Path(sc)
	s.mu.Lock()
	_, cached := s.cells[p]
	s.mu.Unlock()
	if cached {
		t.Fatalf("cell still cached after idle eviction")
	}

	// Reopen: the txid mirror must reload from cell_meta and continue.
	c2, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	id, err := c2.PutTx(ctx, "k2", []byte("v"), nil)
	if err != nil {
		t.Fatalf("put after reopen: %v", err)
	}
	if id != 2 {
		t.Fatalf("txid after reopen = %d, want 2", id)
	}
}

func TestSweepSkipsBusyKey(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.IdleTTL = time.Hour
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("cell: %v", err)
	}
	forceIdle(t, s, sc, 2*time.Hour)

	end := s.BeginRequest(sc.String())
	if n := s.Sweep(ctx); n != 0 {
		t.Fatalf("sweep evicted %d while busy, want 0", n)
	}
	end()
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep after end evicted %d, want 1", n)
	}
}

func TestSweepEnforcesMaxOpenLRU(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.MaxOpenCells = 2
	scopes := []cell.Scope{
		{Namespace: "demo", Class: "__kv__", ID: "a"},
		{Namespace: "demo", Class: "__kv__", ID: "b"},
		{Namespace: "demo", Class: "__kv__", ID: "c"},
	}
	for _, sc := range scopes {
		if _, err := s.Cell(ctx, sc); err != nil {
			t.Fatalf("cell %s: %v", sc, err)
		}
	}
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	s.mu.Lock()
	n := len(s.cells)
	_, hasA := s.cells[mustPath(t, s, scopes[0])]
	_, hasC := s.cells[mustPath(t, s, scopes[2])]
	s.mu.Unlock()
	if n != 2 {
		t.Fatalf("cached cells = %d, want 2", n)
	}
	if hasA {
		t.Fatalf("LRU cell a should have been evicted")
	}
	if !hasC {
		t.Fatalf("most-recent cell c should be cached")
	}
}

func mustPath(t *testing.T, s *Store, sc cell.Scope) string {
	t.Helper()
	p, err := s.Path(sc)
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	return p
}

func TestDropRunsBeforeClose(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.IdleTTL = time.Hour
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("cell: %v", err)
	}
	var dropped []cell.Scope
	s.Drop = func(_ context.Context, got cell.Scope) error {
		// The cell must still be usable when Drop runs (capture is stopped, the
		// handle is closed only afterwards).
		dropped = append(dropped, got)
		return nil
	}
	forceIdle(t, s, sc, 2*time.Hour)
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	if len(dropped) != 1 || dropped[0] != sc {
		t.Fatalf("drop called with %v, want [%v]", dropped, sc)
	}
}

func TestDeleteNamespaceClearsLRU(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	if _, err := s.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "a"}); err != nil {
		t.Fatalf("cell: %v", err)
	}
	if err := s.DeleteNamespace(ctx, "acme"); err != nil {
		t.Fatalf("delete ns: %v", err)
	}
	s.mu.Lock()
	n, lru := len(s.cells), s.lru.Len()
	s.mu.Unlock()
	if n != 0 || lru != 0 {
		t.Fatalf("after DeleteNamespace cells=%d lru=%d, want 0/0", n, lru)
	}
}

func TestStatsCountsEvictions(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.IdleTTL = time.Hour
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("cell: %v", err)
	}
	forceIdle(t, s, sc, 2*time.Hour)
	s.Sweep(ctx)
	open, evicted, sweeps := s.Stats()
	if open != 0 || evicted != 1 || sweeps != 1 {
		t.Fatalf("stats open=%d evicted=%d sweeps=%d, want 0/1/1", open, evicted, sweeps)
	}
}

func TestSweepPerCellGate(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	s.IdleTTL = time.Hour
	a := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "a"} // LRU (older)
	b := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "b"} // MRU (newer)
	if _, err := s.Cell(ctx, a); err != nil {
		t.Fatalf("cell a: %v", err)
	}
	if _, err := s.Cell(ctx, b); err != nil {
		t.Fatalf("cell b: %v", err)
	}
	forceIdle(t, s, a, 2*time.Hour)
	forceIdle(t, s, b, 2*time.Hour)

	end := s.BeginRequest(a.String()) // only cell a is busy
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d, want 1 (only the non-busy cell)", n)
	}
	s.mu.Lock()
	_, hasA := s.cells[mustPath(t, s, a)]
	_, hasB := s.cells[mustPath(t, s, b)]
	s.mu.Unlock()
	if !hasA || hasB {
		t.Fatalf("per-cell gate wrong: hasA=%v hasB=%v, want a cached, b evicted", hasA, hasB)
	}
	end()
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep after end evicted %d, want 1", n)
	}
}

// TestForgetWaitsForInFlightRequest covers the eviction-drain fix: Forget must
// not close/delete a cell while a request is still using it (it waits for the
// BeginRequest gate), and BeginRequest must not start a new request while an
// eviction is in progress.
func TestForgetWaitsForInFlightRequest(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "forget-wait"}
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("cell: %v", err)
	}
	end := s.BeginRequest(sc.String())
	done := make(chan error, 1)
	go func() { done <- s.Forget(ctx, sc) }()
	select {
	case err := <-done:
		end()
		t.Fatalf("Forget returned while a request was in flight: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("forget: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forget did not complete after the request drained")
	}
	// The cell is gone and reopening it starts a fresh one.
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("reopen: %v", err)
	}
}

// TestForgetWithVerifyRefuses covers the disk-janitor re-check: when the verify
// hook reports the bucket manifest no longer covers the cell, ForgetWithVerify
// must leave the cell in place.
func TestForgetWithVerifyRefuses(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "verify"}
	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	refused := errors.New("not covered")
	if err := s.ForgetWithVerify(ctx, sc, func() error { return refused }); !errors.Is(err, refused) {
		t.Fatalf("ForgetWithVerify = %v, want the verify error", err)
	}
	if _, ok := s.cellsMustExist(sc); !ok {
		t.Fatal("cell was evicted despite the verify refusal")
	}
	// With a passing verify it evicts.
	if err := s.ForgetWithVerify(ctx, sc, func() error { return nil }); err != nil {
		t.Fatalf("ForgetWithVerify (ok): %v", err)
	}
	if _, ok := s.cellsMustExist(sc); ok {
		t.Fatal("cell survived a successful ForgetWithVerify")
	}
}

// TestDeleteNamespaceDrainsAndDrops covers the namespace purge path: cached
// scopes must be evicted through the regular drain (waiting for in-flight
// requests and calling the Drop hook to stop capture) before the files go away.
func TestDeleteNamespaceDrainsAndDrops(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	var dropped []string
	s.Drop = func(_ context.Context, sc cell.Scope) error {
		dropped = append(dropped, sc.String())
		return nil
	}
	a := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "a"}
	b := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "b"}
	other := cell.Scope{Namespace: "other", Class: "__kv__", ID: "c"}
	for _, sc := range []cell.Scope{a, b, other} {
		if _, err := s.Cell(ctx, sc); err != nil {
			t.Fatalf("cell %s: %v", sc, err)
		}
	}
	// A request is in flight for one of the namespace's cells: the delete must
	// wait for it (and must not close the database under it).
	end := s.BeginRequest(a.String())
	done := make(chan error, 1)
	go func() { done <- s.DeleteNamespace(ctx, "acme") }()
	select {
	case err := <-done:
		end()
		t.Fatalf("DeleteNamespace returned with a request in flight: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("delete namespace: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteNamespace did not complete after the request drained")
	}
	// Both scopes were dropped (capture stopped) and neither is cached anymore.
	if len(dropped) != 2 {
		t.Fatalf("Drop calls = %v, want the namespace's two scopes", dropped)
	}
	for _, sc := range []cell.Scope{a, b} {
		if _, ok := s.cellsMustExist(sc); ok {
			t.Fatalf("scope %s still cached after namespace delete", sc)
		}
	}
	// Other namespaces are untouched.
	if _, ok := s.cellsMustExist(other); !ok {
		t.Fatal("unrelated namespace was evicted")
	}
}

// TestForgetPrefixDropsMatchingScopes covers the purge hook's local cleanup:
// only scopes whose string starts with the prefix are dropped, others stay
// (ADR-142).
func TestForgetPrefixDropsMatchingScopes(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	var dropped []string
	s.Drop = func(_ context.Context, sc cell.Scope) error {
		dropped = append(dropped, sc.String())
		return nil
	}
	match := cell.Scope{Namespace: "acme", Class: "__do__", ID: "api~Room~shard1"}
	other := cell.Scope{Namespace: "acme", Class: "__do__", ID: "api2~Room~shard1"}
	kv := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	for _, sc := range []cell.Scope{match, other, kv} {
		if _, err := s.Cell(ctx, sc); err != nil {
			t.Fatalf("cell %s: %v", sc, err)
		}
	}
	n, err := s.ForgetPrefix(ctx, "acme/__do__/api~")
	if err != nil {
		t.Fatalf("forget prefix: %v", err)
	}
	if n != 1 || len(dropped) != 1 || dropped[0] != match.String() {
		t.Fatalf("forget prefix dropped %v (n=%d), want only %s", dropped, n, match.String())
	}
	if _, ok := s.cellsMustExist(match); ok {
		t.Fatal("matching scope still cached")
	}
	for _, sc := range []cell.Scope{other, kv} {
		if _, ok := s.cellsMustExist(sc); !ok {
			t.Fatalf("unrelated scope %s was evicted", sc.String())
		}
	}
	// Idempotent: nothing left to forget.
	if n, err := s.ForgetPrefix(ctx, "acme/__do__/api~"); err != nil || n != 0 {
		t.Fatalf("re-run = %d, %v; want 0", n, err)
	}
}
