package timer

import (
	"context"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Open(context.Background(), cell.Scope{Namespace: "demo", Class: "__timers__", ID: "main"})
	if err != nil {
		t.Fatalf("open cell: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	st, err := NewStore(context.Background(), c)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return st
}

func TestStoreDueOrderingAndDedup(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	sc := "app/__kv__/main"
	// Same identity -> same token.
	if TokenFor(sc, KindCron, 1000, "slot1") != New(1000, KindCron, sc, "slot1").Token {
		t.Fatalf("TokenFor mismatch")
	}
	a := New(1000, KindQueueDelay, sc, "a")
	b := New(500, KindQueueDelay, sc, "b")
	c := New(2000, KindCron, sc, "slot2")
	for _, tm := range []Timer{a, b, c} {
		if err := st.Upsert(ctx, tm); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	// Re-upserting the same identity is idempotent.
	if err := st.Upsert(ctx, a); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if n, _ := st.Count(ctx); n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
	due, err := st.Due(ctx, 1000, 0)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if len(due) != 2 || due[0].Token != b.Token || due[1].Token != a.Token {
		t.Fatalf("due order = %+v", due)
	}
	// limit applies.
	if lim, _ := st.Due(ctx, 1000, 1); len(lim) != 1 || lim[0].Token != b.Token {
		t.Fatalf("limited due = %+v", lim)
	}
}

func TestStoreMarkFiredSuppressesUntilTTL(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	tm := New(1000, KindDOAlarm, "app/__do__/a", "actor1")
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := st.MarkFired(ctx, tm.Token, 5000); err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	// Fired suppresses redelivery and the row is gone.
	if due, _ := st.Due(ctx, 2000, 0); len(due) != 0 {
		t.Fatalf("fired timer still due: %+v", due)
	}
	if ok, _ := st.IsFired(ctx, tm.Token, 2000); !ok {
		t.Fatalf("IsFired = false within TTL")
	}
	if n, _ := st.Count(ctx); n != 0 {
		t.Fatalf("timer row not removed: %d", n)
	}
	// After the TTL the fired marker is pruned and no longer suppresses.
	if n, err := st.Prune(ctx, 6000); err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	if ok, _ := st.IsFired(ctx, tm.Token, 6000); ok {
		t.Fatalf("IsFired = true after TTL")
	}
}

func TestStoreRejectsInvalidKind(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	bad := Timer{DueAtMs: 1, Kind: "nope", Scope: "s", Occurrence: "o", Token: "t"}
	if err := st.Upsert(ctx, bad); err == nil {
		t.Fatalf("want error for invalid kind")
	}
}

func TestStoreTableIsPerCell(t *testing.T) {
	// The store's schema lives in the given cell DB; a second cell is isolated.
	cs, err := cellstore.New(filepath.Join(t.TempDir()))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c1, _ := cs.Open(context.Background(), cell.Scope{Namespace: "demo", Class: "__timers__", ID: "one"})
	c2, _ := cs.Open(context.Background(), cell.Scope{Namespace: "demo", Class: "__timers__", ID: "two"})
	defer c1.Close()
	defer c2.Close()
	s1, _ := NewStore(context.Background(), c1)
	s2, _ := NewStore(context.Background(), c2)
	_ = s1.Upsert(context.Background(), New(1, KindCron, "a", "x"))
	if n, _ := s2.Count(context.Background()); n != 0 {
		t.Fatalf("second cell leaked timer rows: %d", n)
	}
}
