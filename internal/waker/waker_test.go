package waker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
)

func newBucket(t *testing.T, dir string) bucket.Bucket {
	t.Helper()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return b
}

func TestElectionSingleLeaderAndTTLSteal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a := NewElection(newBucket(t, dir), "node-a", "s1", 10*time.Second)
	b := NewElection(newBucket(t, dir), "node-b", "s2", 10*time.Second)
	base := time.Unix(1_000_000, 0)
	a.Now = func() time.Time { return base }
	if ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a acquire = %v, %v", ok, err)
	}
	b.Now = func() time.Time { return base.Add(time.Second) }
	if ok, _ := b.Acquire(ctx); ok {
		t.Fatalf("b became leader while a holds a live lease")
	}
	// After a's TTL, b takes over.
	b.Now = func() time.Time { return base.Add(11 * time.Second) }
	if ok, err := b.Acquire(ctx); err != nil || !ok {
		t.Fatalf("b steal = %v, %v", ok, err)
	}
	if l, _, _ := b.Holder(ctx); l.Node != "node-b" {
		t.Fatalf("leader = %q, want node-b", l.Node)
	}
	// a steps down only if it is the holder; it is not anymore.
	if err := a.StepDown(ctx); err != nil {
		t.Fatalf("a step down: %v", err)
	}
	if l, ok, _ := b.Holder(ctx); !ok || l.Node != "node-b" {
		t.Fatalf("a non-holder step down changed the lease: %+v", l)
	}
}

type countingDispatcher struct{ n int }

func (c *countingDispatcher) Dispatch(context.Context, timer.Timer) error {
	c.n++
	return nil
}

func newWaker(t *testing.T, b bucket.Bucket, node string, d timer.Dispatcher, scope cell.Scope, st *timer.Store) *Waker {
	t.Helper()
	return &Waker{
		Election:   NewElection(b, node, node+"-s", 10*time.Second),
		DeadScopes: func(context.Context) ([]string, error) { return []string{scope.String()}, nil },
		Open:       func(context.Context, string) (*timer.Store, error) { return st, nil },
		Dispatcher: d,
		Batch:      10,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:        func() time.Time { return time.UnixMilli(2000) },
	}
}

func TestWakerOnlyLeaderDispatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := newBucket(t, dir)
	// One shared cell holding a due timer.
	cs, _ := cellstore.New(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__timers__", ID: "w"}
	c, _ := cs.Open(ctx, sc)
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.Upsert(ctx, timer.New(1000, timer.KindDOAlarm, sc.String(), "actor")); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	da := &countingDispatcher{}
	db := &countingDispatcher{}
	a := newWaker(t, b, "node-a", da, sc, st)
	bb := newWaker(t, b, "node-b", db, sc, st)

	// node-a becomes leader and dispatches.
	n, leader, err := a.Pass(ctx)
	if err != nil || !leader || n != 1 {
		t.Fatalf("a pass = %d leader=%v err=%v", n, leader, err)
	}
	// node-b is not leader and must not dispatch.
	n, leader, err = bb.Pass(ctx)
	if err != nil || leader || n != 0 {
		t.Fatalf("b pass = %d leader=%v err=%v (must be a non-leader no-op)", n, leader, err)
	}
	if da.n != 1 || db.n != 0 {
		t.Fatalf("dispatch counts a=%d b=%d", da.n, db.n)
	}
	// A second leader pass must not redispatch (fired).
	if n, _, _ := a.Pass(ctx); n != 0 {
		t.Fatalf("leader redelivered %d", n)
	}
}

func TestWakerFailoverDispatchesAfterLeaderExpiry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := newBucket(t, dir)
	cs, _ := cellstore.New(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__timers__", ID: "f"}
	c, _ := cs.Open(ctx, sc)
	defer c.Close()
	st, _ := timer.NewStore(ctx, c)
	base := time.Unix(2_000_000, 0)
	// Due only at base+10s, i.e. not yet due while a leads.
	_ = st.Upsert(ctx, timer.New(base.UnixMilli()+10_000, timer.KindCron, sc.String(), "slot"))
	da := &countingDispatcher{}
	db := &countingDispatcher{}
	a := newWaker(t, b, "node-a", da, sc, st)
	a.Now = func() time.Time { return base }
	a.Election.Now = func() time.Time { return base }
	bb := newWaker(t, b, "node-b", db, sc, st)
	bb.Now = func() time.Time { return base }
	bb.Election.Now = func() time.Time { return base }

	if n, leader, _ := a.Pass(ctx); !leader || n != 0 {
		t.Fatalf("a should lead with nothing due yet (n=%d leader=%v)", n, leader)
	}
	// While a is alive, b stays a follower.
	bb.Now = func() time.Time { return base.Add(2 * time.Second) }
	bb.Election.Now = func() time.Time { return base.Add(2 * time.Second) }
	if _, leader, _ := bb.Pass(ctx); leader {
		t.Fatalf("b became leader while a live")
	}
	// a dies (no renewal); after its TTL b takes over and dispatches.
	bb.Now = func() time.Time { return base.Add(11 * time.Second) }
	bb.Election.Now = func() time.Time { return base.Add(11 * time.Second) }
	n, leader, err := bb.Pass(ctx)
	if err != nil || !leader || n != 1 {
		t.Fatalf("b failover pass = %d leader=%v err=%v", n, leader, err)
	}
	if db.n != 1 {
		t.Fatalf("b dispatched %d, want 1", db.n)
	}
}

func TestWakerRecoverNodesLeaderOnly(t *testing.T) {
	ctx := context.Background()
	b := newBucket(t, t.TempDir())
	base := time.Unix(3_000_000, 0)
	var aCalls, bCalls int
	hook := func(c *int) func(context.Context) (int, int, int, error) {
		return func(context.Context) (int, int, int, error) { *c++; return 0, 0, 0, nil }
	}
	a := &Waker{
		Election:     NewElection(b, "node-a", "a-s", 10*time.Second),
		RecoverNodes: hook(&aCalls),
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	a.Election.Now = func() time.Time { return base }
	bb := &Waker{
		Election:     NewElection(b, "node-b", "b-s", 10*time.Second),
		RecoverNodes: hook(&bCalls),
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	bb.Election.Now = func() time.Time { return base.Add(time.Second) }
	// RecoverNodes runs even with no timer providers configured.
	if _, leader, err := a.Pass(ctx); err != nil || !leader {
		t.Fatalf("leader pass = leader=%v err=%v", leader, err)
	}
	if _, leader, _ := bb.Pass(ctx); leader {
		t.Fatalf("follower became leader")
	}
	if aCalls != 1 || bCalls != 0 {
		t.Fatalf("recover hook calls a=%d b=%d; want 1,0", aCalls, bCalls)
	}
}

// TestBackoffDelay: consecutive errors grow the loop delay exponentially up to
// the cap; success (fails=0) and a disabled cap keep the base interval
// (ADR-152).
func TestBackoffDelay(t *testing.T) {
	base := 5 * time.Second
	max := 40 * time.Second
	for _, tc := range []struct {
		fails int
		want  time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{10, 40 * time.Second}, // capped
	} {
		if got := backoffDelay(base, max, tc.fails); got != tc.want {
			t.Errorf("backoffDelay(fails=%d) = %v, want %v", tc.fails, got, tc.want)
		}
	}
	// max <= base disables extra backoff; a zero base falls back to 5s.
	if got := backoffDelay(base, 0, 5); got != base {
		t.Errorf("disabled cap = %v, want %v", got, base)
	}
	if got := backoffDelay(0, max, 3); got != 40*time.Second {
		t.Errorf("zero base = %v, want capped 40s", got)
	}
}
