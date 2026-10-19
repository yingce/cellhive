package timer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type recordingDispatcher struct {
	got []Timer
	err error
}

func (d *recordingDispatcher) Dispatch(_ context.Context, t Timer) error {
	if d.err != nil {
		return d.err
	}
	d.got = append(d.got, t)
	return nil
}

func newRunner(t *testing.T, d Dispatcher) (*Runner, *Registry, *Store) {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__timers__", ID: "run"}
	c, err := cs.Open(context.Background(), sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	st, err := NewStore(context.Background(), c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	reg := NewRegistry()
	reg.Add(sc.String())
	r := &Runner{
		Registry:   reg,
		Dispatcher: d,
		Batch:      10,
		FiredTTL:   0, // default
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Open: func(context.Context, string) (*Store, error) {
			return st, nil
		},
	}
	return r, reg, st
}

func TestRunnerDispatchesDueExactlyOnce(t *testing.T) {
	ctx := context.Background()
	d := &recordingDispatcher{}
	r, _, st := newRunner(t, d)
	due := New(1000, KindCron, "demo/__timers__/run", "slot")
	future := New(9_000_000_000_000, KindCron, "demo/__timers__/run", "later")
	_ = st.Upsert(ctx, due)
	_ = st.Upsert(ctx, future)
	r.Now = func() time.Time { return time.UnixMilli(2000) }

	n, err := r.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("pass1 = %d, %v", n, err)
	}
	if len(d.got) != 1 || d.got[0].Token != due.Token {
		t.Fatalf("dispatched = %+v", d.got)
	}
	// A second pass must not redispatch the fired timer.
	if n, _ := r.Pass(ctx); n != 0 {
		t.Fatalf("pass2 dispatched %d, want 0", n)
	}
	if len(d.got) != 1 {
		t.Fatalf("redelivered: %+v", d.got)
	}
}

func TestRunnerRejectsStaleReplicaAndCommitsFired(t *testing.T) {
	ctx := context.Background()
	d := &recordingDispatcher{}
	r, reg, st := newRunner(t, d)
	tm := New(1000, KindCron, "demo/__timers__/run", "slot")
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return time.UnixMilli(2000) }
	owned := false
	commits := 0
	r.Own = func(context.Context, string) (bool, error) { return owned, nil }
	r.Commit = func(_ context.Context, _ string, fn func() error) error { commits++; return fn() }
	if n, err := r.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("stale pass = %d, %v", n, err)
	}
	if len(d.got) != 0 || commits != 0 {
		t.Fatalf("stale copy dispatched or committed: calls=%d commits=%d", len(d.got), commits)
	}
	owned = true
	reg.Add(tm.Scope)
	if n, err := r.Pass(ctx); err != nil || n != 1 {
		t.Fatalf("owner pass = %d, %v", n, err)
	}
	if len(d.got) != 1 || commits == 0 {
		t.Fatalf("owner result: calls=%d commits=%d", len(d.got), commits)
	}
	if fired, _ := st.IsFired(ctx, tm.Token, 2000); !fired {
		t.Fatal("fired token missing")
	}
}

func TestRunnerIdlePassDoesNotWriteCell(t *testing.T) {
	ctx := context.Background()
	r, reg, st := newRunner(t, &recordingDispatcher{})
	r.Now = func() time.Time { return time.UnixMilli(2000) }
	reg.Add("demo/__timers__/run")
	before, err := st.cell.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := r.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("idle pass = %d, %v", n, err)
	}
	after, err := st.cell.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("idle pass advanced txid from %d to %d", before, after)
	}
}

func TestRunnerRetriesOnDispatchFailure(t *testing.T) {
	ctx := context.Background()
	d := &recordingDispatcher{err: errors.New("endpoint down")}
	r, _, st := newRunner(t, d)
	tm := New(1000, KindQueueRetry, "demo/__timers__/run", "r1")
	_ = st.Upsert(ctx, tm)
	r.Now = func() time.Time { return time.UnixMilli(2000) }

	if n, _ := r.Pass(ctx); n != 0 {
		t.Fatalf("failed dispatch should not count as fired")
	}
	if ok, _ := st.IsFired(ctx, tm.Token, 2000); ok {
		t.Fatalf("timer marked fired despite dispatch failure")
	}
	// Non-cron timers keep their existing immediate retry semantics.
	d.err = nil
	if n, _ := r.Pass(ctx); n != 1 {
		t.Fatalf("retry pass did not deliver")
	}
}

func TestRunnerCronFailureBacksOffAndPreservesSlot(t *testing.T) {
	ctx := t.Context()
	d := &recordingDispatcher{err: errors.New("handler failed")}
	r, _, st := newRunner(t, d)
	tm := New(1000, KindCron, "demo/__timers__/run", "slot")
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return time.UnixMilli(2000) }
	if n, _ := r.Pass(ctx); n != 0 {
		t.Fatalf("failed cron counted: %d", n)
	}
	d.err = nil
	if n, _ := r.Pass(ctx); n != 0 {
		t.Fatalf("cron retried before backoff: %d", n)
	}
	r.Now = func() time.Time { return time.UnixMilli(7000) }
	if n, _ := r.Pass(ctx); n != 1 || len(d.got) != 1 || d.got[0].DueAtMs != tm.DueAtMs {
		t.Fatalf("cron retry lost original slot: n=%d got=%+v", n, d.got)
	}
}

func TestRunnerBatchBounds(t *testing.T) {
	ctx := context.Background()
	d := &recordingDispatcher{}
	r, reg, st := newRunner(t, d)
	r.Batch = 1
	for i := 0; i < 3; i++ {
		_ = st.Upsert(ctx, New(int64(1000+i), KindQueueDelay, reg.List()[0], string(rune('a'+i))))
	}
	r.Now = func() time.Time { return time.UnixMilli(2000) }
	if n, _ := r.Pass(ctx); n != 1 {
		t.Fatalf("batch=1 dispatched %d", n)
	}
}

func TestRegistryDueFiltersByTime(t *testing.T) {
	now := time.Now().UnixMilli()
	r := NewRegistry()
	r.Arm("a", now-time.Second.Milliseconds())
	r.Arm("b", now+time.Hour.Milliseconds())
	r.Add("c") // unknown due -> always checked
	due := map[string]bool{}
	for _, s := range r.Due(now) {
		due[s] = true
	}
	if !due["a"] || !due["c"] || due["b"] {
		t.Fatalf("due = %v, want a and c only", due)
	}
}

func TestRunnerSkipsFarFutureScope(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Open(ctx, cell.Scope{Namespace: "demo", Class: "__timers__", ID: "run"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	st, err := NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	opens := 0
	reg := NewRegistry()
	r := &Runner{
		Registry: reg, Dispatcher: NoopDispatcher{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Open: func(context.Context, string) (*Store, error) {
			opens++
			return st, nil
		},
	}
	reg.Arm("demo/__timers__/run", time.Now().Add(time.Hour).UnixMilli())
	if _, err := r.Pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if opens != 0 {
		t.Fatalf("opened %d far-future scopes, want 0", opens)
	}
	reg.Arm("demo/__timers__/run", time.Now().Add(-time.Second).UnixMilli())
	if _, err := r.Pass(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if opens != 1 {
		t.Fatalf("opened %d due scopes, want 1", opens)
	}
	// No pending timers left -> deregistered.
	if len(reg.List()) != 0 {
		t.Fatalf("registry = %v, want empty after dispatch", reg.List())
	}
}

type fakeIndex struct {
	puts []string
	dels []string
	fail bool
}

func (f *fakeIndex) Put(_ context.Context, scope string, _ int64, _, _ string) error {
	if f.fail {
		return errors.New("wake index unavailable")
	}
	f.puts = append(f.puts, scope)
	return nil
}
func (f *fakeIndex) Delete(_ context.Context, scope string) error {
	if f.fail {
		return errors.New("wake index unavailable")
	}
	f.dels = append(f.dels, scope)
	return nil
}

func TestStoreSyncsWakeIndex(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Open(ctx, cell.Scope{Namespace: "demo", Class: "__kv__", ID: "sessions"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	st, err := NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	fi := &fakeIndex{}
	st.Index = fi

	tm := New(time.Now().Add(time.Minute).UnixMilli(), KindKVExpire, "demo/__kv__/sessions", "kv-expire")
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// ADR-177: the entry is published before the commit and corrected after,
	// so at least one Put names the scope (and a durable row exists).
	if len(fi.puts) == 0 || fi.puts[0] != "demo/__kv__/sessions" {
		t.Fatalf("index puts = %v, want the scope", fi.puts)
	}
	if next, _ := st.NextDue(ctx); next != tm.DueAtMs {
		t.Fatalf("next due = %d, want %d", next, tm.DueAtMs)
	}
	if err := st.RemoveByOccurrence(ctx, "kv-expire"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if len(fi.dels) != 1 || fi.dels[0] != "demo/__kv__/sessions" {
		t.Fatalf("index dels = %v, want the scope", fi.dels)
	}
}

// TestUpsertFailsClosedWhenIndexDown covers the ADR-177 invariant: the wake
// index is published before the timer row commits, so when the index write
// fails the timer is not committed (no wake can be lost) and the caller can
// retry.
func TestUpsertFailsClosedWhenIndexDown(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Open(ctx, cell.Scope{Namespace: "demo", Class: "__kv__", ID: "sessions"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	st, err := NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	fi := &fakeIndex{fail: true}
	st.Index = fi

	tm := New(time.Now().Add(time.Minute).UnixMilli(), KindKVExpire, "demo/__kv__/sessions", "kv-expire")
	if err := st.Upsert(ctx, tm); err == nil {
		t.Fatal("upsert with a down index should fail closed")
	}
	if next, _ := st.NextDue(ctx); next != 0 {
		t.Fatalf("timer committed despite index failure: next=%d", next)
	}
	// The index recovers: the same write now commits and publishes.
	fi.fail = false
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatalf("upsert after recovery: %v", err)
	}
	if next, _ := st.NextDue(ctx); next != tm.DueAtMs {
		t.Fatalf("next due = %d, want %d", next, tm.DueAtMs)
	}
}

// TestSyncIndexRepairsMissingEntry is the repair entry point used on claim/open
// and by the periodic local repair pass.
func TestSyncIndexRepairsMissingEntry(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Open(ctx, cell.Scope{Namespace: "demo", Class: "__kv__", ID: "sessions"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	st, err := NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	fi := &fakeIndex{}
	st.Index = fi
	tm := New(time.Now().Add(time.Minute).UnixMilli(), KindKVExpire, "demo/__kv__/sessions", "kv-expire")
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Simulate an entry lost to a crash: clear the recorded writes.
	fi.puts = nil
	fi.dels = nil
	if err := st.SyncIndex(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(fi.puts) != 1 || fi.puts[0] != "demo/__kv__/sessions" {
		t.Fatalf("repair puts = %v, want the scope", fi.puts)
	}
}
