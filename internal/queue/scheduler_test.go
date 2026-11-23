package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScheduledQueueIdleBudgetAndMissedWake(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	base := time.Unix(10000, 0)
	now := base
	st.now = func() time.Time { return now }
	ref := Ref{Namespace: "ns", Name: "q", Worker: "worker"}
	discovery, owner, lookups := 0, 0, 0
	r := &Runner{Store: st, Dispatch: &detailedDispatcher{},
		Queues:  func(context.Context) ([]Ref, error) { discovery++; return []Ref{ref}, nil },
		Filter:  func(context.Context, Ref) (bool, error) { owner++; return true, nil },
		Resolve: func(context.Context, string, string) (Ref, bool, error) { lookups++; return ref, true, nil },
	}
	next := r.scheduledPass(ctx, now, time.Time{})
	// Simulate a missed enqueue notification just after startup.
	if _, err := st.Send(ctx, "ns", "q", []byte("one"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 60; i++ {
		now = base.Add(time.Duration(i) * time.Second)
		next = r.scheduledPass(ctx, now, next)
	}
	if discovery != 1 || owner != 1 || lookups != 0 {
		t.Fatalf("59 idle ticks: discovery=%d owner=%d consumer=%d", discovery, owner, lookups)
	}
	now = base.Add(time.Minute)
	next = r.scheduledPass(ctx, now, next)
	if n, err := st.Depth(ctx, "ns", "q"); err != nil || n != 0 || discovery != 2 || lookups != 2 {
		t.Fatalf("reconcile: depth=%d discovery=%d consumer=%d err=%v", n, discovery, lookups, err)
	}
	if _, err := st.Send(ctx, "ns", "q", []byte("two"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	r.Nudge("ns", "q")
	r.Nudge("ns", "q")
	r.scheduledPass(ctx, now, next)
	if n, err := st.Depth(ctx, "ns", "q"); err != nil || n != 0 || discovery != 2 || lookups != 4 {
		t.Fatalf("enqueue: depth=%d discovery=%d consumer=%d err=%v", n, discovery, lookups, err)
	}
}

func TestScheduledQueueRestartRebuildsDelayBatchAndLease(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	base := time.Unix(1000, 0)
	now := base
	st.now = func() time.Time { return now }
	ref := Ref{Namespace: "ns", Name: "q", MaxBatchSize: 2, MaxBatchTimeoutSeconds: 5}
	if _, err := st.Send(ctx, "ns", "q", []byte("delay"), "text", 3, ""); err != nil {
		t.Fatal(err)
	}
	makeRunner := func() *Runner {
		return &Runner{Store: st, Dispatch: &detailedDispatcher{}, Queues: func(context.Context) ([]Ref, error) { return []Ref{ref}, nil }, Resolve: func(context.Context, string, string) (Ref, bool, error) { return ref, true, nil }}
	}
	r := makeRunner()
	next := r.scheduledPass(ctx, now, time.Time{})
	check := func(want time.Time) {
		t.Helper()
		if at, ok := r.deadlines[[2]string{"ns", "q"}]; !ok || !at.Equal(want) {
			t.Fatalf("wake=%v want=%v", at, want)
		}
	}
	check(base.Add(3 * time.Second))
	now = base.Add(3 * time.Second)
	next = r.scheduledPass(ctx, now, next)
	check(base.Add(8 * time.Second))
	msgs, err := st.Claim(ctx, "ns", "q", 1, 10000)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("claim=%v err=%v", msgs, err)
	}
	r = makeRunner()
	next = r.scheduledPass(ctx, now, time.Time{})
	check(base.Add(13 * time.Second))
	now = base.Add(13 * time.Second)
	r.scheduledPass(ctx, now, next)
	if n, err := st.Depth(ctx, "ns", "q"); err != nil || n != 0 {
		t.Fatalf("lease recovery depth=%d err=%v", n, err)
	}
}

func TestScheduledQueueConfigChangeAndInFlightNudge(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	now := time.Unix(1000, 0)
	st.now = func() time.Time { return now }
	if _, err := st.Send(ctx, "ns", "q", []byte("one"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	ref := Ref{Namespace: "ns", Name: "q", Worker: "old"}
	r := &Runner{Store: st, Dispatch: &detailedDispatcher{}, LeaseMs: 1000, Queues: func(context.Context) ([]Ref, error) { return []Ref{ref}, nil }}
	calls := 0
	r.Resolve = func(context.Context, string, string) (Ref, bool, error) {
		calls++
		if calls == 2 {
			ref.Worker = "new"
			// Producer notification races the old wave's lease rearm.
			r.Nudge("ns", "q")
		}
		return ref, true, nil
	}
	next := r.scheduledPass(ctx, now, time.Time{})
	if at, ok := r.deadlines[[2]string{"ns", "q"}]; !ok || !at.IsZero() {
		t.Fatalf("lost concurrent nudge: %v", at)
	}
	if n, err := st.Depth(ctx, "ns", "q"); err != nil || n != 1 {
		t.Fatalf("stale dispatch: depth=%d err=%v", n, err)
	}
	next = r.scheduledPass(ctx, now, next)
	now = now.Add(time.Second)
	r.scheduledPass(ctx, now, next)
	if n, err := st.Depth(ctx, "ns", "q"); err != nil || n != 0 {
		t.Fatalf("new configuration: depth=%d err=%v", n, err)
	}
}

func TestScheduledRetryWakesWithoutDiscovery(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(10000, 0)
	st := newStore(t)
	st.now = func() time.Time { return now }
	if _, err := st.Send(ctx, "ns", "q", []byte("one"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	ref := Ref{Namespace: "ns", Name: "q", RetryDelaySeconds: 4, MaxConcurrency: 2}
	d := &detailedDispatcher{err: errors.New("retry")}
	listed := 0
	r := &Runner{Store: st, Dispatch: d, Queues: func(context.Context) ([]Ref, error) { listed++; return []Ref{ref}, nil },
		Resolve: func(context.Context, string, string) (Ref, bool, error) { return ref, true, nil }}
	next := r.scheduledPass(ctx, now, time.Time{})
	want := now.Add(4 * time.Second)
	if got := r.deadlines[[2]string{"ns", "q"}]; !got.Equal(want) {
		t.Fatalf("retry deadline=%v want=%v", got, want)
	}
	d.err = nil
	now = want
	r.scheduledPass(ctx, now, next)
	if depth, err := st.Depth(ctx, "ns", "q"); err != nil || depth != 0 || listed != 1 || len(r.deadlines) != 0 {
		t.Fatalf("depth=%d discovery=%d deadlines=%v err=%v", depth, listed, r.deadlines, err)
	}
}
