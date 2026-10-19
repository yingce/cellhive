package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatchReadyWaitsForFullBatchOrOldestTimeout(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	now := time.Unix(1000, 0)
	st.now = func() time.Time { return now }
	if _, err := st.Send(ctx, "acme", "q", []byte("one"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	check := func(size, wait int, want bool) {
		t.Helper()
		got, err := st.BatchReady(ctx, "acme", "q", size, wait)
		if err != nil || got != want {
			t.Fatalf("BatchReady(size=%d,wait=%d) = %v,%v, want %v", size, wait, got, err, want)
		}
	}
	check(2, 5, false)
	now = now.Add(4 * time.Second)
	check(2, 5, false)
	if _, err := st.Send(ctx, "acme", "q", []byte("two"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	check(2, 5, true)
	claimed, err := st.Claim(ctx, "acme", "q", 2, 1000)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim = %d,%v", len(claimed), err)
	}
	for _, msg := range claimed {
		if err := st.Ack(ctx, "acme", "q", msg.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Send(ctx, "acme", "q", []byte("three"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	check(2, 5, false)
	now = now.Add(5 * time.Second)
	// The waiting deadline is derived from persisted visible_at_ms, so a new
	// runner/store after a process restart reaches the same decision.
	restarted := New(st.cs)
	restarted.now = func() time.Time { return now }
	ready, err := restarted.BatchReady(ctx, "acme", "q", 2, 5)
	if err != nil || !ready {
		t.Fatalf("restarted batch readiness = %v,%v", ready, err)
	}
}

func TestBatchReadyDoesNotDelayRetryAndConsumerRetryDelayWins(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	now := time.Unix(2000, 0)
	st.now = func() time.Time { return now }
	if _, err := st.Send(ctx, "acme", "q", []byte("retry"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	dispatcher := &detailedDispatcher{err: errors.New("down")}
	ref := Ref{Namespace: "acme", Name: "q", Worker: "worker", BundleSHA: "sha", MaxBatchSize: 2, MaxBatchTimeoutSeconds: 5, RetryDelaySeconds: 7}
	runner := &Runner{Store: st, Dispatch: dispatcher, Queues: func(context.Context) ([]Ref, error) { return []Ref{ref}, nil }}
	if n, err := runner.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("early pass = %d,%v", n, err)
	}
	now = now.Add(5 * time.Second)
	if _, err := runner.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	items, _, err := st.PeekPage(ctx, "acme", "q", "", 10)
	if err != nil || len(items) != 1 || items[0].Attempts != 1 || items[0].VisibleAtMs != now.Add(7*time.Second).UnixMilli() {
		t.Fatalf("retry state = %+v,%v", items, err)
	}
	now = now.Add(7 * time.Second)
	ready, err := st.BatchReady(ctx, "acme", "q", 2, 5)
	if err != nil || !ready {
		t.Fatalf("retry should dispatch immediately = %v,%v", ready, err)
	}
}

func TestZeroRetryConfigUsesFiniteDefault(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	now := time.Unix(3000, 0)
	st.now = func() time.Time { return now }
	if _, err := st.Send(ctx, "acme", "q", []byte("fail"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Store: st, Dispatch: &detailedDispatcher{err: errors.New("down")}, RetryDelayMs: 1000,
		Queues: func(context.Context) ([]Ref, error) {
			return []Ref{{Namespace: "acme", Name: "q", DeadLetterQueue: "dlq"}}, nil
		}}
	for attempt := 1; attempt <= 4; attempt++ {
		if _, err := runner.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		if depth, err := st.Depth(ctx, "acme", "q"); err != nil || depth != 1 && attempt < 4 || depth != 0 && attempt == 4 {
			t.Fatalf("attempt=%d source depth=%d err=%v", attempt, depth, err)
		}
		now = now.Add(time.Second)
	}
	if depth, err := st.Depth(ctx, "acme", "dlq"); err != nil || depth != 1 {
		t.Fatalf("DLQ depth=%d err=%v", depth, err)
	}
}
