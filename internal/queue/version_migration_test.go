package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type versionDispatch struct {
	seen    chan Ref
	release chan struct{}
	msgs    []Message
}

func (d *versionDispatch) Dispatch(_ context.Context, ref Ref, msgs []Message) error {
	d.msgs = append([]Message(nil), msgs...)
	d.seen <- ref
	if d.release != nil {
		<-d.release
	}
	return nil
}

// A rollout does not replace the handler already dispatching a claimed batch.
// A crash after claim has no persisted Worker pin: the lease-expired attempt is
// a new delivery against the current active version. This is the explicit
// Queue migration boundary in ADR-216, not Workflow-style instance pinning.
func TestRunnerVersionMigrationAtRedeliveryBoundary(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	now := time.Unix(1_800_000_000, 0)
	store.now = func() time.Time { return now }
	var active atomic.Int32
	active.Store(1)
	refs := func(context.Context) ([]Ref, error) {
		v := int(active.Load())
		sha := "sha1"
		if v == 2 {
			sha = "sha2"
		}
		return []Ref{{Namespace: "acme", Name: "jobs", Worker: "consumer", BundleSHA: sha, Version: v}}, nil
	}
	if _, err := store.Send(ctx, "acme", "jobs", []byte("first"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	first := &versionDispatch{seen: make(chan Ref, 1), release: make(chan struct{})}
	runner := &Runner{Store: store, Queues: refs, Dispatch: first, LeaseMs: 1_000}
	done := make(chan int, 1)
	go func() { count, _ := runner.Pass(ctx); done <- count }()
	select {
	case ref := <-first.seen:
		if ref.Version != 1 {
			t.Fatalf("claimed handler version = %d, want 1", ref.Version)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first dispatch did not start")
	}
	active.Store(2)
	close(first.release)
	if count := <-done; count != 1 {
		t.Fatalf("first delivery acked %d messages", count)
	}
	if first.msgs[0].Attempts != 1 {
		t.Fatalf("first attempts = %d", first.msgs[0].Attempts)
	}

	if _, err := store.Send(ctx, "acme", "jobs", []byte("crash"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	// Simulate cell-agent death after the durable claim, before dispatch/ack.
	claimed, err := store.Claim(ctx, "acme", "jobs", 1, 1_000)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("pre-crash claim = %+v, %v", claimed, err)
	}
	if depth, err := store.Depth(ctx, "acme", "jobs"); err != nil || depth != 1 {
		t.Fatalf("claimed message not durable: depth=%d err=%v", depth, err)
	}
	// A new Store mimics a restarted process using the same durable queue cell.
	now = now.Add(1100 * time.Millisecond)
	restarted := New(store.cs)
	restarted.now = func() time.Time { return now }
	second := &versionDispatch{seen: make(chan Ref, 1)}
	newRunner := &Runner{Store: restarted, Queues: refs, Dispatch: second, LeaseMs: 1_000}
	count, err := newRunner.Pass(ctx)
	if err != nil || count != 1 {
		t.Fatalf("redelivery count=%d err=%v", count, err)
	}
	ref := <-second.seen
	if ref.Version != 2 || ref.BundleSHA != "sha2" || len(second.msgs) != 1 || second.msgs[0].ID != claimed[0].ID || second.msgs[0].Attempts != 2 {
		t.Fatalf("redelivery ref=%+v messages=%+v", ref, second.msgs)
	}
	if depth, err := restarted.Depth(ctx, "acme", "jobs"); err != nil || depth != 0 {
		t.Fatalf("redelivery was not acked: depth=%d err=%v", depth, err)
	}
}
