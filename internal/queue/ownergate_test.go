package queue_test

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
)

// TestOwnerGate covers ADR-119: exactly one node (the cell owner) consumes a
// queue, and consumption moves to a peer after the owner releases the cell.
func TestOwnerGate(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	omA := &owner.Manager{B: b, NodeID: "node-1", Session: "s1", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	omB := &owner.Manager{B: b, NodeID: "node-2", Session: "s2", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	gateA, gateB := queue.OwnerGate(omA), queue.OwnerGate(omB)
	ref := queue.Ref{Namespace: "app", Name: "jobs"}

	// Unowned: A claims and consumes; B skips (A holds a live lease).
	if ok, err := gateA(ctx, ref); err != nil || !ok {
		t.Fatalf("A gate on unowned = %v, %v; want claim", ok, err)
	}
	if ok, err := gateB(ctx, ref); err != nil || ok {
		t.Fatalf("B gate while A owns = %v, %v; want skip", ok, err)
	}

	// A releases: B claims and consumes.
	sc := queue.Scope("app", "jobs")
	o, _, err := omA.Resolve(ctx, sc)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := omA.Release(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release: %v", err)
	}
	// B learns about the release when its owner cache (1s) expires.
	time.Sleep(1100 * time.Millisecond)
	if ok, err := gateB(ctx, ref); err != nil || !ok {
		t.Fatalf("B gate after release = %v, %v; want claim", ok, err)
	}
	if ok, err := gateA(ctx, ref); err != nil || ok {
		t.Fatalf("A gate after B claimed = %v, %v; want skip", ok, err)
	}
}
