package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
)

func TestOwnerGateResetFailureRemainsUnowned(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "a", Session: "s", OwnerTTL: time.Minute}
	calls := 0
	gate := queue.OwnerGateWithReset(om, func(context.Context, cell.Scope) error { calls++; return errors.New("disk reset failed") })
	ref := queue.Ref{Namespace: "app", Name: "jobs"}
	if ok, err := gate(ctx, ref); ok || err == nil {
		t.Fatal("first call must fail")
	}
	if ok, err := gate(ctx, ref); ok || err == nil {
		t.Fatalf("second call admitted without reset: ok=%v err=%v reset_calls=%d", ok, err, calls)
	}
}

func TestResetRunsBeforeOwnerPublication(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "a", Session: "s", OwnerTTL: time.Minute}
	ref := queue.Ref{Namespace: "app", Name: "jobs"}
	gate := queue.OwnerGateWithReset(om, func(context.Context, cell.Scope) error {
		if o, _, err := om.Resolve(ctx, queue.Scope(ref.Namespace, ref.Name)); !errors.Is(err, owner.ErrUnowned) {
			t.Fatalf("owner published before reset: %+v %v", o, err)
		}
		return nil
	})
	if ok, err := gate(ctx, ref); !ok || err != nil {
		t.Fatalf("gate: %v %v", ok, err)
	}
}
