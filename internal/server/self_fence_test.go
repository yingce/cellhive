package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
)

func TestCapturedWriteSelfFencesWhenLeaseUnhealthy(t *testing.T) {
	s := &Server{Lease: &lease.Manager{TTL: time.Second}}
	called := false
	err := s.capturedWrite(context.Background(), cell.Scope{Namespace: "x", Class: "__kv__", ID: "y"}, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("capturedWrite err=%v, want ErrNotOwner", err)
	}
	if called {
		t.Fatal("write ran while lease was unhealthy")
	}
	if s.ackAllowed(time.Now()) {
		t.Fatal("unpublished lease considered healthy")
	}
}

// The ack must not be released if the node's lease lapsed while the durability
// proof was in flight: a peer may already be taking the cell over.
func TestAckRefusedWhenLeaseLapsesDuringProof(t *testing.T) {
	m := lease.NewManager(nil, "n", "s", "", "", 10*time.Second)
	s := &Server{Lease: m}
	if !s.ackAllowed(time.Now()) {
		t.Fatal("fresh manager should allow acks")
	}
	if s.ackAllowed(time.Now().Add(9 * time.Second)) {
		t.Fatal("acks must stop inside the margin before the published expiry")
	}
}

func TestAckRefusedWhileLegacyPeerVisible(t *testing.T) {
	m := lease.NewManager(nil, "n", "s", "", "", 10*time.Second)
	s := &Server{Lease: m}
	m.SetCachedForTest([]lease.NodeLease{{Node: "old", Expiry: time.Now().Add(time.Minute).UnixMilli()}})
	if s.ackAllowed(time.Now()) {
		t.Fatal("must not ack beside a live pre-ADR-273 peer")
	}
	m.SetCachedForTest([]lease.NodeLease{{Node: "old", Expiry: time.Now().Add(time.Minute).UnixMilli(), Liveness: lease.NodeLeaseLiveness}})
	if !s.ackAllowed(time.Now()) {
		t.Fatal("a node-lease peer must not block acks")
	}
}
