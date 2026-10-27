package server

import (
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
)

func TestLocalBindingReusesHealthyOwnerEvidence(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	lm := lease.NewManager(s.Bucket, s.Cfg.NodeID, "local-read", "", "", 10*time.Second)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: s.Cfg.NodeID, Session: "local-read", Role: cell.RoleCellAgent, OwnerTTL: 10 * time.Second, LeaseView: lm}
	if _, err := s.Owner.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	close(release)
	b := &bindingCountBucket{Bucket: s.Bucket, entered: make(chan struct{}), release: release}
	s.Owner.B = b
	for i := 0; i < 10; i++ {
		if ok, err := s.pinnedBindingAllowed(t.Context(), p, true); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if n := b.reads.Load(); n != 1 {
		t.Fatalf("10 authorization refreshes did %d owner reads; want initial 1", n)
	}
	s.Owner.Invalidate(control.Scope())
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, true); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if n := b.reads.Load(); n != 2 {
		t.Fatalf("invalidation did not force fresh proof: %d", n)
	}
}
