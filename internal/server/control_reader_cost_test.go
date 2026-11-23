package server

import (
	"errors"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/owner"
)

func TestBackgroundControlLocalAuthorityReadBudget(t *testing.T) {
	s, om, _ := newControlForwardServer(t, t.TempDir(), "owner")
	t.Cleanup(func() { _ = s.Store.Close() })
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	count := &fenceCountingBucket{Bucket: om.B}
	om.B = count
	if _, err := s.BackgroundControl().ProjectionStrict(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := count.gets.Load(); n != 2 {
		t.Fatalf("owner GETs=%d, want one before + one after", n)
	}
	count.gets.Store(0)
	if _, identity, err := s.BackgroundControl().ResourcesByKindWithAuthority(t.Context(), "queue"); err != nil || identity == "" {
		t.Fatalf("identity=%q err=%v", identity, err)
	}
	if n := count.gets.Load(); n != 2 {
		t.Fatalf("resource owner GETs=%d", n)
	}
}
func TestBackgroundControlLocalRejectsOwnerChangeAfterRead(t *testing.T) {
	s, om, _ := newControlForwardServer(t, t.TempDir(), "owner")
	t.Cleanup(func() { _ = s.Store.Close() })
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	om.B = &cleanupValidationOwnerChangeBucket{Bucket: om.B}
	if _, err := s.BackgroundControl().ProjectionStrict(t.Context()); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("accepted changed epoch: %v", err)
	}
}
func TestBackgroundControlRemoteRetainsBothReadBoundaries(t *testing.T) {
	dir := t.TempDir()
	a, oa, ha := newControlForwardServer(t, dir, "owner")
	b, ob, _ := newControlForwardServer(t, dir, "follower")
	t.Cleanup(func() { _ = a.Store.Close(); _ = b.Store.Close() })
	oa.Advertise = ha.URL
	if _, err := oa.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	ca := &fenceCountingBucket{Bucket: oa.B}
	cb := &fenceCountingBucket{Bucket: ob.B}
	oa.B, ob.B = ca, cb
	if _, err := b.BackgroundControl().ProjectionStrict(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ca.gets.Load() != 2 || cb.gets.Load() != 2 {
		t.Fatalf("owner=%d caller=%d, want 2 each", ca.gets.Load(), cb.gets.Load())
	}
}
