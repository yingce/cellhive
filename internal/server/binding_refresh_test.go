package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

type bindingCountBucket struct {
	bucket.Bucket
	reads   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (b *bindingCountBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if key == owner.Key(control.Scope()) {
		if b.reads.Add(1) == 1 {
			close(b.entered)
		}
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return b.Bucket.Get(ctx, key)
}

func TestBindingRefreshCoalescesAndWaiterCancellation(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.BindingCacheTTL = time.Second
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: s.Cfg.NodeID, Session: "coalesce", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "kv", "KV", "main", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Owner.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &bindingCountBucket{Bucket: s.Bucket, entered: make(chan struct{}), release: make(chan struct{})}
	s.Owner.B = b
	done := make(chan error, 9)
	go func() { _, _, err := s.refreshBindingInfo(t.Context(), "acme", "kv", "KV"); done <- err }()
	<-b.entered
	for i := 0; i < 8; i++ {
		go func() { _, _, err := s.refreshBindingInfo(t.Context(), "acme", "kv", "KV"); done <- err }()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := s.refreshBindingInfo(ctx, "acme", "kv", "KV"); err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	// Give concurrent callers time to join while the bucket read is blocked.
	time.Sleep(30 * time.Millisecond)
	close(b.release)
	for i := 0; i < 9; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// Route resolution plus authoritative local check, shared by all callers.
	if n := b.reads.Load(); n > 2 {
		t.Fatalf("duplicate refresh reads: %d", n)
	}
}

func TestLocalBindingRejectsReleasedOwner(t *testing.T) {
	s := newScopeServer(t)
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: s.Cfg.NodeID, Session: "release", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	o, err := s.Owner.Claim(t.Context(), control.Scope(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Owner.ResolveRoute(t.Context(), control.Scope()); err != nil {
		t.Fatal(err)
	}
	if err = s.Owner.Release(t.Context(), control.Scope(), o.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.localBinding(t.Context(), "acme", "kv", "KV"); err == nil {
		t.Fatal("released owner served local binding")
	}
}

func TestBindingColdReadClaimsControlWithoutBackgroundTick(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "cold")
	// An empty authoritative control cell has no binding. It must return a
	// definitive absence after claiming, rather than waiting for a timer loop.
	_, ok, err := s.bindingFromOwner(t.Context(), "absent", "kv", "KV")
	if err != nil || ok {
		t.Fatalf("cold authorization: allowed=%v err=%v", ok, err)
	}
	o, _, err := s.Owner.Resolve(t.Context(), control.Scope())
	if err != nil || o.Node != s.Cfg.NodeID || o.Session != s.Owner.Session {
		t.Fatalf("control not claimed: %+v %v", o, err)
	}
}

func TestBindingCachedRouteFollowsControlHandoff(t *testing.T) {
	dir := t.TempDir()
	old, om, oh := newControlForwardServer(t, dir, "old")
	next, nm, nh := newControlForwardServer(t, dir, "next")
	reader, _, _ := newControlForwardServer(t, dir, "reader")
	om.Advertise = oh.URL
	nm.Advertise = nh.URL
	for _, s := range []*Server{old, next} {
		if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := old.Control.CreateResource(t.Context(), "acme", "kv", "KV", "old-resource", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Control.CreateResource(t.Context(), "acme", "kv", "KV", "new-resource", "test"); err != nil {
		t.Fatal(err)
	}
	o, err := om.Claim(t.Context(), control.Scope(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if id, ok, err := reader.bindingFromOwner(t.Context(), "acme", "kv", "KV"); err != nil || !ok || id != "old-resource" {
		t.Fatalf("before: %s %v %v", id, ok, err)
	}
	if err := om.Release(t.Context(), control.Scope(), o.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := nm.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := reader.bindingFromOwner(t.Context(), "acme", "kv", "KV"); err != nil || !ok || id != "new-resource" {
		t.Fatalf("after cached route handoff: %s %v %v", id, ok, err)
	}
}
