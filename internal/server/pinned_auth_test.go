package server

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/scopedtoken"
)

func pinnedAuthFixture(t *testing.T) (*Server, scopedtoken.Claims) {
	t.Helper()
	s := newScopeServer(t)
	s.Cfg.BindingCacheTTL = time.Second
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: "sha", Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: "data"}}}, "test"); err != nil {
		t.Fatal(err)
	}
	return s, scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "KV", Worker: "web", Version: 1, Resource: "data"}
}

func TestPinnedAuthorizationRevocation(t *testing.T) {
	for _, kind := range []string{"worker", "app", "resource"} {
		t.Run(kind, func(t *testing.T) {
			s, p := pinnedAuthFixture(t)
			if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
				t.Fatalf("initial %v %v", ok, err)
			}
			var err error
			switch kind {
			case "worker":
				err = s.Control.DeleteWorker(t.Context(), "acme", "web", "test")
			case "app":
				err = s.Control.DeleteApp(t.Context(), "acme", "test")
			case "resource":
				err = s.Control.DeleteResource(t.Context(), "acme", "kv", "KV", "test")
			}
			if err != nil {
				t.Fatal(err)
			}
			// A background renewal must cache the denial, without requiring a local invalidation event.
			if ok, err := s.pinnedBindingAllowed(t.Context(), p, true); err != nil || ok {
				t.Fatalf("revocation %v %v", ok, err)
			}
			if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || ok {
				t.Fatalf("cached denial %v %v", ok, err)
			}
		})
	}
}

func TestPinnedAuthorizationFailureNeverExtendsLease(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	key := pinnedAuthKey(p)
	s.bindMu.Lock()
	before := s.bindCache[key]
	s.bindMu.Unlock()
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: "absent", Session: "absent"}
	if _, err := s.pinnedBindingAllowed(t.Context(), p, true); err == nil {
		t.Fatal("expected owner lookup failure")
	}
	s.bindMu.Lock()
	after := s.bindCache[key]
	if !after.exp.Equal(before.exp) {
		t.Error("failed renewal extended authorization")
	}
	after.exp = time.Now().Add(-time.Second)
	s.bindCache[key] = after
	s.bindMu.Unlock()
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err == nil || ok {
		t.Fatalf("expired authorization allowed: %v %v", ok, err)
	}
}

func TestPinnedWarmRetentionBound(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	s.notePinnedAuth(p)
	s.hotKVAccessMu.Lock()
	e := s.hotKVAccesses[pinnedAuthKey(p)]
	e.lastAccess = time.Now().Add(-3 * time.Second)
	e.next = time.Now().Add(time.Hour)
	s.hotKVAccessMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { s.runKVReadWarmLoop(ctx); close(done) }()
	time.Sleep(150 * time.Millisecond)
	s.hotKVAccessMu.Lock()
	_, retained := s.hotKVAccesses[pinnedAuthKey(p)]
	e.lastAccess = time.Now().Add(-61 * time.Second)
	s.hotKVAccessMu.Unlock()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done
	s.hotKVAccessMu.Lock()
	_, expired := s.hotKVAccesses[pinnedAuthKey(p)]
	s.hotKVAccessMu.Unlock()
	if !retained || expired {
		t.Fatalf("retention retained=%v expired=%v", retained, expired)
	}
}

func TestPinnedAuthorizationBackgroundRenewal(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	s.notePinnedAuth(p)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { s.runKVReadWarmLoop(ctx); close(done) }()
	// Past both the authorization TTL and the old two-second hot-set lifetime.
	time.Sleep(2200 * time.Millisecond)
	cancel()
	<-done
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: "absent", Session: "absent"}
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatalf("warm authorization should need no owner read: %v %v", ok, err)
	}
}

func TestPinnedAuthorizationUsesExactVersion(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: "new"}, "test"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, true); err != nil || !ok {
		t.Fatalf("older immutable version lost authorization: %v %v", ok, err)
	}
	for _, change := range []func(*scopedtoken.Claims){func(p *scopedtoken.Claims) { p.Version = 999 }, func(p *scopedtoken.Claims) { p.Worker = "other" }, func(p *scopedtoken.Claims) { p.Resource = "other" }} {
		other := p
		change(&other)
		if ok, err := s.pinnedBindingAllowed(t.Context(), other, true); err != nil || ok {
			t.Fatalf("wrong fixed identity: %v %v", ok, err)
		}
	}
}
