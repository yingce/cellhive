package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
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
				claimSeededScope(t, s, control.Scope())
				err = s.Control.DeleteResource(t.Context(), "acme", "kv", "KV", "test")
			}
			if err != nil {
				t.Fatal(err)
			}
			// An explicit refresh must cache the denial, without requiring a local invalidation event.
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

func TestPinnedAuthorizationDemandCache(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	claimSeededScope(t, s, control.Scope())
	b := &bindingCountBucket{Bucket: s.Bucket, entered: make(chan struct{}), release: make(chan struct{})}
	close(b.release)
	s.Owner.B = b
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	initial := b.reads.Load()
	if initial == 0 {
		t.Fatal("fixture did not read owner")
	}
	for i := 0; i < 100; i++ {
		p.ExpiresMs = time.Now().Add(time.Duration(i+1) * time.Minute).UnixMilli()
		if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if b.reads.Load() != initial {
		t.Fatalf("equivalent token claims repeated owner reads: %d -> %d", initial, b.reads.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if ok, err := s.pinnedBindingAllowed(ctx, p, false); err != context.Canceled || ok {
		t.Fatal("cancelled request accepted cached authorization", ok, err)
	}
	// Expire the entry without sleeping; next demand must query authority.
	s.bindMu.Lock()
	e := s.bindCache[pinnedAuthKey(p)]
	e.exp = time.Now().Add(-time.Second)
	s.bindCache[pinnedAuthKey(p)] = e
	s.bindMu.Unlock()
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if b.reads.Load() <= initial {
		t.Fatal("expired entry reused")
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

func TestPinnedAuthorizationSharedCacheDoesNotExtendTokenLifetime(t *testing.T) {
	s, p := pinnedAuthFixture(t)
	if ok, err := s.pinnedBindingAllowed(t.Context(), p, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Same declaration, but a different already-expired credential. A warm
	// authorization entry must not bypass authentication on the HTTP path.
	s.Cfg.RootKey = testRootKey()
	key, err := s.jwtKey("token/scope")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	token, err := bearertoken.Mint(key, "token/scope", bearertoken.Claims{Ver: 1, Audience: "cellhive", Subject: "test", Type: "binding", Namespace: p.Namespace, Kind: p.Kind, Name: p.Name, Worker: p.Worker, Version: p.Version, Resource: p.Resource, IssuedAt: now - 120, ExpiresAt: now - 60})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/v1/kv/get?ns=acme&key=k", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != 403 || !strings.Contains(rr.Body.String(), "scope_invalid") {
		t.Fatalf("expired token admitted with cached declaration: %d %s", rr.Code, rr.Body.String())
	}
}
