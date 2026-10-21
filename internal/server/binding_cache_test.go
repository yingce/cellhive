package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"cellhive/internal/control"
)

// TestBindingCache verifies the scoped-token binding cache stores positive and
// negative results, is dropped by control-plane invalidation, and is disabled
// when the TTL is zero.
func TestBindingCache(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.BindingCacheTTL = time.Minute
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}

	ok, err := s.bindingAllowed(ctx, "acme", "kv", "KV")
	if err != nil || !ok {
		t.Fatalf("bindingAllowed = %v, %v; want true", ok, err)
	}
	// A negative result is cached too.
	if ok, err := s.bindingAllowed(ctx, "acme", "d1", "DB"); err != nil || ok {
		t.Fatalf("missing binding = %v, %v; want false", ok, err)
	}
	s.bindMu.Lock()
	n := len(s.bindCache)
	s.bindMu.Unlock()
	if n != 2 {
		t.Fatalf("cache entries = %d, want 2 (positive + negative)", n)
	}

	s.invalidateBindings("acme")
	s.bindMu.Lock()
	n = len(s.bindCache)
	s.bindMu.Unlock()
	if n != 0 {
		t.Fatalf("cache entries after invalidate = %d, want 0", n)
	}

	// TTL 0 disables caching (always reads through).
	s.Cfg.BindingCacheTTL = 0
	if ok, err := s.bindingAllowed(ctx, "acme", "kv", "KV"); err != nil || !ok {
		t.Fatalf("bindingAllowed (no cache) = %v, %v; want true", ok, err)
	}
	s.bindMu.Lock()
	n = len(s.bindCache)
	s.bindMu.Unlock()
	if n != 0 {
		t.Fatalf("cache entries with TTL=0 = %d, want 0", n)
	}
}

func TestBindingCheckUsesControlOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ownerServer, ownerManager, ownerHTTP := newControlForwardServer(t, dir, "owner")
	followerServer, _, _ := newControlForwardServer(t, dir, "follower")
	ownerManager.Advertise = ownerHTTP.URL
	if _, err := ownerManager.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := ownerServer.Control.CreateApp(ctx, "probe", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := ownerServer.Control.CreateResource(ctx, "probe", "do", "COUNTER", "Counter", "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := followerServer.Control.Binding(ctx, "probe", "do", "COUNTER"); err != nil || ok {
		t.Fatalf("stale follower local binding = %v, %v; want absent", ok, err)
	}
	id, ok, err := followerServer.bindingFromOwner(ctx, "probe", "do", "COUNTER")
	if err != nil || !ok || id != "Counter" {
		t.Fatalf("owner binding = %q, %v, %v", id, ok, err)
	}
}

func TestRuntimeVersionBindingsReadControlOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	primary, om, primaryHTTP := newControlForwardServer(t, dir, "primary")
	follower, _, _ := newControlForwardServer(t, dir, "follower")
	om.Advertise = primaryHTTP.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha", Vars: map[string]string{"RELEASE": "new"},
	}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := follower.Control.VersionEnv(ctx, "acme", "web", 1); err != nil || ok {
		t.Fatalf("follower unexpectedly has version 1: ok=%v err=%v", ok, err)
	}
	rr := do(t, follower.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=web&version=1", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"RELEASE":"new"`) {
		t.Fatalf("runtime version lookup did not reach owner: status=%d body=%s", rr.Code, rr.Body.String())
	}
}
