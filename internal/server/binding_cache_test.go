package server

import (
	"context"
	"testing"
	"time"
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
