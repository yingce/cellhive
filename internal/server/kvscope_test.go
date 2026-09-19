package server

import (
	"context"
	"net/http"
	"testing"

	"cellhive/internal/cell"
)

// TestKVScopeUsesBindingID covers ADR-097: a KV binding has a required id (the
// resource scope or the worker binding id) that names its cell. There is no
// "default" fallback, and a declared scope must stay inside the app namespace.
func TestKVScopeUsesBindingID(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)

	// Bare id -> ns/__kv__/sessions.
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "sessions", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", tok, []byte("v")); code != http.StatusOK {
		t.Fatalf("put = %d, want 200", code)
	}
	c, err := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "sessions"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if v, _, err := c.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("value at sessions = %q err=%v, want v", v, err)
	}
	if _, err := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}); err == nil {
		// The default cell may exist only if it was opened; assert it holds no key.
		if c0, e := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}); e == nil {
			if _, _, ge := c0.Get(ctx, "k"); ge == nil {
				t.Fatalf("write landed in the default cell")
			}
		}
	}

	// Full cell scope string -> that exact cell.
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV2", "acme/__kv__/other", "test"); err != nil {
		t.Fatalf("resource 2: %v", err)
	}
	tok2 := mint(t, s, "acme", "kv", "KV2")
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", tok2, []byte("v")); code != http.StatusOK {
		t.Fatalf("put KV2 = %d, want 200", code)
	}
	if _, err := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "other"}); err != nil {
		t.Fatalf("cell other: %v", err)
	}

	// A cross-namespace scope is rejected (the app namespace is the boundary).
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV3", "evil/__kv__/x", "test"); err != nil {
		t.Fatalf("resource 3: %v", err)
	}
	tok3 := mint(t, s, "acme", "kv", "KV3")
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", tok3, []byte("v")); code != http.StatusBadRequest {
		t.Fatalf("cross-ns put = %d, want 400", code)
	}
}
