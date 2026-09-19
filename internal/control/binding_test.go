package control

import (
	"context"
	"testing"
)

// TestBindingResolvesResourceScope verifies a binding declaration resolves to
// its resource scope (the resource id used to name the backing cell).
func TestBindingResolvesResourceScope(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "sessions", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	id, ok, err := s.Binding(ctx, "acme", "kv", "KV")
	if err != nil || !ok || id != "sessions" {
		t.Fatalf("Binding = %q ok=%v err=%v, want sessions", id, ok, err)
	}
	if _, ok, _ := s.Binding(ctx, "acme", "kv", "NOPE"); ok {
		t.Fatalf("unknown binding reported as declared")
	}
	// A full cell scope string round-trips as declared.
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV2", "acme/__kv__/main", "test"); err != nil {
		t.Fatalf("resource 2: %v", err)
	}
	if id, ok, _ := s.Binding(ctx, "acme", "kv", "KV2"); !ok || id != "acme/__kv__/main" {
		t.Fatalf("Binding KV2 = %q ok=%v, want acme/__kv__/main", id, ok)
	}
}

// TestBindingFromWorkerVersion verifies a binding declared by a deployed worker
// version resolves to the binding id (and reports a missing id as declared
// without one).
func TestBindingFromWorkerVersion(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{
		BundleSHA: "deadbeef",
		Bindings:  []Binding{{Type: "kv", Name: "SESSIONS", ID: "nskv123"}, {Type: "kv", Name: "NOID"}},
	}, "test"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if id, ok, err := s.Binding(ctx, "acme", "kv", "SESSIONS"); err != nil || !ok || id != "nskv123" {
		t.Fatalf("Binding SESSIONS = %q ok=%v err=%v, want nskv123", id, ok, err)
	}
	if id, ok, err := s.Binding(ctx, "acme", "kv", "NOID"); err != nil || !ok || id != "" {
		t.Fatalf("Binding NOID = %q ok=%v err=%v, want declared with empty id", id, ok, err)
	}
}
