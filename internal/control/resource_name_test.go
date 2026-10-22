package control

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestCreateResourceRejectsUnscopableNameAndScope covers ADR-269: a registry
// entry whose name or scope cannot address a cell is accepted by every data
// path… except that opening the cell fails afterwards, and delete/cleanup fails
// too. Creation must reject it up front instead of leaving an unusable (and
// unremovable) registration.
func TestCreateResourceRejectsUnscopableNameAndScope(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	for _, name := range []string{"vwork-74/CHTEST", `vwork-74\CHTEST`, "..", "a..b", "."} {
		if _, err := s.CreateResource(ctx, "acme", "kv", name, "acme/__kv__/main", "ops"); err == nil {
			t.Fatalf("name %q accepted", name)
		} else if !strings.Contains(err.Error(), "invalid resource name") {
			t.Fatalf("name %q error = %v", name, err)
		}
		if _, err := s.GetResource(ctx, "acme", "kv", name); !errors.Is(err, ErrNotFound) {
			t.Fatalf("name %q left a registry row: %v", name, err)
		}
	}
	for _, scope := range []string{"a/b", "acme/__kv__/x/y", "a..b", `a\b`, "..", "/"} {
		if _, err := s.CreateResource(ctx, "acme", "kv", "KV", scope, "ops"); err == nil {
			t.Fatalf("scope %q accepted", scope)
		} else if !strings.Contains(err.Error(), "invalid resource scope") {
			t.Fatalf("scope %q error = %v", scope, err)
		}
		if _, err := s.GetResource(ctx, "acme", "kv", "KV"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("scope %q left a registry row: %v", scope, err)
		}
	}

	// The two supported shapes stay accepted: a bare cell id and a full scope.
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "sessions", "ops"); err != nil {
		t.Fatalf("bare scope: %v", err)
	}
	if id, ok, err := s.Binding(ctx, "acme", "kv", "KV"); err != nil || !ok || id != "sessions" {
		t.Fatalf("bare-scope binding = %q ok=%v err=%v", id, ok, err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV2", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("full scope: %v", err)
	}
	if id, ok, err := s.Binding(ctx, "acme", "kv", "KV2"); err != nil || !ok || id != "acme/__kv__/main" {
		t.Fatalf("full-scope binding = %q ok=%v err=%v", id, ok, err)
	}
	// A cross-namespace full scope stays registrable; the data path rejects it
	// (ADR-097 boundary). Empty scopes stay registrable for config-only
	// resources and id-less declarations.
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV3", "evil/__kv__/x", "ops"); err != nil {
		t.Fatalf("cross-namespace scope: %v", err)
	}
	if _, err := s.CreateResourceWithConfig(ctx, "acme", "hyperdrive", "HYDR", "", nil, "ops"); err != nil {
		t.Fatalf("empty scope: %v", err)
	}
	// Dots inside a component (not a traversal token) remain valid names.
	if _, err := s.CreateResource(ctx, "acme", "kv", "vwork-74.CHTEST_v2", "acme/__kv__/vwork-74.CHTEST_v2", "ops"); err != nil {
		t.Fatalf("dotted name: %v", err)
	}
}

// TestDeleteResourceRevokesMalformedLegacyRegistration covers recovery for rows
// created before ADR-269 validation: the stored scope cannot address a cell, so
// there is nothing to snapshot/clean. Revocation must still remove the registry
// entry and tombstone it, without inventing a cleanup job (delete used to fail
// with "invalid cell scope: component contains separator" forever).
func TestDeleteResourceRevokesMalformedLegacyRegistration(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatalf("control cell: %v", err)
	}
	const badScope = "acme/__kv__/vwork-74/CHTEST"
	if _, err := c.DB.ExecContext(ctx,
		`INSERT INTO resources(ns,kind,name,scope,created_ms) VALUES('acme','kv','vwork-74/CHTEST',?,1)`,
		badScope); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "vwork-74/CHTEST", "ops"); err != nil {
		t.Fatalf("revoke malformed legacy registration: %v", err)
	}
	if _, err := s.GetResource(ctx, "acme", "kv", "vwork-74/CHTEST"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("registry row survived revocation: %v", err)
	}
	if _, err := s.ResourceCleanup(ctx, "acme", "kv", "vwork-74/CHTEST"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("malformed registration invented a cleanup job: %v", err)
	}
	if _, ok, err := s.Binding(ctx, "acme", "kv", "vwork-74/CHTEST"); err != nil || ok {
		t.Fatalf("tombstone did not fail closed: ok=%v err=%v", ok, err)
	}
}
