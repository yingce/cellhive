package control

import (
	"strings"
	"testing"

	"cellhive/internal/cellstore"
)

func TestBundleGrantPersistenceIsolationAndAudit(t *testing.T) {
	dir := t.TempDir()
	cs, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	s := New(cs, nil)
	ctx := WithActor(t.Context(), Actor{Sub: "uploader", Kind: "namespace"})
	sha := strings.Repeat("a", 64)
	if ok, err := s.HasBundleGrant(ctx, "a", sha); err != nil || ok {
		t.Fatalf("missing grant: %v %v", ok, err)
	}
	for range 2 {
		if err := s.GrantBundle(ctx, "a", sha, "fallback"); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.HasBundleGrant(ctx, "b", sha); err != nil || ok {
		t.Fatalf("foreign grant: %v %v", ok, err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM bundle_grants`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("grant dedup: %d %v", count, err)
	}
	var actor, ns, target string
	if err := c.DB.QueryRowContext(ctx, `SELECT actor,ns,target FROM audit WHERE action='bundle.grant' LIMIT 1`).Scan(&actor, &ns, &target); err != nil {
		t.Fatal(err)
	}
	if actor != "uploader" || ns != "a" || target != sha {
		t.Fatalf("audit: %s %s %s", actor, ns, target)
	}
	if _, err := c.DB.ExecContext(ctx, `CREATE TRIGGER fail_bundle_audit BEFORE INSERT ON audit BEGIN SELECT RAISE(ABORT,'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.GrantBundle(ctx, "b", sha, "fallback"); err == nil {
		t.Fatal("grant ignored audit failure")
	}
	if ok, err := s.HasBundleGrant(ctx, "b", sha); err != nil || ok {
		t.Fatalf("failed grant persisted: %v %v", ok, err)
	}
	cs.Close()
	cs, err = cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = New(cs, nil)
	if ok, err := s.HasBundleGrant(ctx, "a", sha); err != nil || !ok {
		t.Fatalf("reopened grant: %v %v", ok, err)
	}
}

func TestBundleGrantRejectsInvalidIdentity(t *testing.T) {
	s := newStore(t, false)
	t.Cleanup(func() { s.cs.Close() })
	sha := strings.Repeat("a", 64)
	for _, in := range []struct{ ns, sha string }{{"", sha}, {"__platform__", sha}, {"a/b", sha}, {"a", ""}, {"a", strings.Repeat("A", 64)}, {"a", strings.Repeat("g", 64)}, {"a", strings.Repeat("a", 63)}, {"a", strings.Repeat("a", 65)}} {
		if err := s.GrantBundle(t.Context(), in.ns, in.sha, "test"); err == nil {
			t.Errorf("accepted grant %+v", in)
		}
		if ok, err := s.HasBundleGrant(t.Context(), in.ns, in.sha); err == nil || ok {
			t.Errorf("accepted lookup %+v: %v %v", in, ok, err)
		}
	}
}
