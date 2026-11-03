package control

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

func TestIssuerMetadataBudget(t *testing.T) {
	s := newStore(t, false)
	t.Cleanup(func() { s.cs.Close() })
	patterns := func(n, size int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = strings.Repeat("a", size)
		}
		return out
	}
	for _, tc := range []struct {
		name string
		ns   []string
	}{
		{strings.Repeat("a", 257), []string{"acme"}},
		{strings.Repeat("界", 86), []string{"acme"}},
		{"too-many", patterns(65, 1)},
		{"too-long", patterns(1, 129)},
		{"total-budget", patterns(64, 128)},
	} {
		if _, err := s.CreateIssuer(t.Context(), tc.name, tc.ns, 60, "test"); !errors.Is(err, ErrInvalidIssuer) {
			t.Errorf("invalid metadata accepted: name bytes=%d patterns=%d err=%v", len(tc.name), len(tc.ns), err)
		}
	}
	for _, tc := range []struct {
		name string
		ns   []string
	}{
		{strings.Repeat("a", 256), []string{"acme"}},
		{strings.Repeat("界", 85), []string{"acme"}},
		{"max-patterns", patterns(64, 1)},
		{"max-pattern-bytes", patterns(1, 128)},
	} {
		if _, err := s.CreateIssuer(t.Context(), tc.name, tc.ns, 86400, "test"); err != nil {
			t.Fatalf("valid boundary rejected: %v", err)
		}
	}
	for _, ttl := range []int64{0, 86401} {
		if _, err := s.CreateIssuer(t.Context(), "invalid-ttl", []string{"acme"}, ttl, "test"); !errors.Is(err, ErrInvalidIssuer) {
			t.Fatalf("TTL validation sentinel: %v", err)
		}
	}
}

func TestIssuerLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	t.Cleanup(func() { s.cs.Close() })
	now := int64(1000)
	s.now = func() time.Time { return time.UnixMilli(now) }
	i, err := s.CreateIssuer(ctx, "vwork", []string{"vwork-*", "demo"}, 3600, "ops")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32,}$`).MatchString(i.ID) || !i.Enabled || i.CreatedMs != 1000 || i.UpdatedMs != 1000 || i.DeletedMs != 0 || i.MaxTokenTTLSeconds != 3600 {
		t.Fatalf("issuer = %+v", i)
	}
	got, err := s.GetIssuer(ctx, i.ID)
	if err != nil || !reflect.DeepEqual(got, i) {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := s.CreateIssuer(ctx, "vwork", []string{"*"}, 1, "ops"); !errors.Is(err, ErrIssuerExists) {
		t.Fatalf("duplicate = %v", err)
	}
	now = 2000
	if err := s.SetIssuerEnabled(ctx, i.ID, false, "ops"); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetIssuer(ctx, i.ID)
	if err != nil || got.Enabled || got.UpdatedMs != 2000 || got.CreatedMs != 1000 {
		t.Fatalf("disabled = %+v, %v", got, err)
	}
	if _, err := s.CreateIssuer(ctx, "vwork", []string{"*"}, 1, "ops"); !errors.Is(err, ErrIssuerExists) {
		t.Fatalf("disabled duplicate = %v", err)
	}
	list, err := s.ListIssuers(ctx)
	if err != nil || len(list) != 1 || list[0].Enabled {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if err := s.SetIssuerEnabled(ctx, i.ID, true, "ops"); err != nil {
		t.Fatal(err)
	}
	now = 3000
	if err := s.DeleteIssuer(ctx, i.ID, "ops"); err != nil {
		t.Fatal(err)
	}
	now = 4000
	if err := s.DeleteIssuer(ctx, i.ID, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetIssuerEnabled(ctx, i.ID, true, "ops"); !errors.Is(err, ErrIssuerDeleted) {
		t.Fatalf("resurrect = %v", err)
	}
	got, err = s.GetIssuer(ctx, i.ID)
	if err != nil || got.Enabled || got.DeletedMs != 3000 || got.UpdatedMs != 3000 {
		t.Fatalf("tombstone = %+v, %v", got, err)
	}
	fresh, err := s.CreateIssuer(ctx, "vwork", []string{"*"}, 86400, "ops")
	if err != nil || fresh.ID == i.ID {
		t.Fatalf("recreate = %+v, %v", fresh, err)
	}
	list, err = s.ListIssuers(ctx)
	if err != nil || len(list) != 1 || list[0].ID != fresh.ID {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := s.GetIssuer(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing get = %v", err)
	}
	if err := s.SetIssuerEnabled(ctx, "missing", true, "ops"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing set = %v", err)
	}
	if err := s.DeleteIssuer(ctx, "missing", "ops"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing delete = %v", err)
	}
}

func TestIssuerValidation(t *testing.T) {
	s := newStore(t, false)
	t.Cleanup(func() { s.cs.Close() })
	ctx := context.Background()
	for _, ns := range []string{"caf\u00e9", "a:b", "a@b", "a?b", "a+b", "a%b", "a[b]", "\u732b*"} {
		if _, err := s.CreateIssuer(ctx, ns, []string{ns}, 60, "ops"); err == nil {
			t.Errorf("accepted non-ASCII namespace grammar %q", ns)
		}
	}
	for _, ns := range []string{"", "__platform__", "__other*", "a/b", `a\b`, "a b", "a\t", "a\n", "a\x00", "a\x7f", "a\u00a0b", "a*b", "**", "a**", ".", "..", "a..b", string([]byte{0xff})} {
		t.Run(ns, func(t *testing.T) {
			if _, err := s.CreateIssuer(ctx, "bad", []string{ns}, 60, "ops"); err == nil {
				t.Fatalf("accepted %q", ns)
			}
		})
	}
	for _, ttl := range []int64{-1, 0, 86401} {
		if _, err := s.CreateIssuer(ctx, "bad", []string{"demo"}, ttl, "ops"); err == nil {
			t.Fatalf("accepted TTL %d", ttl)
		}
	}
	if _, err := s.CreateIssuer(ctx, "bad", nil, 60, "ops"); err == nil {
		t.Fatal("accepted empty namespaces")
	}
	for _, name := range []string{"", "   ", "bad\nname", string([]byte{0xff})} {
		if _, err := s.CreateIssuer(ctx, name, []string{"demo"}, 60, "ops"); err == nil {
			t.Fatalf("accepted name %q", name)
		}
	}
	if rev, err := s.Rev(ctx); err != nil || rev != 0 {
		t.Fatalf("invalid writes advanced revision: %d, %v", rev, err)
	}
	for _, ns := range []string{"demo", "vwork-*", "*"} {
		if _, err := s.CreateIssuer(ctx, ns, []string{ns}, 1, "ops"); err != nil {
			t.Fatalf("valid %q: %v", ns, err)
		}
	}
}

func TestIssuerPersistenceAndAuditAtomicity(t *testing.T) {
	ctx := WithActor(context.Background(), Actor{Sub: "admin", Kind: "user", RequestID: "request"})
	dir := t.TempDir()
	cs, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	s := New(cs, nil)
	i, err := s.CreateIssuer(ctx, "persisted", []string{"demo"}, 60, "fallback")
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ns, actor, kind, req, action, target string
	if err := c.DB.QueryRowContext(ctx, `SELECT ns,actor,actor_kind,request_id,action,target FROM audit`).Scan(&ns, &actor, &kind, &req, &action, &target); err != nil {
		t.Fatal(err)
	}
	if ns != GlobalNamespace || actor != "admin" || kind != "user" || req != "request" || action != "issuer.create" || target != i.ID {
		t.Fatalf("audit = %q %q %q %q %q %q", ns, actor, kind, req, action, target)
	}
	if _, err := c.DB.ExecContext(ctx, `CREATE TRIGGER fail_issuer_audit BEFORE INSERT ON audit BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateIssuer(ctx, "rolled-back", []string{"*"}, 60, "ops"); err == nil {
		t.Fatal("create ignored audit failure")
	}
	if err := s.SetIssuerEnabled(ctx, i.ID, false, "ops"); err == nil {
		t.Fatal("disable ignored audit failure")
	}
	if err := s.DeleteIssuer(ctx, i.ID, "ops"); err == nil {
		t.Fatal("delete ignored audit failure")
	}
	got, err := s.GetIssuer(ctx, i.ID)
	if err != nil || !reflect.DeepEqual(got, i) {
		t.Fatalf("rollback = %+v, %v", got, err)
	}
	if rev, err := s.Rev(ctx); err != nil || rev != 1 {
		t.Fatalf("revision = %d, %v", rev, err)
	}
	if _, err := c.DB.ExecContext(ctx, `DROP TRIGGER fail_issuer_audit`); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	cs, err = cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = New(cs, nil)
	got, err = s.GetIssuer(ctx, i.ID)
	if err != nil || !reflect.DeepEqual(got, i) {
		t.Fatalf("reopen = %+v, %v", got, err)
	}
	list, err := s.ListIssuers(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("rollback list = %+v, %v", list, err)
	}
}
