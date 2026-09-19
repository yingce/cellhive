package objectstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/bucket"
)

func TestObjectsPrefixScopeAndSafety(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	o := NewOwned(b, PrefixSupervisor, OwnerSupervisor) // suffix added
	if o.Prefix != "dosupervisor/" {
		t.Fatalf("prefix = %q", o.Prefix)
	}
	if _, err := o.Put(ctx, "dosupervisor/workerd__do__/manifest.json", []byte("m")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := o.Get(ctx, "dosupervisor/workerd__do__/manifest.json")
	if err != nil || string(got) != "m" {
		t.Fatalf("get = %q, %v", got, err)
	}
	// Out-of-prefix and traversal are rejected before touching the bucket.
	for _, bad := range []string{"cells/x", "dosupervisor/../cells/x", "/dosupervisor/x", `dosupervisor\..\x`, "bundles/x"} {
		if _, err := o.Get(ctx, bad); !errors.Is(err, ErrBadKey) {
			t.Fatalf("Get(%q) err = %v, want ErrBadKey", bad, err)
		}
		if _, err := o.Put(ctx, bad, []byte("x")); !errors.Is(err, ErrBadKey) {
			t.Fatalf("Put(%q) err = %v, want ErrBadKey", bad, err)
		}
	}
	if _, err := o.Get(ctx, "dosupervisor/missing"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("missing err = %v, want ErrNotFound", err)
	}
	keys, err := o.List(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatalf("list = %v, %v", keys, err)
	}
}

func TestReservedPrefixGuard(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// A reserved prefix cannot be opened generically.
	bad := NewObjects(b, PrefixCells)
	if _, err := bad.Get(ctx, PrefixCells+"a/b/owner.json"); !errors.Is(err, ErrBadKey) && err == nil {
		t.Fatalf("reserved prefix via NewObjects: err = %v, want refusal", err)
	}
	if _, err := bad.Put(ctx, PrefixCells+"x", []byte("x")); err == nil {
		t.Fatal("reserved prefix via NewObjects must refuse Put")
	}
	// Nor opened by the wrong owner.
	wrong := NewOwned(b, PrefixCells, OwnerArtifacts)
	if _, err := wrong.Put(ctx, PrefixCells+"x", []byte("x")); err == nil {
		t.Fatal("wrong owner must refuse")
	}
	// The real owner works.
	ok := NewOwned(b, PrefixCells, OwnerReplica)
	if _, err := ok.Put(ctx, PrefixCells+"a/b/owner.json", []byte("o")); err != nil {
		t.Fatalf("owner put: %v", err)
	}
	// Non-reserved prefixes are open.
	if _, err := NewObjects(b, "custom/").Put(ctx, "custom/k", []byte("v")); err != nil {
		t.Fatalf("non-reserved put: %v", err)
	}
}

func TestReservedPrefixesDisjointAndOwned(t *testing.T) {
	ps := ReservedPrefixes()
	if len(ps) == 0 {
		t.Fatal("no reserved prefixes")
	}
	seen := map[string]bool{}
	for _, p := range ps {
		if !strings.HasSuffix(p, "/") {
			t.Fatalf("reserved prefix %q must end with /", p)
		}
		if seen[p] {
			t.Fatalf("duplicate reserved prefix %q", p)
		}
		seen[p] = true
		if OwnerOf(p) == "" {
			t.Fatalf("reserved prefix %q has no owner", p)
		}
	}
	// No prefix may be nested under another (would make ownership ambiguous).
	for _, a := range ps {
		for _, c := range ps {
			if a != c && strings.HasPrefix(c, a) {
				t.Fatalf("reserved prefix %q is nested under %q", c, a)
			}
		}
	}
}

func TestObjectsListPage(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := NewOwned(b, "custom/", "x")
	// "x" is not a reserved prefix; NewOwned works but ListPage still enforces
	// the prefix boundary.
	for _, k := range []string{"custom/a", "custom/b", "custom/c", "custom/d", "custom/e"} {
		if _, err := st.Put(ctx, k, []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	page, next, err := st.ListPage(ctx, "custom/", "", 2)
	if err != nil || len(page) != 2 || next == "" {
		t.Fatalf("page1 = %v next=%q err=%v", page, next, err)
	}
	if page[0].Key != "custom/a" || page[1].Key != "custom/b" {
		t.Fatalf("page1 keys = %v", page)
	}
	page, next, err = st.ListPage(ctx, "custom/", next, 2)
	if err != nil || len(page) != 2 || page[0].Key != "custom/c" || page[1].Key != "custom/d" || next == "" {
		t.Fatalf("page2 = %v next=%q err=%v", page, next, err)
	}
	page, next, err = st.ListPage(ctx, "custom/", next, 2)
	if err != nil || len(page) != 1 || page[0].Key != "custom/e" || next != "" {
		t.Fatalf("page3 = %v next=%q err=%v", page, next, err)
	}
	// Prefix outside the store is refused.
	if _, _, err := st.ListPage(ctx, "other/", "", 2); err == nil {
		t.Fatal("out-of-prefix ListPage accepted")
	}
	// A reserved prefix cannot be opened generically.
	if _, _, err := NewObjects(b, PrefixCells).ListPage(ctx, PrefixCells, "", 2); err == nil {
		t.Fatal("reserved prefix ListPage accepted")
	}
}
