package bucket

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestConditionalWritesAndCAS(t *testing.T) {
	ctx := context.Background()
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}

	e1, err := b.ConditionalCreate(ctx, "k", []byte("one"))
	if err != nil {
		t.Fatalf("conditional create: %v", err)
	}
	if _, err := b.ConditionalCreate(ctx, "k", []byte("two")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("reject-create: got %v, want ErrPrecondition", err)
	}
	if _, err := b.CAS(ctx, "k", []byte("three"), e1); err != nil {
		t.Fatalf("cas update: %v", err)
	}
	if _, err := b.CAS(ctx, "k", []byte("four"), e1); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("reject-stale: got %v, want ErrPrecondition", err)
	}
}

func TestRangedGet(t *testing.T) {
	ctx := context.Background()
	b, _ := NewFSBucket(t.TempDir())
	if _, err := b.Put(ctx, "r", []byte("0123456789")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, _, err := b.RangedGet(ctx, "r", 2, 4)
	if err != nil {
		t.Fatalf("ranged get: %v", err)
	}
	if string(got) != "2345" {
		t.Fatalf("ranged get = %q, want 2345", got)
	}
}

func TestGetMissing(t *testing.T) {
	ctx := context.Background()
	b, _ := NewFSBucket(t.TempDir())
	if _, _, err := b.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: got %v, want ErrNotFound", err)
	}
}

func TestDiagnose(t *testing.T) {
	ctx := context.Background()
	b, _ := NewFSBucket(t.TempDir())
	if err := Diagnose(ctx, b); err != nil {
		t.Fatalf("diagnose: %v", err)
	}
}

// deleteIgnoringEtag simulates an S3-compatible store that accepts If-Match on
// PutObject but ignores it on DeleteObject (the degradation the diagnose probe
// must catch).
type deleteIgnoringEtag struct{ *FSBucket }

func (d deleteIgnoringEtag) ConditionalDelete(ctx context.Context, key, _ string) error {
	return d.FSBucket.Delete(ctx, key)
}

func TestDiagnoseDetectsConditionalDeleteIgnored(t *testing.T) {
	ctx := context.Background()
	b, _ := NewFSBucket(t.TempDir())
	err := Diagnose(ctx, deleteIgnoringEtag{b})
	if err == nil {
		t.Fatal("diagnose accepted a store that ignores If-Match on delete")
	}
	if !strings.Contains(err.Error(), "conditional delete (reject-stale)") {
		t.Fatalf("diagnose error = %v, want the conditional-delete probe to name the failure", err)
	}
}

// TestFSListPage covers bounded cursor listing on the filesystem bucket
// (ADR-145): exclusive cursor, ordered keys, empty next on the last page.
func TestFSListPage(t *testing.T) {
	ctx := context.Background()
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"p/c", "p/a", "p/e", "p/b", "p/d", "q/x"} {
		if _, err := b.Put(ctx, k, []byte(k)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	page1, next, err := b.ListPage(ctx, "p/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].Key != "p/a" || page1[1].Key != "p/b" || next != "p/b" {
		t.Fatalf("page1 = %+v next=%q", page1, next)
	}
	page2, next2, err := b.ListPage(ctx, "p/", next, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].Key != "p/c" || next2 != "p/d" {
		t.Fatalf("page2 = %+v next=%q", page2, next2)
	}
	page3, next3, err := b.ListPage(ctx, "p/", next2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 || page3[0].Key != "p/e" || next3 != "" {
		t.Fatalf("page3 = %+v next=%q (want the last key and no cursor)", page3, next3)
	}
	// Past the end.
	if objs, next, err := b.ListPage(ctx, "p/", "p/z", 2); err != nil || len(objs) != 0 || next != "" {
		t.Fatalf("past end = %+v %q %v", objs, next, err)
	}
}
