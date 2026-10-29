package bucket

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestFSListPageArbitraryPrefix(t *testing.T) {
	ctx := context.Background()
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"r2/ns/b/xmeta.txt", "r2/ns/b/xmeta2.txt", "r2/ns/b/other.txt"} {
		if _, err := b.Put(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	objs, _, err := b.ListPage(ctx, "r2/ns/b/xmeta", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("prefix objects = %v, want xmeta.txt/xmeta2.txt", objs)
	}
	objs, _, err = b.ListPage(ctx, "r2/ns/b/xmeta.", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 || objs[0].Key != "r2/ns/b/xmeta.txt" {
		t.Fatalf("dot-prefix objects = %v", objs)
	}
}

// TestFSBucketLocksArePerKey guards the fix for node-lease starvation: a slow
// write on one key must not hold up a different key (the lease key).
func TestFSBucketLocksArePerKey(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	unlockA := b.acquire("a")
	done := make(chan struct{})
	go func() {
		unlockB := b.acquire("b")
		unlockB()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		unlockA()
		t.Fatal("different key was blocked")
	}
	unlockA()

	unlockA = b.acquire("a")
	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		close(started)
		unlock := b.acquire("a")
		unlock()
		close(finished)
	}()
	<-started
	select {
	case <-finished:
		unlockA()
		t.Fatal("same key was not serialized")
	case <-time.After(50 * time.Millisecond):
	}
	unlockA()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("same key stayed blocked after release")
	}
}

// TestFSBucketConcurrentKeys checks that independent keys can be written and
// read concurrently without corrupting each other.
func TestFSBucketConcurrentKeys(t *testing.T) {
	ctx := context.Background()
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k/" + string(rune('a'+i%26)) + "-" + string(rune('a'+i/26))
			want := []byte(key)
			if _, err := b.Put(ctx, key, want); err != nil {
				t.Errorf("put %s: %v", key, err)
				return
			}
			got, _, err := b.Get(ctx, key)
			if err != nil || string(got) != string(want) {
				t.Errorf("get %s = %q, %v", key, got, err)
			}
		}()
	}
	wg.Wait()
}

// TestFSBucketListSkipsTemp verifies that incomplete temp files are not exposed
// as objects while another key is being written.
func TestFSBucketListSkipsTemp(t *testing.T) {
	ctx := context.Background()
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, "p/real", []byte("v")); err != nil {
		t.Fatal(err)
	}
	tmp := b.path("p/inflight.tmp")
	if err := os.MkdirAll(b.path("p"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	keys, err := b.List(ctx, "p/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "p/real" {
		t.Fatalf("List = %v, want only p/real", keys)
	}
}
