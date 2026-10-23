package cellstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

type fakePagedSource struct {
	ps, commit int
	txid       uint64
}

func (f fakePagedSource) PageSize() int { return f.ps }
func (f fakePagedSource) Commit() int   { return f.commit }
func (f fakePagedSource) ReadPage(uint32) ([]byte, error) {
	return make([]byte, f.ps), nil
}
func (f fakePagedSource) TxID() uint64 { return f.txid }

func TestPagedMarkerTxIDRoundTrip(t *testing.T) {
	p := t.TempDir() + "/x.db"
	if err := writePagedMarker(p, 4096, 12, 99, true); err != nil {
		t.Fatal(err)
	}
	ps, commit, txid, clean, exists, valid := readPagedMarker(p)
	if !exists || !valid || !clean || ps != 4096 || commit != 12 || txid != 99 {
		t.Fatalf("marker = ps=%d commit=%d txid=%d clean=%v exists=%v valid=%v", ps, commit, txid, clean, exists, valid)
	}
	// Older markers without a clean flag are treated as dirty (fail closed).
	for _, body := range []string{"4096 12", "4096 12 99"} {
		if err := os.WriteFile(pagedMarkerPath(p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		_, _, _, clean, exists, valid = readPagedMarker(p)
		if !exists || !valid || clean {
			t.Fatalf("legacy marker %q = clean=%v exists=%v valid=%v", body, clean, exists, valid)
		}
	}
}

func TestPagedCacheRejectsOlderBucketCut(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-old"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("sparse"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 10, 100, true); err != nil {
		t.Fatal(err)
	}
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return fakePagedSource{ps: 4096, commit: 10, txid: 50}, true, nil
	}
	if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("older bucket cut accepted: %v", err)
	}
}

func TestPagedDirtyCacheFailsClosed(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-dirty"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("sparse"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 10, 100, false); err != nil {
		t.Fatal(err)
	}
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return fakePagedSource{ps: 4096, commit: 10, txid: 100}, true, nil
	}
	if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("dirty cache accepted: %v", err)
	}
}

// buildPagedCut builds a durable image and returns a source that serves it.
func buildPagedCut(t *testing.T, id string) (cell.Scope, *fileSource) {
	t.Helper()
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: id}
	built, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c, err := built.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 900)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := 0; i < 2500; i++ {
		if err := c.Put(ctx, "key-"+itoa(i), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatal(err)
	}
	image := c.Path
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return sc, sourceFromImage(t, image)
}

func TestPagedCellSweepEvictionReopensWithData(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	sc, source := buildPagedCut(t, "evict")
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return source, true, nil
	}
	st.IdleTTL = time.Millisecond
	st.PagedHydrateMBPS = 0
	pc, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pc.Get(ctx, "key-42"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if n := st.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d cells, want 1", n)
	}
	if pagedvfs.IsPaged(pc.Path) {
		t.Fatal("paged registration survived sweep")
	}
	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st2.Paged = st.Paged
	st2.PagedHydrateMBPS = 0
	pc2, err := st2.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("reopen after sweep: %v", err)
	}
	if v, _, err := pc2.Get(ctx, "key-42"); err != nil || len(v) != 900 {
		t.Fatalf("reopen after sweep = %d bytes, %v", len(v), err)
	}
	_ = pc2.Close()
}

func TestPagedCellCrashLeavesDirtyMarkerAndFailsClosed(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	sc, source := buildPagedCut(t, "crash")
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return source, true, nil
	}
	st.PagedHydrateMBPS = 0
	pc, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pc.Get(ctx, "key-7"); err != nil {
		t.Fatal(err)
	}
	if err := pc.Put(ctx, "acked-after-open", []byte("must-not-be-silently-dropped"), nil); err != nil {
		t.Fatal(err)
	}
	// Simulate SIGKILL: close the SQLite fd and release the VFS registration
	// without running Cell.Close (which would write a clean marker).
	if err := pc.DB.Close(); err != nil {
		t.Fatal(err)
	}
	pagedvfs.Release(pc.Path)
	if _, _, _, clean, exists, valid := readPagedMarker(pc.Path); !exists || !valid || clean {
		t.Fatalf("crash marker = clean=%v exists=%v valid=%v, want dirty", clean, exists, valid)
	}
	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st2.Paged = st.Paged
	if _, err := st2.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("crash cache accepted: %v", err)
	}
}
