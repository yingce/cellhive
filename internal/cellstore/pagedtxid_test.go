package cellstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
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
func buildPagedCut(t *testing.T, id string) (cell.Scope, *fileSource, string) {
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
	return sc, sourceFromImage(t, image), image
}

func TestPagedCellSweepEvictionReopensWithData(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	sc, source, _ := buildPagedCut(t, "evict")
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
	sc, source, _ := buildPagedCut(t, "crash")
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

func TestPagedCellCrashHelper(t *testing.T) {
	if os.Getenv("PAGED_CRASH_HELPER") != "1" {
		return
	}
	image := os.Getenv("PAGED_CRASH_IMAGE")
	dir := os.Getenv("PAGED_CRASH_DIR")
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "kill"}
	src := sourceFromImage(t, image)
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return src, true, nil
	}
	st.PagedHydrateMBPS = 0
	pc, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pc.Get(ctx, "key-7"); err != nil {
		t.Fatal(err)
	}
	if err := pc.Put(ctx, "acked-after-open", []byte("crash-me"), nil); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
}

func TestPagedCellProcessKillFailsClosed(t *testing.T) {
	if !pagedvfs.Available() {
		t.Skip("paged VFS unavailable")
	}
	_, _, image := buildPagedCut(t, "kill-parent")
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestPagedCellCrashHelper")
	cmd.Env = append(os.Environ(), "PAGED_CRASH_HELPER=1", "PAGED_CRASH_IMAGE="+image, "PAGED_CRASH_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("helper did not crash: %s", out)
	}
	if _, _, _, clean, exists, valid := readPagedMarker(filepath.Join(dir, "cells", "acme", "__kv__", "kill.db")); !exists || !valid || clean {
		t.Fatalf("process crash marker = clean=%v exists=%v valid=%v, want dirty", clean, exists, valid)
	}
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return sourceFromImage(t, image), true, nil
	}
	if _, err := st.Cell(context.Background(), cell.Scope{Namespace: "acme", Class: "__kv__", ID: "kill"}); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("process crash cache accepted: %v", err)
	}
}

// A valid marker records a local txid the bucket may not cover yet. If the
// paged probe cannot provide a source, discarding the cache could roll back
// locally acked pages, so both dirty and clean caches must fail closed.
func TestPagedCacheFailsClosedWithoutSource(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "dirty", true: "clean"}[clean], func(t *testing.T) {
			ctx := context.Background()
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-nosource"}
			p, err := s.Path(sc)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte("sparse"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := writePagedMarker(p, 4096, 10, 100, clean); err != nil {
				t.Fatal(err)
			}
			s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
				return nil, false, nil
			}
			if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
				t.Fatalf("cache accepted without a paged source: %v", err)
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("cache file was removed: %v", err)
			}
			if _, _, _, _, exists, _ := readPagedMarker(p); !exists {
				t.Fatal("paged marker was removed")
			}
		})
	}
}

// A transient probe error must propagate and must not be treated as "no paged
// source" (which would discard the cache).
func TestPagedProbeErrorKeepsCache(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-probe-error"}
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
	probeErr := errors.New("bucket unavailable")
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return nil, false, probeErr
	}
	if _, err := s.Cell(ctx, sc); !errors.Is(err, probeErr) {
		t.Fatalf("probe error not propagated: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("cache file was removed on probe error: %v", err)
	}
}
