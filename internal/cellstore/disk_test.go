package cellstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// TestPlanDiskEviction covers the disk-budget planner (ADR-122): LRU order,
// budget arithmetic, and the rule that an owned, non-safe file is never evicted.
func TestPlanDiskEviction(t *testing.T) {
	mk := func(path string, bytes, modMs int64, owned, safe bool) DiskFile {
		return DiskFile{Path: path, Bytes: bytes, ModMs: modMs, Owned: owned, Safe: safe}
	}
	files := []DiskFile{
		mk("old", 100, 10, false, false),
		mk("mid", 100, 20, false, false),
		mk("new", 100, 30, false, false),
		mk("owned", 100, 5, true, false),
		mk("owned-safe", 100, 1, true, true),
	}
	// Budget 300 of 500 bytes: delete oldest first (owned-safe then old).
	plan := PlanDiskEviction(files, 300)
	if len(plan) != 2 {
		t.Fatalf("plan = %v, want 2 evictions", plan)
	}
	got := map[string]bool{}
	for _, i := range plan {
		got[files[i].Path] = true
	}
	if !got["owned-safe"] || !got["old"] {
		t.Fatalf("plan = %v, want owned-safe and old", got)
	}
	if got["owned"] {
		t.Fatal("owned non-safe file must never be evicted")
	}

	// Under budget: nothing.
	if p := PlanDiskEviction(files, 1000); p != nil {
		t.Fatalf("plan under budget = %v, want none", p)
	}
	// Budget 0 (disabled): nothing.
	if p := PlanDiskEviction(files, 0); p != nil {
		t.Fatalf("plan with budget 0 = %v, want none", p)
	}
	// Every eligible file when the budget is tiny, and nothing infinite-loops.
	plan = PlanDiskEviction(files, 1)
	if len(plan) != 4 {
		t.Fatalf("plan for tiny budget = %v, want 4 (all but owned)", plan)
	}
}

// TestDiskFilesAndForget covers the on-disk ledger and file deletion.
func TestDiskFilesAndForget(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ctx := context.Background()
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "main"}
	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	files, err := s.DiskFiles()
	if err != nil {
		t.Fatalf("disk files: %v", err)
	}
	if len(files) != 1 || files[0].Scope != sc || files[0].Bytes == 0 || files[0].ModMs == 0 {
		t.Fatalf("disk files = %+v", files)
	}
	if n, bytes := s.DiskUsage(); n != 1 || bytes <= 0 {
		t.Fatalf("disk usage = %d, %d", n, bytes)
	}

	// Forget closes the handle and deletes the files.
	if err := s.Forget(ctx, sc); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := os.Stat(files[0].Path); !os.IsNotExist(err) {
		t.Fatalf("cell file still present after Forget: %v", err)
	}
	if err := s.Forget(ctx, sc); err != nil { // idempotent
		t.Fatalf("second forget: %v", err)
	}
	_ = time.Now
}

// TestSelectDiskEvictionsLazy covers the lazy eligibility selector (ADR-122/123):
// the expensive Owned/Safe decision is only made for files the selector actually
// considers, and never at all when the total is under budget.
func TestSelectDiskEvictionsLazy(t *testing.T) {
	mk := func(path string, bytes, modMs int64) DiskFile {
		return DiskFile{Path: path, Bytes: bytes, ModMs: modMs}
	}
	files := []DiskFile{mk("f1", 100, 10), mk("f2", 100, 20), mk("f3", 100, 30)}

	// Under budget: the callback is never called.
	calls := 0
	if plan := SelectDiskEvictions(files, 1000, func(int) (bool, bool) { calls++; return false, false }); plan != nil || calls != 0 {
		t.Fatalf("under budget: plan=%v calls=%d, want none/0", plan, calls)
	}

	// Budget 150 of 300: delete the two oldest, and only decide for them.
	calls = 0
	plan := SelectDiskEvictions(files, 150, func(int) (bool, bool) { calls++; return false, false })
	if len(plan) != 2 || plan[0] != 0 || plan[1] != 1 || calls != 2 {
		t.Fatalf("plan=%v calls=%d, want [0 1] and 2 decisions", plan, calls)
	}

	// Owned+unsafe files are skipped (and still counted as decided).
	calls = 0
	plan = SelectDiskEvictions(files, 150, func(i int) (bool, bool) {
		calls++
		return i == 0, false // f1 is owned and not safe
	})
	if len(plan) != 2 || plan[0] != 1 || plan[1] != 2 || calls != 3 {
		t.Fatalf("plan=%v calls=%d, want [1 2] and 3 decisions", plan, calls)
	}
}

// TestDiskUsageCountsAllocatedBytes: a sparse paged cell must be accounted by
// its allocated blocks, not its apparent size (ADR-160), so the disk high
// watermark does not trip on cache files that occupy almost nothing.
func TestDiskUsageCountsAllocatedBytes(t *testing.T) {
	dir := t.TempDir()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "sparse"}
	p := filepath.Join(dir, "cells", "acme", "__kv__", "sparse.db")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	// Half a megabyte of hole, one page of real data.
	if err := f.Truncate(512 << 10); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{7}, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	files, err := s.DiskFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Scope != sc {
		t.Fatalf("files = %+v, want the sparse cell", files)
	}
	got := files[0]
	if got.Bytes < 512<<10 {
		t.Fatalf("apparent size = %d, want >= 512KiB", got.Bytes)
	}
	if got.AllocBytes <= 0 {
		t.Skip("platform does not report allocated blocks")
	}
	if got.AllocBytes > got.Bytes/2 {
		t.Fatalf("allocated = %d, apparent = %d: sparse file not accounted by blocks", got.AllocBytes, got.Bytes)
	}
	if n, total := s.DiskUsage(); n != 1 || total >= got.Bytes {
		t.Fatalf("DiskUsage = %d files/%d bytes, want allocated (< %d)", n, total, got.Bytes)
	}
}

// TestDiskEvictionUsesAllocatedBytes: a sparse cache file frees (almost) nothing
// when deleted, so eviction must rank/account it by allocated bytes.
func TestDiskEvictionUsesAllocatedBytes(t *testing.T) {
	sparse := DiskFile{Path: "a", Bytes: 1 << 20, AllocBytes: 4096, ModMs: 1}
	dense := DiskFile{Path: "b", Bytes: 1 << 20, AllocBytes: 1 << 20, ModMs: 2}
	plan := PlanDiskEviction([]DiskFile{sparse, dense}, 1<<20)
	// Total allocated is ~1MiB+4KiB, so the oldest file (sparse) is dropped and
	// the budget is met: only one eviction.
	if len(plan) != 1 || plan[0] != 0 {
		t.Fatalf("plan = %v, want [0] (the sparse oldest)", plan)
	}
	unknown := DiskFile{Path: "c", Bytes: 1 << 20, AllocBytes: -1, ModMs: 0}
	if plan := PlanDiskEviction([]DiskFile{unknown}, 1<<20); len(plan) != 0 {
		t.Fatalf("apparent-size accounting regressed: %v", plan)
	}
}
