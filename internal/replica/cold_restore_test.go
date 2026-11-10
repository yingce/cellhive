package replica

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

// countingBucket records object-store reads so a test can tell a whole-object
// download from a paged (ranged) one.
type countingBucket struct {
	bucket.Bucket
	mu          sync.Mutex
	gets        int
	rangedGets  int
	getBytes    int64
	rangedBytes int64
	lists       int
}

func (c *countingBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	return c.Bucket.(bucket.Statter).Stat(ctx, key)
}

func (c *countingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := c.Bucket.Get(ctx, key)
	c.mu.Lock()
	c.gets++
	c.getBytes += int64(len(data))
	c.mu.Unlock()
	return data, etag, err
}

func (c *countingBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := c.Bucket.List(ctx, prefix)
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return keys, err
}

func (c *countingBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	data, etag, err := c.Bucket.RangedGet(ctx, key, off, length)
	c.mu.Lock()
	c.rangedGets++
	c.rangedBytes += int64(len(data))
	c.mu.Unlock()
	return data, etag, err
}

func (c *countingBucket) stats() (gets, ranged int, getBytes, rangedBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.rangedGets, c.getBytes, c.rangedBytes
}

// syntheticSnapshot writes one paged LTX snapshot object of pages*pageSize bytes
// into the bucket and returns the segment.
func syntheticSnapshot(t *testing.T, m *Manager, sc cell.Scope, epoch uint64, pageSize uint32, pages int) []byte {
	t.Helper()
	walPages := make([]ltx.WALPage, 0, pages)
	for i := 1; i <= pages; i++ {
		data := make([]byte, pageSize)
		// Page 1 carries the SQLite header magic so a later integrity check
		// would find a plausible file; the rest is deterministic filler.
		if i == 1 {
			copy(data, "SQLite format 3\x00")
		}
		for j := range data {
			data[j] = byte(i + j)
		}
		walPages = append(walPages, ltx.WALPage{PageNo: uint32(i), Data: data})
	}
	seg, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: uint32(pages), MinTXID: 1, MaxTXID: 1}, walPages)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(context.Background(), sc, epoch, seg); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	return seg
}

// Whole-object restore remains available for offline export and materialization.
// Production activation also supports native indexed paging, tested separately.
func TestWholeObjectRestoreMaterializesNativeImage(t *testing.T) {
	ctx := context.Background()
	const pageSize = 4096
	const pages = 1024 // 4 MiB image
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cb := &countingBucket{Bucket: b}
	m := New(cb)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "big"}
	seg := syntheticSnapshot(t, m, sc, 1, pageSize, pages)
	// The write path may read/create a resource fence. Measure only hydrate.
	cb.mu.Lock()
	cb.gets, cb.rangedGets, cb.getBytes, cb.rangedBytes = 0, 0, 0, 0
	cb.mu.Unlock()

	// Whole-object restore followed by SQLite image materialization.
	ep, ok, err := m.LatestEpoch(ctx, sc, 0)
	if err != nil || !ok {
		t.Fatalf("latest epoch = %d, %v, %v", ep, ok, err)
	}
	segs, err := m.Restore(ctx, sc, ep)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	raw := make([][]byte, 0, len(segs))
	for _, sg := range segs {
		raw = append(raw, sg.Raw)
	}
	dest := filepath.Join(t.TempDir(), "cells", "demo", "__kv__", "big.db")
	if _, err := restore.ApplyFile(dest, raw); err != nil {
		t.Fatalf("apply: %v", err)
	}
	gets, ranged, getBytes, rangedBytes := cb.stats()
	// No page faults are needed by the whole-object API.
	if ranged != 0 {
		t.Fatalf("hydrate issued %d ranged reads; want 0 (whole-object only)", ranged)
	}
	if gets < 1 {
		t.Fatalf("hydrate bucket reads = %d whole-object; want at least the segment download", gets)
	}
	if getBytes != int64(len(seg)) {
		t.Fatalf("hydrate read %d bytes, want the whole object %d", getBytes, len(seg))
	}
	fi, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != int64(pageSize)*pages {
		t.Fatalf("restored file = %d bytes, want %d", fi.Size(), int64(pageSize)*pages)
	}

	// --- Path B: the paged primitive can materialize a page subset only.
	ix, err := restore.IndexChain(raw)
	if err != nil {
		t.Fatalf("index chain: %v", err)
	}
	sparse := filepath.Join(t.TempDir(), "partial.db")
	filled, err := ix.SparseFile(sparse, [][2]uint32{{1, 4}})
	if err != nil {
		t.Fatalf("sparse file: %v", err)
	}
	if filled != 4 {
		t.Fatalf("sparse filled = %d pages, want 4", filled)
	}
	if sfi, err := os.Stat(sparse); err != nil || sfi.Size() != int64(pageSize)*pages {
		t.Fatalf("sparse size = %v, %v; want the full image size with holes", sfi, err)
	}
	t.Logf("hydrate: 1 whole-object read (%d bytes); paged primitive: %d pages materialized (ranged reads are exercised by TestPageFetcherOnDemandFromL1Index; this bucket recorded %d ranged bytes here)",
		getBytes, filled, rangedBytes)
}

func TestRestoreChainUsesSeparateLevelScans(t *testing.T) {
	ctx := context.Background()
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cb := &countingBucket{Bucket: base}
	m := New(cb)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "one-list"}
	// Each epoch contains an independent native snapshot.
	for e := uint64(1); e <= 3; e++ {
		snap := nativeBlob(t, 1, e, byte(e))
		if _, _, err := m.Append(ctx, sc, e, snap); err != nil {
			t.Fatal(err)
		}
	}
	cb.mu.Lock()
	cb.lists = 0
	cb.mu.Unlock()
	if _, err := m.RestoreChain(ctx, sc, 3); err != nil {
		t.Fatal(err)
	}
	cb.mu.Lock()
	lists := cb.lists
	cb.mu.Unlock()
	if lists < 2 {
		t.Fatalf("RestoreChain listed %d times, want separate level scans", lists)
	}
}

func TestRestoreChainKeepsCompactedEpochWithOnlyNativeL1(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "compacted"}
	snapshot := syntheticSnapshot(t, m, sc, 1, 4096, 2)
	_, _, err = m.AppendL1(ctx, sc, 1, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	l0, err := m.ListL0Segments(ctx, sc, 1)
	if err != nil || len(l0) == 0 {
		t.Fatalf("L0 before GC: %v %v", l0, err)
	}
	for _, key := range l0 {
		if _, err := m.DeleteObjectWithOptions(ctx, sc, 1, key, RetireOptions{OfflineQuiescent: true}); err != nil {
			t.Fatal(err)
		}
	}
	// A new owner finds the snapshot directly in L1 after L0 reclamation.
	segs, err := m.RestoreChain(ctx, sc, 1)
	if err != nil || len(segs) != 1 || string(segs[0].Raw) != string(snapshot) {
		t.Fatalf("compacted base lost: segments=%d err=%v", len(segs), err)
	}
}

func TestRestoreChainSkipsEpochsOlderThanNewestBase(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__invocations__", ID: "main"}
	old := nativeBlob(t, 1, 1, 1)
	if _, _, err := m.Append(ctx, sc, 1, old); err != nil {
		t.Fatal(err)
	}
	_ = syntheticSnapshot(t, m, sc, 3, 4096, 1)
	fresh, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 4096)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, 3, fresh); err != nil {
		t.Fatal(err)
	}
	segs, err := m.RestoreChain(ctx, sc, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, sg := range segs {
		if sg.Header.Epoch < 3 {
			t.Fatalf("restored old epoch segment %q above newest base", sg.Key)
		}
	}
	if len(segs) < 2 {
		t.Fatalf("segments = %d, want newest snapshot and its delta", len(segs))
	}
}
