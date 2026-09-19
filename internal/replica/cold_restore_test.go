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
}

func (c *countingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := c.Bucket.Get(ctx, key)
	c.mu.Lock()
	c.gets++
	c.getBytes += int64(len(data))
	c.mu.Unlock()
	return data, etag, err
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
	payload, err := ltx.EncodeWALPageMap(pageSize, uint32(pages), 1, walPages)
	if err != nil {
		t.Fatalf("encode page map: %v", err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 1, EndTxID: 1}, payload)
	if _, _, err := m.Append(context.Background(), sc, epoch, seg); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	return seg
}

// TestColdHydrateIsWholeObjectNotPaged pins the documented boundary
// (ADR-056/159, known-issues): the cell-agent cold-restore path — exactly the
// hook wired in cmd/cell-agent/main.go — downloads the whole replica object and
// writes the whole database file. The paged machinery (restore.PageIndex /
// replica.PageFetcher) exists but is not used by that path.
//
// It is a regression guard: if hydrate ever becomes paged, this test must be
// updated deliberately (with the VFS/hydration-set work that implies).
func TestColdHydrateIsWholeObjectNotPaged(t *testing.T) {
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

	// --- Path A: cell-agent hydrate (LatestEpoch -> Restore -> ApplyFile).
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
	// ranged == 0 is the point: hydrate never page-faults. gets includes the
	// manifest probes (404 -> nil) plus the one real segment download, and the
	// payload byte count must equal the whole object.
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
