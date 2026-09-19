package restore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
)

func buildChain(t *testing.T, dir, id string) (segments [][]byte, pageSize, commitA, commitB uint32) {
	t.Helper()
	store, _ := cellstore.New(filepath.Join(dir, "db"))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: id}
	c, err := store.Open(context.Background(), sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.Put(ctx, "before", []byte("1"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint A: %v", err)
	}
	pageSize, commitA, pagesA, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot A: %v", err)
	}
	for i := 0; i < 30; i++ {
		if err := c.Put(ctx, key(i), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint B: %v", err)
	}
	ps2, commitB, pagesB, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot B: %v", err)
	}
	if ps2 != pageSize {
		t.Fatalf("page size changed")
	}
	changed := map[uint32][]byte{}
	for pgno, data := range pagesB {
		if old, ok := pagesA[pgno]; !ok || string(old) != string(data) {
			changed[pgno] = data
		}
	}
	return [][]byte{
		snapshotSegment(t, pageSize, commitA, pagesA),
		deltaSegment(t, pageSize, commitB, 1, changed),
	}, pageSize, commitA, commitB
}

func TestPageIndexAndSparseFile(t *testing.T) {
	dir := t.TempDir()
	segments, pageSize, _, commitB := buildChain(t, dir, "paging")

	ix, err := IndexChain(segments)
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	if ix.PageSize != pageSize || ix.Commit != commitB {
		t.Fatalf("index dims pageSize=%d commit=%d want %d/%d", ix.PageSize, ix.Commit, pageSize, commitB)
	}
	if ix.Watermark != 1 {
		t.Fatalf("watermark = %d, want 1", ix.Watermark)
	}

	// Full materialization for comparison.
	full := filepath.Join(dir, "full.db")
	if _, err := ApplyFile(full, segments); err != nil {
		t.Fatalf("apply: %v", err)
	}
	fullBytes, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read full: %v", err)
	}
	if len(fullBytes) != int(commitB)*int(pageSize) {
		t.Fatalf("full size %d, want %d", len(fullBytes), int(commitB)*int(pageSize))
	}

	// Every page must be resolvable on demand and match the materialized image.
	if got := ix.Ranges(); len(got) == 0 {
		t.Fatalf("no ranges")
	}
	for pgno := uint32(1); pgno <= commitB; pgno++ {
		page, err := ix.Page(pgno)
		if err != nil {
			t.Fatalf("page %d: %v", pgno, err)
		}
		want := fullBytes[(int(pgno)-1)*int(pageSize) : int(pgno)*int(pageSize)]
		if !bytes.Equal(page, want) {
			t.Fatalf("page %d differs from materialized image", pgno)
		}
	}

	// Sparse file with only pages 1..2 filled: the rest must be a hole (zeros).
	sparse := filepath.Join(dir, "sparse.db")
	written, err := ix.SparseFile(sparse, [][2]uint32{{1, 2}})
	if err != nil {
		t.Fatalf("sparse: %v", err)
	}
	if written != 2 {
		t.Fatalf("wrote %d pages, want 2", written)
	}
	sparseBytes, err := os.ReadFile(sparse)
	if err != nil {
		t.Fatalf("read sparse: %v", err)
	}
	if len(sparseBytes) != len(fullBytes) {
		t.Fatalf("sparse size %d, want %d", len(sparseBytes), len(fullBytes))
	}
	if !bytes.Equal(sparseBytes[:2*int(pageSize)], fullBytes[:2*int(pageSize)]) {
		t.Fatalf("filled range differs")
	}
	if !bytes.Equal(sparseBytes[2*int(pageSize):], make([]byte, len(fullBytes)-2*int(pageSize))) {
		t.Fatalf("expected a hole after the filled range")
	}

	// Filling every range reproduces the materialized image byte for byte.
	dense := filepath.Join(dir, "dense.db")
	if _, err := ix.SparseFile(dense, ix.Ranges()); err != nil {
		t.Fatalf("sparse all: %v", err)
	}
	denseBytes, err := os.ReadFile(dense)
	if err != nil {
		t.Fatalf("read dense: %v", err)
	}
	if !bytes.Equal(denseBytes, fullBytes) {
		t.Fatalf("full-range sparse materialization differs from ApplyFile")
	}
}

func TestPageIndexRejectsGap(t *testing.T) {
	dir := t.TempDir()
	segments, pageSize, commitA, _ := buildChain(t, dir, "gapidx")
	// Replace the delta (txid 1) with one starting at txid 2.
	bad := [][]byte{segments[0], deltaSegment(t, pageSize, commitA, 2, map[uint32][]byte{1: make([]byte, pageSize)})}
	if _, err := IndexChain(bad); err == nil {
		t.Fatalf("want non-contiguous error")
	}
}

func TestPageMapNumbersAndLookupRoundTrip(t *testing.T) {
	pages := []ltx.WALPage{
		{PageNo: 1, Data: bytes.Repeat([]byte{0xaa}, 64)},
		{PageNo: 3, Data: bytes.Repeat([]byte{0xbb}, 64)},
	}
	payload, err := ltx.EncodeWALPageMap(64, 3, 1, pages)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	nums, err := ltx.PageMapNumbers(payload)
	if err != nil || len(nums) != 2 || nums[0] != 1 || nums[1] != 3 {
		t.Fatalf("numbers = %v, %v", nums, err)
	}
	got, ok, err := ltx.PageMapLookup(payload, 3)
	if err != nil || !ok || !bytes.Equal(got, pages[1].Data) {
		t.Fatalf("lookup page 3 = %x ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := ltx.PageMapLookup(payload, 2); ok {
		t.Fatalf("page 2 should be absent")
	}
}
