package restore

import (
	"context"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
)

func encodePageMap(t *testing.T, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			continue
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatalf("encode page map: %v", err)
	}
	return payload
}

func snapshotSegment(t *testing.T, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	t.Helper()
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 0, EndTxID: 0}, encodePageMap(t, pageSize, commit, pages))
}

func deltaSegment(t *testing.T, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if data, ok := pages[pgno]; ok {
			sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
		}
	}
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: start, EndTxID: start}, payload)
}

func TestApplyFileSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(dir)
	cellScope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "snap"}
	c, err := store.Open(ctx, cellScope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	for i := 0; i < 20; i++ {
		if err := c.Put(ctx, key(i), []byte("value"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pageSize, commit, pages, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot pages: %v", err)
	}
	dest := filepath.Join(dir, "restored.db")
	if _, err := ApplyFile(dest, [][]byte{snapshotSegment(t, pageSize, commit, pages)}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if v, _, err := restored.Get(ctx, "k19"); err != nil || string(v) != "value" {
		t.Fatalf("restored get = %q, %v", v, err)
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func TestApplyFileAppliesDelta(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(dir)
	cellScope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "delta"}
	c, err := store.Open(ctx, cellScope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
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
	// Change the database and build a delta of the pages that changed.
	for i := 0; i < 30; i++ {
		if err := c.Put(ctx, key(i), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint B: %v", err)
	}
	pageSizeB, commitB, pagesB, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot B: %v", err)
	}
	changed := map[uint32][]byte{}
	for pgno, data := range pagesB {
		old, ok := pagesA[pgno]
		if !ok || string(old) != string(data) {
			changed[pgno] = data
		}
	}
	// Simple chains keep one page size; fail loudly if VACUUM changed it.
	if pageSize != pageSizeB {
		t.Fatalf("page size changed %d -> %d", pageSize, pageSizeB)
	}
	dest := filepath.Join(dir, "restored.db")
	if _, err := ApplyFile(dest, [][]byte{
		snapshotSegment(t, pageSize, commitA, pagesA),
		deltaSegment(t, pageSizeB, commitB, 1, changed),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if v, _, err := restored.Get(ctx, "before"); err != nil || string(v) != "1" {
		t.Fatalf("pre-delta row lost: %q, %v", v, err)
	}
	if v, _, err := restored.Get(ctx, "k29"); err != nil || string(v) != "v" {
		t.Fatalf("post-delta row missing: %q, %v", v, err)
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

// pagedSnapshotSegments encodes a complete snapshot as ordered parts, each at
// most maxBytes, forcing paging when the budget is small.
func pagedSnapshotSegments(t *testing.T, h ltx.Header, pageSize, commit uint32, pages map[uint32][]byte, maxBytes int) [][]byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			t.Fatalf("snapshot missing page %d of %d", pgno, commit)
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	parts, err := ltx.EncodeSnapshotParts(h, pageSize, commit, sorted, maxBytes)
	if err != nil {
		t.Fatalf("encode snapshot parts: %v", err)
	}
	return parts
}

func TestApplyFilePagedSnapshotAndDelta(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(dir)
	cellScope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "paged"}
	c, err := store.Open(ctx, cellScope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
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
	if commitA < 2 {
		t.Skipf("need more than one page to force paging, got %d", commitA)
	}
	// One page per part forces a multi-part snapshot.
	parts := pagedSnapshotSegments(t,
		ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 0, EndTxID: 0},
		pageSize, commitA, pagesA, ltx.HeaderSize+20+4+int(pageSize))
	if len(parts) < 2 {
		t.Fatalf("expected a paged snapshot, got %d part(s)", len(parts))
	}

	for i := 0; i < 30; i++ {
		if err := c.Put(ctx, key(i), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint B: %v", err)
	}
	pageSizeB, commitB, pagesB, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot B: %v", err)
	}
	if pageSize != pageSizeB {
		t.Fatalf("page size changed %d -> %d", pageSize, pageSizeB)
	}
	changed := map[uint32][]byte{}
	for pgno, data := range pagesB {
		if old, ok := pagesA[pgno]; !ok || string(old) != string(data) {
			changed[pgno] = data
		}
	}

	segs := append([][]byte{}, parts...)
	segs = append(segs, deltaSegment(t, pageSizeB, commitB, 1, changed))

	dest := filepath.Join(dir, "restored.db")
	if _, err := ApplyFile(dest, segs); err != nil {
		t.Fatalf("apply: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if v, _, err := restored.Get(ctx, "before"); err != nil || string(v) != "1" {
		t.Fatalf("pre-delta row lost: %q, %v", v, err)
	}
	if v, _, err := restored.Get(ctx, "k29"); err != nil || string(v) != "v" {
		t.Fatalf("post-delta row missing: %q, %v", v, err)
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func TestCompactFoldsChain(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(dir)
	cellScope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "compact"}
	c, err := store.Open(ctx, cellScope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
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
	pageSizeB, commitB, pagesB, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot B: %v", err)
	}
	if pageSize != pageSizeB {
		t.Fatalf("page size changed %d -> %d", pageSize, pageSizeB)
	}
	changed := map[uint32][]byte{}
	for pgno, data := range pagesB {
		if old, ok := pagesA[pgno]; !ok || string(old) != string(data) {
			changed[pgno] = data
		}
	}
	chain := [][]byte{
		snapshotSegment(t, pageSize, commitA, pagesA),
		deltaSegment(t, pageSizeB, commitB, 1, changed),
	}

	compacted, err := Compact(chain, 0)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(compacted) >= len(chain) {
		t.Fatalf("compaction did not reduce segments: %d >= %d", len(compacted), len(chain))
	}
	h, _, err := ltx.Decode(compacted[0])
	if err != nil {
		t.Fatalf("decode compacted: %v", err)
	}
	if h.Kind != ltx.KindSnapshot {
		t.Fatalf("compacted kind = %d, want snapshot", h.Kind)
	}
	if h.StartTxID != 1 {
		t.Fatalf("compacted watermark = %d, want 1", h.StartTxID)
	}

	dest := filepath.Join(dir, "restored.db")
	if _, err := ApplyFile(dest, compacted); err != nil {
		t.Fatalf("apply compacted: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	if v, _, err := restored.Get(ctx, "before"); err != nil || string(v) != "1" {
		t.Fatalf("pre-delta row lost: %q, %v", v, err)
	}
	if v, _, err := restored.Get(ctx, "k29"); err != nil || string(v) != "v" {
		t.Fatalf("post-delta row missing: %q, %v", v, err)
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func key(i int) string {
	return "k" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}

func TestApplyFileRejectsTxIDGap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(dir)
	cellScope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "gap"}
	c, err := store.Open(ctx, cellScope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	if err := c.Put(ctx, "x", []byte("1"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pageSize, commit, pages, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Snapshot watermark 0 with a delta starting at txid 2 (txid 1 missing).
	dest := filepath.Join(dir, "gap.db")
	_, err = ApplyFile(dest, [][]byte{
		snapshotSegment(t, pageSize, commit, pages),
		deltaSegment(t, pageSize, commit, 2, map[uint32][]byte{1: make([]byte, pageSize)}),
	})
	if err == nil {
		t.Fatalf("want non-contiguous error")
	}
}
