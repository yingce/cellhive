package restore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
)

func nativeTestFile(t testing.TB, ps, commit uint32, lo, hi uint64, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= commit; pg++ {
		if data, ok := pages[pg]; ok {
			sorted = append(sorted, ltx.WALPage{PageNo: pg, Data: data})
		}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: lo, MaxTXID: hi}, sorted)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func snapshotSegment(t testing.TB, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	return nativeTestFile(t, pageSize, commit, 1, 1, pages)
}
func deltaSegment(t testing.TB, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
	return nativeTestFile(t, pageSize, commit, start+1, start+1, pages)
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

func TestApplyFileNativeMultiPageSnapshotAndDelta(t *testing.T) {
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
	// Native snapshots retain every page in one indexed LTX object.
	parts := [][]byte{snapshotSegment(t, pageSize, commitA, pagesA)}

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

	compacted, err := Compact(chain)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(compacted) >= len(chain) {
		t.Fatalf("compaction did not reduce segments: %d >= %d", len(compacted), len(chain))
	}
	h, err := ltx.ValidateNative(compacted[0])
	if err != nil {
		t.Fatalf("decode compacted: %v", err)
	}
	if !h.IsSnapshot() {
		t.Fatalf("compacted header = %+v, want snapshot", h)
	}
	if h.MinTXID != 1 {
		t.Fatalf("compacted watermark = %d, want 1", h.MinTXID)
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
	// Snapshot watermark 1 with a delta starting at txid 3 (txid 2 missing).
	dest := filepath.Join(dir, "gap.db")
	_, err = ApplyFile(dest, [][]byte{
		snapshotSegment(t, pageSize, commit, pages),
		deltaSegment(t, pageSize, commit, 2, map[uint32][]byte{1: make([]byte, pageSize)}),
	})
	if err == nil {
		t.Fatalf("want non-contiguous error")
	}
}

func TestApplyFileStats(t *testing.T) {
	const ps, commit = uint32(4096), uint32(3)
	before := SnapshotStats()
	dest := t.TempDir() + "/stats.db"
	page := make([]byte, ps)
	for i := range page {
		page[i] = 7
	}
	pages := map[uint32][]byte{1: page, 2: page, 3: page}
	if _, err := ApplyFile(dest, [][]byte{snapshotSegment(t, ps, commit, pages)}); err != nil {
		t.Fatal(err)
	}
	after := SnapshotStats()
	if after.Files != before.Files+1 || after.Pages != before.Pages+uint64(commit) {
		t.Fatalf("restore stats files=%d->%d pages=%d->%d", before.Files, after.Files, before.Pages, after.Pages)
	}
	if after.LastSize != uint64(ps)*uint64(commit) || after.MaxSize < after.LastSize {
		t.Fatalf("restore sizes last=%d max=%d", after.LastSize, after.MaxSize)
	}
}

func TestApplyFileKeepsDestinationOnSyncFailure(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "x.db")
	if err := os.WriteFile(dest, []byte("old-image"), 0o644); err != nil {
		t.Fatal(err)
	}
	seg := snapshotSegment(t, 4096, 2, map[uint32][]byte{
		1: make([]byte, 4096),
		2: make([]byte, 4096),
	})
	oldSync := syncFile
	syncFile = func(*os.File) error { return errors.New("injected sync failure") }
	defer func() { syncFile = oldSync }()

	if _, err := ApplyFile(dest, [][]byte{seg}); err == nil {
		t.Fatal("ApplyFile succeeded despite sync failure")
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old-image" {
		t.Fatalf("destination was modified on failure: %q", got)
	}
	if leftovers, _ := filepath.Glob(dest + ".tmp-*"); len(leftovers) != 0 {
		t.Fatalf("temporary files left behind: %v", leftovers)
	}
}

func TestApplyFileFinalizeFailureKeepsDestination(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(dest, []byte("previous-image"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 7}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("local metadata write failed")
	_, err = ApplyFileWithFinalize(dest, [][]byte{raw}, func(path string, watermark uint64) error {
		if path == dest || watermark != 7 {
			t.Fatalf("published before finalization: %s watermark%d", path, watermark)
		}
		if err := os.WriteFile(path+"-wal", []byte("unfinished bookkeeping"), 0600); err != nil {
			t.Fatal(err)
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("error %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "previous-image" {
		t.Fatalf("destination changed: %q %v", got, err)
	}
	if leftovers, _ := filepath.Glob(dest + ".tmp-*"); len(leftovers) != 0 {
		t.Fatalf("temporary files leaked: %v", leftovers)
	}
}
