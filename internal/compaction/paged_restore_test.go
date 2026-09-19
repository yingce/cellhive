package compaction

import (
	"bytes"
	"cellhive/internal/ltx"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/replica"
)

// rangedCounter records page-level reads from the replica.
type rangedCounter struct {
	bucket.Bucket
	mu          sync.Mutex
	rangedGets  int
	rangedBytes int64
	getBytes    int64
}

func (c *rangedCounter) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	data, etag, err := c.Bucket.RangedGet(ctx, key, off, length)
	c.mu.Lock()
	c.rangedGets++
	c.rangedBytes += int64(len(data))
	c.mu.Unlock()
	return data, etag, err
}

func (c *rangedCounter) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := c.Bucket.Get(ctx, key)
	c.mu.Lock()
	c.getBytes += int64(len(data))
	c.mu.Unlock()
	return data, etag, err
}

func (c *rangedCounter) ranged() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rangedGets
}

func (c *rangedCounter) reset() {
	c.mu.Lock()
	c.rangedGets, c.rangedBytes, c.getBytes = 0, 0, 0
	c.mu.Unlock()
}

func (c *rangedCounter) bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rangedBytes
}

// pfSource adapts replica.PageFetcher to pagedvfs.Source.
type pfSource struct{ pf *replica.PageFetcher }

func (p pfSource) PageSize() int { return int(p.pf.PageSize()) }
func (p pfSource) Commit() int   { return int(p.pf.Commit()) }
func (p pfSource) ReadPage(pgno uint32) ([]byte, error) {
	return p.pf.Page(context.Background(), pgno)
}

// TestPagedCellAgentEndToEnd is the ADR-160 end-to-end path: a real cell is
// captured into the bucket as an LTX snapshot, compacted into an L1 page index,
// and then a cold cellstore serves it through the fault-in VFS backed by
// replica.PageFetcher — a KV point read faults a handful of pages with ranged
// reads instead of downloading the whole image; SnapshotPages hydrates all.
func TestPagedCellAgentEndToEnd(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	counting := &rangedCounter{Bucket: b}
	rep := replica.New(counting)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "paged"}
	const epoch = uint64(1)

	// Real cell image with a few thousand KV rows.
	src, err := cellstore.New(filepath.Join(dir, "src"))
	if err != nil {
		t.Fatalf("src store: %v", err)
	}
	c, err := src.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("src cell: %v", err)
	}
	value := make([]byte, 900)
	for i := range value {
		value[i] = byte('A' + i%26)
	}
	for i := 0; i < 3000; i++ {
		if err := c.Put(ctx, fmt.Sprintf("key-%d", i), value, nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pageSize, commit, pages, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot pages: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}
	if commit < 100 {
		t.Fatalf("image too small: %d pages", commit)
	}

	// Capture as one snapshot object and compact it so an L1 index exists.
	if _, _, err := rep.Append(ctx, sc, epoch, snapshotSegment(t, epoch, pageSize, commit, pages)); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	if res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1, GC: true}); err != nil || !res.Compacted {
		t.Fatalf("compact: %+v, %v", res, err)
	}
	// Measure only the cold open from here: index + L0 metadata + faulted pages.
	counting.reset()
	pf, err := rep.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("page fetcher: %v", err)
	}
	if pf.Commit() != commit || pf.PageSize() != pageSize {
		t.Fatalf("fetcher cut = %d/%d pages, want %d/%d", pf.Commit(), pf.PageSize(), commit, pageSize)
	}

	// Cold open through the paged hook.
	dst, err := cellstore.New(filepath.Join(dir, "dst"))
	if err != nil {
		t.Fatalf("dst store: %v", err)
	}
	dst.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return pfSource{pf: pf}, true, nil
	}
	dst.PagedHydrateMBPS = 0
	pc, err := dst.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("paged cell: %v", err)
	}
	got, _, err := pc.Get(ctx, "key-42")
	if err != nil || string(got) != string(value) {
		t.Fatalf("paged get = %d bytes, %v", len(got), err)
	}
	faults := pagedvfs.Faults(pc.Path)
	if faults == 0 {
		t.Fatal("no page faulted through the VFS")
	}
	if int(faults) >= int(commit) {
		t.Fatalf("faulted %d of %d pages; a point read must not pull the image", faults, commit)
	}
	rangedCold := counting.ranged()
	if rangedCold == 0 {
		t.Fatal("no ranged read: pages should come from the L1 page index")
	}
	if rangedCold > 32 {
		t.Fatalf("cold open did %d ranged reads for %d page faults; want one per fault", rangedCold, faults)
	}
	// A full scan must amortize: one ranged read per window, not per page.
	var rows int
	if err := pc.DB.QueryRowContext(ctx, `SELECT count(1) FROM kv`).Scan(&rows); err != nil {
		t.Fatalf("scan paged cell: %v", err)
	}
	if rows != 3000 {
		t.Fatalf("paged scan rows = %d, want 3000", rows)
	}
	rangedAfterScan := counting.ranged()
	if rangedAfterScan > int(commit)/4 {
		t.Fatalf("full scan used %d ranged reads for %d pages: window prefetch not effective", rangedAfterScan, commit)
	}

	// The index/L0 metadata reads are small compared to the image.
	if counting.getBytes >= int64(pageSize)*int64(commit)/2 {
		t.Fatalf("cold open whole-object reads = %d bytes, want well under the image %d", counting.getBytes, int64(pageSize)*int64(commit))
	}

	// SnapshotPages hydrates everything before reading the file directly.
	if _, oc, op, err := pc.SnapshotPages(ctx); err != nil {
		t.Fatalf("snapshot pages: %v", err)
	} else if oc != commit || len(op) != int(commit) {
		t.Fatalf("snapshot = %d/%d pages, want %d", oc, len(op), commit)
	}
	if err := pc.Close(); err != nil {
		t.Fatalf("close paged: %v", err)
	}
	rawImage := int64(pageSize) * int64(commit)
	t.Logf("paged cold open: %d faults/%d pages, %d ranged reads (%d bytes = %.1f%% of the raw image), %d whole-object bytes (image %d); after a full scan: %d ranged reads",
		faults, commit, rangedCold, counting.bytes(), 100*float64(counting.bytes())/float64(rawImage),
		counting.getBytes, rawImage, rangedAfterScan)
}

// TestL1SnapshotCompression: the L1 payload written by compaction compresses
// repetitive SQLite pages with LZ4 while staying byte-exact on read (ADR-160).
func TestL1SnapshotCompression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "ratio"}
	st, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 900)
	for i := range value {
		value[i] = byte('a' + i%4) // highly repetitive page content
	}
	for i := 0; i < 3000; i++ {
		if err := c.Put(ctx, fmt.Sprintf("key-%d", i), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pages, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()

	sorted := make([]ltx.WALPage, 0, commit)
	for p := uint32(1); p <= commit; p++ {
		sorted = append(sorted, ltx.WALPage{PageNo: p, Data: pages[p]})
	}
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatal(err)
	}
	raw := int64(commit) * int64(4+pageSize) // v1 fixed-frame size
	ratio := 100 * float64(len(payload)) / float64(raw)
	if ratio >= 60 {
		t.Fatalf("L1 payload = %d bytes, raw %d (%.1f%%): compression not effective", len(payload), raw, ratio)
	}
	// Every page must round-trip exactly (compressed or raw frames).
	locs, err := ltx.PageLocs(payload)
	if err != nil || len(locs) != int(commit) {
		t.Fatalf("locs = %d, %v; want %d", len(locs), err, commit)
	}
	compressed := 0
	for _, l := range locs {
		if l.Codec == ltx.CodecLZ4 {
			compressed++
		}
	}
	if compressed == 0 {
		t.Fatal("no frame compressed")
	}
	ps, cm, _, got, err := ltx.DecodeWALPageMap(payload)
	if err != nil || ps != pageSize || cm != commit || len(got) != int(commit) {
		t.Fatalf("decode = %d/%d/%d, %v", ps, cm, len(got), err)
	}
	for i := range sorted {
		if !bytes.Equal(got[i].Data, sorted[i].Data) {
			t.Fatalf("page %d differs after compression", sorted[i].PageNo)
		}
	}
	t.Logf("L1 snapshot: %d pages, %d -> %d bytes (%.1f%% of raw), %d compressed frames",
		commit, raw, len(payload), ratio, compressed)
}
