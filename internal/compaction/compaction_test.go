package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

func snapshotSegment(t *testing.T, epoch uint64, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			t.Fatalf("snapshot missing page %d of %d", pgno, commit)
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: 1, MaxTXID: 1}, sorted)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return raw
}

func deltaSegment(t *testing.T, epoch uint64, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if data, ok := pages[pgno]; ok {
			sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
		}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: start, MaxTXID: start}, sorted)
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	return raw
}

func raws(segs []replica.Segment) [][]byte {
	out := make([][]byte, len(segs))
	for i := range segs {
		out[i] = segs[i].Raw
	}
	return out
}

func applyAndCheck(t *testing.T, segs []replica.Segment, dest, key, want string) {
	t.Helper()
	if _, err := restore.ApplyFile(dest, raws(segs)); err != nil {
		t.Fatalf("apply %s: %v", dest, err)
	}
	c, err := cellstore.OpenAt(context.Background(), dest)
	if err != nil {
		t.Fatalf("open %s: %v", dest, err)
	}
	defer c.Close()
	v, _, err := c.Get(context.Background(), key)
	if err != nil || string(v) != want {
		t.Fatalf("%s get %q = %q, %v", dest, key, v, err)
	}
	var ic string
	if err := c.DB.QueryRowContext(context.Background(), "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("%s integrity = %q, %v", dest, ic, err)
	}
}

func TestCompactFoldsToL1AndRestoreMatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := cellstore.New(filepath.Join(dir, "db"))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "c1"}
	c, err := store.Open(ctx, sc)
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
		if err := c.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), nil); err != nil {
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
		t.Fatalf("page size changed")
	}
	changed1 := map[uint32][]byte{}
	for pgno, data := range pagesB {
		if old, ok := pagesA[pgno]; !ok || string(old) != string(data) {
			changed1[pgno] = data
		}
	}

	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	const epoch = uint64(1)
	for _, seg := range [][]byte{
		snapshotSegment(t, epoch, pageSize, commitA, pagesA),
		deltaSegment(t, epoch, pageSizeB, commitB, 2, changed1),
	} {
		if _, _, err := rep.Append(ctx, sc, epoch, seg); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	l0, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore L0: %v", err)
	}
	applyAndCheck(t, l0, filepath.Join(dir, "l0.db"), "k29", "v")

	res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !res.Compacted {
		t.Fatalf("not compacted: %s", res.Skipped)
	}
	if res.MaxTxID != 2 {
		t.Fatalf("max txid = %d, want 2", res.MaxTxID)
	}
	keys, err := rep.ListLevel(ctx, sc, epoch, 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("L1 objects=%v err=%v", keys, err)
	}
	raw, err := rep.ReadSegment(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	h, err := ltx.ValidateNative(raw)
	if err != nil || h.Commit != commitB || h.MinTXID != 1 || h.MaxTXID != 2 {
		t.Fatalf("native L1: %+v %v", h, err)
	}

	l1, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore L1: %v", err)
	}
	if len(l1) >= len(l0) {
		t.Fatalf("L1 restore did not shrink: %d >= %d", len(l1), len(l0))
	}
	applyAndCheck(t, l1, filepath.Join(dir, "l1.db"), "k29", "v")

	// A new delta after the fold must still apply on top of L1.
	if err := c.Put(ctx, "after", []byte("2"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint C: %v", err)
	}
	_, commitC, pagesC, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot C: %v", err)
	}
	changed2 := map[uint32][]byte{}
	for pgno, data := range pagesC {
		if old, ok := pagesB[pgno]; !ok || string(old) != string(data) {
			changed2[pgno] = data
		}
	}
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, commitC, 3, changed2)); err != nil {
		t.Fatalf("append delta2: %v", err)
	}
	withDelta, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore L1+delta: %v", err)
	}
	applyAndCheck(t, withDelta, filepath.Join(dir, "d2.db"), "after", "2")

	// Compacting again folds the new delta into a higher L1 watermark.
	res2, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil {
		t.Fatalf("recompact: %v", err)
	}
	if !res2.Compacted || res2.MaxTxID != 3 {
		t.Fatalf("recompact = %+v", res2)
	}
	after, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore after recompact: %v", err)
	}
	applyAndCheck(t, after, filepath.Join(dir, "d3.db"), "after", "2")
}

func TestCompactPromotesDeltaOnlyEpochAndHonorsThreshold(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "c2"}
	const epoch = uint64(3)

	// A continued epoch can promote delta ranges without an independent snapshot.
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !res.Compacted || res.MinTxID != 2 || res.MaxTxID != 2 {
		t.Fatalf("result = %+v", res)
	}

	// An independent snapshot below a high threshold is retained at L0.
	sc.ID = "below-threshold"
	if _, _, err := rep.Append(ctx, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	res, err = Compact(ctx, rep, sc, epoch, Options{MinSegments: 100, MinBytes: 1 << 30})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if res.Compacted || res.Skipped != "below threshold" {
		t.Fatalf("result = %+v", res)
	}
}

func TestCompactVerifyAbortsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "c3"}
	const epoch = uint64(1)
	if _, _, err := rep.Append(ctx, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatalf("append: %v", err)
	}
	fail := fmt.Errorf("not owner")
	_, err = Compact(ctx, rep, sc, epoch, Options{MinSegments: 1, Verify: func(context.Context) error { return fail }})
	if err == nil {
		t.Fatalf("want verify error")
	}
	if keys, err := rep.ListLevel(ctx, sc, epoch, 1); err != nil || len(keys) != 0 {
		t.Fatalf("published after Verify failure: %v %v", keys, err)
	}
}

func TestPageFetcherOnDemandFromNativeL1Index(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "fetch"}
	const epoch = uint64(1)
	const pageSize = uint32(4096)

	mk := func(fill byte) []byte {
		p := make([]byte, pageSize)
		for i := range p {
			p[i] = fill
		}
		return p
	}
	snap := map[uint32][]byte{1: mk(1), 2: mk(2), 3: mk(3), 4: mk(4), 5: mk(5)}
	d1 := map[uint32][]byte{3: mk(0x31)} // page 3 changes at txid 1
	d2 := map[uint32][]byte{5: mk(0x52)} // page 5 changes at txid 2 (after compaction)

	if _, _, err := rep.Append(ctx, sc, epoch, snapshotSegment(t, epoch, pageSize, 5, snap)); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 2, d1)); err != nil {
		t.Fatalf("append delta1: %v", err)
	}
	res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("compact: %+v, %v", res, err)
	}

	// A newer delta lands after compaction and is not folded into L1.
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 3, d2)); err != nil {
		t.Fatalf("append delta2: %v", err)
	}

	pf, err := rep.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}
	if pf.Commit() != 5 || pf.PageSize() != pageSize || pf.Watermark() != 3 {
		t.Fatalf("fetcher dims commit=%d pageSize=%d watermark=%d", pf.Commit(), pf.PageSize(), pf.Watermark())
	}
	// Page 3 comes from L1 (ranged read), page 5 from the newer L0 delta, page 1
	// untouched from L1.
	if got, err := pf.Page(ctx, 3); err != nil || got[0] != 0x31 {
		t.Fatalf("page 3 = %x err=%v", got, err)
	}
	if got, err := pf.Page(ctx, 5); err != nil || got[0] != 0x52 {
		t.Fatalf("page 5 = %x err=%v", got, err)
	}
	if got, err := pf.Page(ctx, 1); err != nil || got[0] != 1 {
		t.Fatalf("page 1 = %x err=%v", got, err)
	}

	// The full on-demand image must equal a full ApplyFile of the same chain.
	expected := filepath.Join(dir, "expected.db")
	if _, err := restore.ApplyFile(expected, [][]byte{
		snapshotSegment(t, epoch, pageSize, 5, snap),
		deltaSegment(t, epoch, pageSize, 5, 2, d1),
		deltaSegment(t, epoch, pageSize, 5, 3, d2),
	}); err != nil {
		t.Fatalf("expected apply: %v", err)
	}
	want, _ := os.ReadFile(expected)

	got := filepath.Join(dir, "ondemand.db")
	n, err := pf.Materialize(ctx, got, nil)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if n != 5 {
		t.Fatalf("materialized %d pages, want 5", n)
	}
	gotBytes, _ := os.ReadFile(got)
	if !bytes.Equal(gotBytes, want) {
		t.Fatalf("on-demand image differs from ApplyFile")
	}

	// A partial materialization leaves the rest as holes.
	sparse := filepath.Join(dir, "ondemand-sparse.db")
	if _, err := pf.Materialize(ctx, sparse, [][2]uint32{{1, 1}}); err != nil {
		t.Fatalf("materialize sparse: %v", err)
	}
	sb, _ := os.ReadFile(sparse)
	if len(sb) != len(want) || !bytes.Equal(sb[:int(pageSize)], want[:int(pageSize)]) {
		t.Fatalf("sparse filled range mismatch")
	}
	if !bytes.Equal(sb[int(pageSize):], make([]byte, len(want)-int(pageSize))) {
		t.Fatalf("expected a hole after the filled page")
	}
}

func TestPageFetcherDeduplicatesRecoveredDelta(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "fetch-dedup"}
	const epoch = uint64(1)
	const pageSize = uint32(4096)
	mk := func(fill byte) []byte {
		p := make([]byte, pageSize)
		for i := range p {
			p[i] = fill
		}
		return p
	}
	snap := map[uint32][]byte{1: mk(1), 2: mk(2), 3: mk(3), 4: mk(4), 5: mk(5)}
	if _, _, err := rep.Append(ctx, sc, epoch, snapshotSegment(t, epoch, pageSize, 5, snap)); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 2, map[uint32][]byte{3: mk(0x31)})); err != nil {
		t.Fatalf("append delta1: %v", err)
	}
	if _, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// Owner uploads the delta as a batch; recovery re-uploads the same single
	// segment. PageFetcher must keep one logical copy.
	dup := deltaSegment(t, epoch, pageSize, 5, 3, map[uint32][]byte{5: mk(0x52)})
	if _, _, err := rep.AppendBatch(ctx, sc, epoch, [][]byte{dup}); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if _, _, err := rep.Append(ctx, sc, epoch, dup); err != nil {
		t.Fatalf("append single: %v", err)
	}
	pf, err := rep.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}
	if got, err := pf.Page(ctx, 5); err != nil || got[0] != 0x52 {
		t.Fatalf("page 5 = %x, %v", got, err)
	}
}
