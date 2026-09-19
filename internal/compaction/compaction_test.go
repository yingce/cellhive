package compaction

import (
	"bytes"
	"context"
	"encoding/binary"
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
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 0, EndTxID: 0}, payload)
}

func deltaSegment(t *testing.T, epoch uint64, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
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
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: start, EndTxID: start}, payload)
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
		deltaSegment(t, epoch, pageSizeB, commitB, 1, changed1),
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
	if res.MaxTxID != 1 {
		t.Fatalf("max txid = %d, want 1", res.MaxTxID)
	}
	man, ok, err := rep.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok {
		t.Fatalf("manifest ok=%v err=%v", ok, err)
	}
	if len(man.Objects) == 0 || man.Commit != commitB {
		t.Fatalf("manifest objects=%d commit=%d want %d", len(man.Objects), man.Commit, commitB)
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
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, commitC, 2, changed2)); err != nil {
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
	if !res2.Compacted || res2.MaxTxID != 2 {
		t.Fatalf("recompact = %+v", res2)
	}
	after, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore after recompact: %v", err)
	}
	applyAndCheck(t, after, filepath.Join(dir, "d3.db"), "after", "2")
}

func TestCompactSkipsBelowThresholdAndWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "c2"}
	const epoch = uint64(3)

	// Deltas only: no snapshot baseline -> skip, not error.
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if res.Compacted || res.Skipped != "no snapshot baseline" {
		t.Fatalf("result = %+v", res)
	}

	// Snapshot present but below a high threshold -> skip.
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
	if _, ok, _ := rep.ReadManifest(ctx, sc, epoch); ok {
		t.Fatalf("manifest must not be written when Verify fails")
	}
}

func TestPageFetcherOnDemandFromL1Index(t *testing.T) {
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
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 1, d1)); err != nil {
		t.Fatalf("append delta1: %v", err)
	}
	res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("compact: %+v, %v", res, err)
	}
	if _, ok, err := rep.ReadIndex(ctx, sc, epoch); err != nil || !ok {
		t.Fatalf("index ok=%v err=%v", ok, err)
	}
	// A newer delta lands after compaction and is not folded into L1.
	if _, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 2, d2)); err != nil {
		t.Fatalf("append delta2: %v", err)
	}

	pf, err := rep.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}
	if pf.Commit() != 5 || pf.PageSize() != pageSize || pf.Watermark() != 1 {
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
		deltaSegment(t, epoch, pageSize, 5, 1, d1),
		deltaSegment(t, epoch, pageSize, 5, 2, d2),
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

func TestCompactGarbageCollectsSupersededObjects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "gc"}
	const epoch = uint64(1)
	const pageSize = uint32(4096)
	mk := func(f byte) []byte {
		p := make([]byte, pageSize)
		for i := range p {
			p[i] = f
		}
		return p
	}
	appendSeg := func(seg []byte) string {
		t.Helper()
		key, _, err := rep.Append(ctx, sc, epoch, seg)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		return key
	}

	snapKey := appendSeg(snapshotSegment(t, epoch, pageSize, 5, map[uint32][]byte{1: mk(1), 2: mk(2), 3: mk(3), 4: mk(4), 5: mk(5)}))
	d1Key := appendSeg(deltaSegment(t, epoch, pageSize, 5, 1, map[uint32][]byte{3: mk(0x31)}))
	if res, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1}); err != nil || !res.Compacted {
		t.Fatalf("compact1: %+v %v", res, err)
	}
	man1, _, err := rep.ReadManifest(ctx, sc, epoch)
	if err != nil || len(man1.Objects) == 0 {
		t.Fatalf("manifest1: %+v %v", man1, err)
	}
	l1v1 := man1.Objects[0]

	d2Key := appendSeg(deltaSegment(t, epoch, pageSize, 5, 2, map[uint32][]byte{5: mk(0x52)}))
	res2, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1, GC: true})
	if err != nil {
		t.Fatalf("compact2: %v", err)
	}
	if res2.DeletedL1 == 0 {
		t.Fatalf("expected a superseded L1 object to be deleted: %+v", res2)
	}
	if res2.DeletedL0 == 0 {
		t.Fatalf("expected folded L0 objects to be deleted: %+v", res2)
	}
	if _, _, err := b.Get(ctx, l1v1); err == nil {
		t.Fatalf("superseded L1 object %s still present", l1v1)
	}
	for _, k := range []string{snapKey, d1Key, d2Key} {
		if _, _, err := b.Get(ctx, k); err == nil {
			t.Fatalf("folded L0 object %s still present", k)
		}
	}

	// The chain still restores from the surviving L1 snapshot.
	segs, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	dest := filepath.Join(dir, "gc.db")
	if _, err := restore.ApplyFile(dest, raws(segs)); err != nil {
		t.Fatalf("apply after gc: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	page := func(n int) []byte { return got[(n-1)*int(pageSize) : n*int(pageSize)] }
	if page(1)[0] != 1 || page(3)[0] != 0x31 || page(5)[0] != 0x52 {
		t.Fatalf("restored image wrong after gc: p1=%x p3=%x p5=%x", page(1)[0], page(3)[0], page(5)[0])
	}
}

// wal2SnapshotSegment hand-builds a v1 (WAL2, uncompressed fixed-frame) snapshot,
// the format written before ADR-160, so the tests keep proving that compaction
// and GC fold legacy segments into the new compressed L1.
func wal2SnapshotSegment(t *testing.T, epoch uint64, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	t.Helper()
	payload := make([]byte, 0, 20+int(commit)*(4+int(pageSize)))
	payload = append(payload, []byte("WAL2")...)
	var tmp [4]byte
	for _, v := range []uint32{pageSize, commit, 1, commit} {
		binary.BigEndian.PutUint32(tmp[:], v)
		payload = append(payload, tmp[:]...)
	}
	for p := uint32(1); p <= commit; p++ {
		data, ok := pages[p]
		if !ok {
			t.Fatalf("wal2 snapshot missing page %d", p)
		}
		binary.BigEndian.PutUint32(tmp[:], p)
		payload = append(payload, tmp[:]...)
		payload = append(payload, data...)
	}
	// Baseline snapshots sort before every delta (StartTxID 0), matching the
	// convention the capture path uses.
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 0, EndTxID: 0}, payload)
}

// TestCompactFoldsLegacyV1AndGCsCompressedL1: GC is key/watermark based, so it
// must keep working when the input chain mixes the legacy WAL2 snapshot with new
// compressed (WAL3) segments and the folded L1 is compressed. Paged reads and a
// full restore must both work from the surviving objects.
func TestCompactFoldsLegacyV1AndGCsCompressedL1(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "gc-v1"}
	const epoch = uint64(1)
	const pageSize = uint32(4096)
	mk := func(f byte) []byte {
		p := make([]byte, pageSize)
		for i := range p {
			p[i] = f
		}
		return p
	}
	// Legacy v1 baseline snapshot + a new delta.
	v1Key, _, err := rep.Append(ctx, sc, epoch, wal2SnapshotSegment(t, epoch, pageSize, 5, map[uint32][]byte{1: mk(1), 2: mk(2), 3: mk(3), 4: mk(4), 5: mk(5)}))
	if err != nil {
		t.Fatal(err)
	}
	d1Key, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 1, map[uint32][]byte{3: mk(0x31)}))
	if err != nil {
		t.Fatal(err)
	}
	res1, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1})
	if err != nil || !res1.Compacted {
		t.Fatalf("compact1: %+v %v", res1, err)
	}
	man1, _, err := rep.ReadManifest(ctx, sc, epoch)
	if err != nil || len(man1.Objects) == 0 {
		t.Fatalf("manifest1: %+v %v", man1, err)
	}
	oldL1 := man1.Objects[0]
	// The folded L1 is compressed (WAL3) and the index carries frame locations.
	raw1, err := rep.ReadSegment(ctx, oldL1)
	if err != nil {
		t.Fatal(err)
	}
	if _, payload, err := ltx.Decode(raw1); err != nil {
		t.Fatal(err)
	} else {
		locs, lerr := ltx.PageLocs(payload)
		if lerr != nil || len(locs) != 5 {
			t.Fatalf("folded L1 locs = %d, %v", len(locs), lerr)
		}
		compressed := 0
		for _, l := range locs {
			if l.Codec == ltx.CodecLZ4 {
				compressed++
			}
		}
		if compressed == 0 {
			t.Fatal("folded L1 is not compressed")
		}
	}
	idx, ok, err := rep.ReadIndex(ctx, sc, epoch)
	if err != nil || !ok || len(idx.Entries) == 0 || len(idx.Entries[0].Locs) != len(idx.Entries[0].Pages) {
		t.Fatalf("index after compaction = %+v, %v", idx, err)
	}

	// A new delta lands, then compaction folds again and GCs the old L1 + L0.
	d2Key, _, err := rep.Append(ctx, sc, epoch, deltaSegment(t, epoch, pageSize, 5, 2, map[uint32][]byte{5: mk(0x52)}))
	if err != nil {
		t.Fatal(err)
	}
	res2, err := Compact(ctx, rep, sc, epoch, Options{MinSegments: 1, GC: true})
	if err != nil {
		t.Fatalf("compact2: %v", err)
	}
	if res2.DeletedL1 == 0 || res2.DeletedL0 == 0 {
		t.Fatalf("GC did not delete superseded objects: %+v", res2)
	}
	if _, _, err := b.Get(ctx, oldL1); err == nil {
		t.Fatalf("superseded L1 %s still present", oldL1)
	}
	for _, k := range []string{v1Key, d1Key, d2Key} {
		if _, _, err := b.Get(ctx, k); err == nil {
			t.Fatalf("folded L0 %s still present", k)
		}
	}
	// The manifest and page index are overwritten in place, never GC'd.
	if _, _, err := b.Get(ctx, replica.IndexName(sc, epoch)); err != nil {
		t.Fatalf("index.bin missing after GC: %v", err)
	}
	if _, _, err := b.Get(ctx, replica.ManifestName(sc, epoch)); err != nil {
		t.Fatalf("manifest.json missing after GC: %v", err)
	}

	// Paged reads work from the surviving compressed L1 (decompression path).
	pf, err := rep.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("page fetcher after GC: %v", err)
	}
	for pgno, want := range map[uint32]byte{1: 1, 3: 0x31, 5: 0x52} {
		page, err := pf.Page(ctx, pgno)
		if err != nil || page[0] != want {
			t.Fatalf("paged read after GC: page %d = %x, %v; want %x", pgno, page[0], err, want)
		}
	}
	run, err := pf.ReadRun(ctx, 1, 8)
	if err != nil || len(run) == 0 || run[0][0] != 1 {
		t.Fatalf("paged run after GC = %d pages, %v", len(run), err)
	}

	// Full restore from the surviving objects is still byte-exact.
	segs, err := rep.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore after GC: %v", err)
	}
	dest := filepath.Join(dir, "gc-v1.db")
	if _, err := restore.ApplyFile(dest, raws(segs)); err != nil {
		t.Fatalf("apply after GC: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 1 || got[2*int(pageSize)] != 0x31 || got[4*int(pageSize)] != 0x52 {
		t.Fatalf("restored image wrong after GC: p1=%x p3=%x p5=%x", got[0], got[2*int(pageSize)], got[4*int(pageSize)])
	}
}
