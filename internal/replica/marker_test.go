package replica

import (
	"context"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func putSeg(t *testing.T, b bucket.Bucket, sc cell.Scope, epoch uint64, name string, kind ltx.Kind, start, end uint64) {
	t.Helper()
	seg := ltx.Encode(ltx.Header{Kind: kind, Epoch: epoch, StartTxID: start, EndTxID: end}, []byte("x"))
	if _, err := b.Put(context.Background(), Prefix(sc, epoch)+name, seg); err != nil {
		t.Fatal(err)
	}
}

// The marker written by MarkEpoch has to come back out of RestoreChain, or
// restore.selectChain never sees it and cannot cut a fenced owner's late tail.
func TestMarkerReachesRestoreChain(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "marker"}
	putSeg(t, b, sc, 1, "5.snapshot", ltx.KindSnapshot, 5, 5)
	if err := m.MarkSnapshotDone(ctx, sc, 1, 5); err != nil {
		t.Fatal(err)
	}
	putSeg(t, b, sc, 1, "6-7.ltx", ltx.KindDelta, 6, 7)
	if _, err := m.MarkEpoch(ctx, sc, 2, 7); err != nil {
		t.Fatal(err)
	}
	putSeg(t, b, sc, 1, "8-9.ltx", ltx.KindDelta, 8, 9) // fenced old owner, late
	putSeg(t, b, sc, 2, "8-8.ltx", ltx.KindDelta, 8, 8)

	segs, err := m.RestoreChain(ctx, sc, 2)
	if err != nil {
		t.Fatal(err)
	}
	var marker bool
	for _, sg := range segs {
		if sg.Header.Kind == ltx.KindMarker {
			marker = true
			if sg.Header.Epoch != 2 || sg.Header.StartTxID != 8 || sg.Header.EndTxID != 7 {
				t.Fatalf("marker header = %+v", sg.Header)
			}
		}
	}
	if !marker {
		t.Fatalf("RestoreChain dropped the epoch marker; got %d segments", len(segs))
	}
}

// A fenced owner's tail above the cut must not extend the durable watermark.
func TestDurableWatermarkChainIgnoresFencedTail(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "wm"}
	putSeg(t, b, sc, 1, "5.snapshot", ltx.KindSnapshot, 5, 5)
	if err := m.MarkSnapshotDone(ctx, sc, 1, 5); err != nil {
		t.Fatal(err)
	}
	putSeg(t, b, sc, 1, "6-7.ltx", ltx.KindDelta, 6, 7)
	putSeg(t, b, sc, 1, "8-9.ltx", ltx.KindDelta, 8, 9) // late, above the cut
	if got, err := m.DurableWatermarkChain(ctx, sc, 2); err != nil || got != 9 {
		t.Fatalf("before marker: watermark = %d, %v; want 9", got, err)
	}
	if _, err := m.MarkEpoch(ctx, sc, 2, 7); err != nil {
		t.Fatal(err)
	}
	if got, err := m.DurableWatermarkChain(ctx, sc, 2); err != nil || got != 7 {
		t.Fatalf("after marker: watermark = %d, %v; want 7 (fenced tail ignored)", got, err)
	}
	putSeg(t, b, sc, 2, "8-8.ltx", ltx.KindDelta, 8, 8)
	if got, err := m.DurableWatermarkChain(ctx, sc, 2); err != nil || got != 8 {
		t.Fatalf("with new-epoch delta: watermark = %d, %v; want 8", got, err)
	}
}

// ".d.batch" and ".s.batch" names carry their kind, so the watermark must not
// download them; legacy and ".m.batch" names still have to be decoded.
func TestDurableWatermarkSkipsDownloadForSpanNamedBatch(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cb := &countingBucket{Bucket: fs}
	m := New(cb)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "span"}
	putSeg(t, fs, sc, 1, "5.snapshot", ltx.KindSnapshot, 5, 5)
	if err := m.MarkSnapshotDone(ctx, sc, 1, 5); err != nil {
		t.Fatal(err)
	}
	// Baseline: the Gets a watermark needs with no batch present (manifest and
	// snapshot-done lookups).
	b0, _, _, _ := cb.stats()
	if _, err := m.DurableWatermark(ctx, sc, 1); err != nil {
		t.Fatal(err)
	}
	b1, _, _, _ := cb.stats()
	base := b1 - b0

	two := append(
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 6, EndTxID: 6}, []byte("a")),
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 7, EndTxID: 7}, []byte("b"))...)
	if _, err := fs.Put(ctx, Prefix(sc, 1)+"6-7.d.batch", two); err != nil {
		t.Fatal(err)
	}
	before, _, _, _ := cb.stats()
	got, err := m.DurableWatermark(ctx, sc, 1)
	if err != nil || got != 7 {
		t.Fatalf("watermark = %d, %v; want 7", got, err)
	}
	after, _, _, _ := cb.stats()
	if after-before != base {
		t.Fatalf("watermark issued %d Gets, want %d: a span-named .d.batch must not be downloaded", after-before, base)
	}

	legacy := append(
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 8, EndTxID: 8}, []byte("c")),
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 9, EndTxID: 9}, []byte("d"))...)
	if _, err := fs.Put(ctx, Prefix(sc, 1)+"8-9.batch", legacy); err != nil {
		t.Fatal(err)
	}
	if got, err := m.DurableWatermark(ctx, sc, 1); err != nil || got != 9 {
		t.Fatalf("with legacy batch: watermark = %d, %v; want 9", got, err)
	}
}

func TestEpochsIgnoresRetiredEmptyDirs(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gc"}
	putSeg(t, b, sc, 1, "5.snapshot", ltx.KindSnapshot, 5, 5)
	putSeg(t, b, sc, 2, "6-6.ltx", ltx.KindDelta, 6, 6)
	// Retire epoch 1 the way RetireEpochs does: delete its objects.
	keys, _ := b.List(ctx, Prefix(sc, 1))
	for _, k := range keys {
		if err := b.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	got, err := m.Epochs(ctx, sc, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("Epochs after retiring e1 = %v, want [2]", got)
	}
}
