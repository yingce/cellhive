package replica

import (
	"context"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestDurableWatermark(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	const epoch = uint64(1)
	put := func(name string, kind ltx.Kind, start, end uint64) {
		seg := ltx.Encode(ltx.Header{Kind: kind, Epoch: epoch, StartTxID: start, EndTxID: end}, []byte("x"))
		if _, err := b.Put(ctx, Prefix(sc, epoch)+name, seg); err != nil {
			t.Fatal(err)
		}
	}
	watermark := func() uint64 {
		d, err := m.DurableWatermark(ctx, sc, epoch)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	if got := watermark(); got != 0 {
		t.Fatalf("empty watermark = %d, want 0", got)
	}
	put("10.snapshot", ltx.KindSnapshot, 10, 10)
	if got := watermark(); got != 0 {
		t.Fatalf("unmarked snapshot watermark = %d, want 0", got)
	}
	if err := m.MarkSnapshotDone(ctx, sc, epoch, 10); err != nil {
		t.Fatal(err)
	}
	if got := watermark(); got != 10 {
		t.Fatalf("snapshot watermark = %d, want 10", got)
	}
	put("11-12.ltx", ltx.KindDelta, 11, 12)
	put("13-14.batch", ltx.KindDelta, 13, 14)
	if got := watermark(); got != 14 {
		t.Fatalf("contiguous watermark = %d, want 14", got)
	}
	// A gap stops the watermark: 16-17 cannot be restored across missing 15.
	put("16-17.ltx", ltx.KindDelta, 16, 17)
	if got := watermark(); got != 14 {
		t.Fatalf("gapped watermark = %d, want 14", got)
	}
	// A newer snapshot re-anchors the chain above the gap.
	put("20.snapshot", ltx.KindSnapshot, 20, 20)
	if err := m.MarkSnapshotDone(ctx, sc, epoch, 20); err != nil {
		t.Fatal(err)
	}
	put("21-22.ltx", ltx.KindDelta, 21, 22)
	if got := watermark(); got != 22 {
		t.Fatalf("re-anchored watermark = %d, want 22", got)
	}
	// The L1 manifest is a baseline too; deltas continue above it.
	if err := m.PutManifest(ctx, sc, epoch, Manifest{Epoch: epoch, MaxTxID: 30, MinTxID: 1}); err != nil {
		t.Fatal(err)
	}
	put("31-32.ltx", ltx.KindDelta, 31, 32)
	if got := watermark(); got != 32 {
		t.Fatalf("manifest watermark = %d, want 32", got)
	}
	// A group-committed batch may carry snapshot parts; the name alone is a
	// range, so the watermark must decode the batch headers.
	batch := [][]byte{
		ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 40, EndTxID: 40, Flags: ltx.SnapshotFlags(0)}, []byte("part0")),
		ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 40, EndTxID: 40, Flags: ltx.SnapshotFlags(1)}, []byte("part1")),
	}
	var raw []byte
	for _, seg := range batch {
		raw = append(raw, seg...)
	}
	if _, err := b.Put(ctx, Prefix(sc, epoch)+"40-40.batch", raw); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkSnapshotDone(ctx, sc, epoch, 40); err != nil {
		t.Fatal(err)
	}
	if got := watermark(); got != 40 {
		t.Fatalf("batched snapshot watermark = %d, want 40", got)
	}

}

func TestSkipPartialSnapshot(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "partial"}
	const epoch = uint64(1)
	cache := map[uint64]bool{}

	// A snapshot that started but never finished must be skipped.
	if err := m.MarkSnapshotStarted(ctx, sc, epoch, 10); err != nil {
		t.Fatal(err)
	}
	if skip, err := m.skipPartialSnapshot(ctx, sc, epoch, 10, cache); err != nil || !skip {
		t.Fatalf("partial skip = %v, %v; want true", skip, err)
	}
	// Completing it makes it selectable.
	if err := m.MarkSnapshotDone(ctx, sc, epoch, 10); err != nil {
		t.Fatal(err)
	}
	cache = map[uint64]bool{}
	if skip, err := m.skipPartialSnapshot(ctx, sc, epoch, 10, cache); err != nil || skip {
		t.Fatalf("complete skip = %v, %v; want false", skip, err)
	}
	// A legacy snapshot (no markers at all) is assumed complete.
	cache = map[uint64]bool{}
	if skip, err := m.skipPartialSnapshot(ctx, sc, epoch, 20, cache); err != nil || skip {
		t.Fatalf("legacy skip = %v, %v; want false", skip, err)
	}
}

func TestDurableWatermarkChainAcrossEpochs(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "chain"}
	put := func(epoch uint64, name string, kind ltx.Kind, start, end uint64) {
		seg := ltx.Encode(ltx.Header{Kind: kind, Epoch: epoch, StartTxID: start, EndTxID: end}, []byte("x"))
		if _, err := b.Put(ctx, Prefix(sc, epoch)+name, seg); err != nil {
			t.Fatal(err)
		}
	}
	put(1, "10.snapshot", ltx.KindSnapshot, 10, 10)
	if err := m.MarkSnapshotDone(ctx, sc, 1, 10); err != nil {
		t.Fatal(err)
	}
	put(1, "11-12.ltx", ltx.KindDelta, 11, 12)
	put(2, "13-14.ltx", ltx.KindDelta, 13, 14)
	if got, err := m.DurableWatermarkChain(ctx, sc, 2); err != nil || got != 14 {
		t.Fatalf("chain watermark = %d, %v; want 14", got, err)
	}
	// RestoreChain must include both epochs so the new epoch's deltas have a
	// snapshot base.
	segs, err := m.RestoreChain(ctx, sc, 2)
	if err != nil {
		t.Fatal(err)
	}
	var epochsSeen = map[uint64]bool{}
	for _, sg := range segs {
		epochsSeen[sg.Header.Epoch] = true
	}
	if !epochsSeen[1] || !epochsSeen[2] {
		t.Fatalf("restore chain epochs = %v, want 1 and 2", epochsSeen)
	}
}
