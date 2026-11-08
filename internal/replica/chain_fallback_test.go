package replica

import (
	"bytes"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"context"
	"testing"
)

func overlapFixture(t *testing.T) (*Manager, cell.Scope) {
	t.Helper()
	b, e := bucket.NewFSBucket(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "p0", Class: "__kv__", ID: "overlap"}
	payload, e := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 64)}})
	if e != nil {
		t.Fatal(e)
	}
	for _, h := range []ltx.Header{{Kind: ltx.KindSnapshot, Epoch: 22, StartTxID: 3766, EndTxID: 3766}, {Kind: ltx.KindDelta, Epoch: 22, StartTxID: 3767, EndTxID: 3768}, {Kind: ltx.KindMarker, Epoch: 23, StartTxID: 3769, EndTxID: 3768}, {Kind: ltx.KindDelta, Epoch: 23, StartTxID: 3769, EndTxID: 3769}, {Kind: ltx.KindSnapshot, Epoch: 23, StartTxID: 3771, EndTxID: 3771}, {Kind: ltx.KindDelta, Epoch: 23, StartTxID: 3770, EndTxID: 3772}} {
		if _, _, e = m.Append(context.Background(), sc, h.Epoch, ltx.Encode(h, payload)); e != nil {
			t.Fatal(e)
		}
	}
	if e = m.MarkSnapshotDone(context.Background(), sc, 23, 3771); e != nil {
		t.Fatal(e)
	}
	return m, sc
}
func TestRestoreChainFallsBackAcrossEpochBoundary(t *testing.T) {
	m, sc := overlapFixture(t)
	segs, e := m.RestoreChain(context.Background(), sc, 23)
	if e != nil {
		t.Fatal(e)
	}
	var raws [][]byte
	for _, s := range segs {
		raws = append(raws, s.Raw)
	}
	ix, e := restore.IndexChain(raws)
	if e != nil {
		t.Fatal(e)
	}
	if ix.Watermark != 3772 {
		t.Fatal(ix.Watermark)
	}
}
func TestRetireEpochsPreservesRequiredFallback(t *testing.T) {
	m, sc := overlapFixture(t)
	n, _ := m.RetireEpochsWithOptions(context.Background(), sc, 23, 0, RetireOptions{OfflineQuiescent: true})
	if n != 0 {
		t.Fatalf("deleted %d required predecessor objects", n)
	}
	keys, e := m.ListL0Segments(context.Background(), sc, 22)
	if e != nil || len(keys) == 0 {
		t.Fatalf("old baseline missing: %v", e)
	}
}

func TestRestoreChainFallbackRejectsMissingPredecessorDelta(t *testing.T) {
	m, sc := overlapFixture(t)
	ctx := context.Background()
	if e := m.B.Delete(ctx, Prefix(sc, 22)+"3767-3768.ltx"); e != nil {
		t.Fatal(e)
	}
	if _, e := m.RestoreChain(ctx, sc, 23); e == nil {
		t.Fatal("fallback silently accepted missing transactions")
	}
	if n, _ := m.RetireEpochsWithOptions(ctx, sc, 23, 0, RetireOptions{OfflineQuiescent: true}); n != 0 {
		t.Fatalf("GC removed %d objects after invalid chain", n)
	}
}

// A predecessor manifest above the successor cut is a dead-tail image: restore
// must ignore it and use the L0 chain that reaches the cut, not fail because the
// manifest's objects are unusable.
func TestRestoreChainCutBeforeManifestPrune(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cut-before-prune"}
	put := func(epoch uint64, name string, h ltx.Header) {
		t.Helper()
		if _, _, err := m.Append(ctx, sc, epoch, ltx.Encode(h, []byte("x"))); err != nil {
			t.Fatal(err)
		}
	}
	put(22, "3766.snapshot", ltx.Header{Kind: ltx.KindSnapshot, Epoch: 22, StartTxID: 3766, EndTxID: 3766})
	put(22, "3767-3768.ltx", ltx.Header{Kind: ltx.KindDelta, Epoch: 22, StartTxID: 3767, EndTxID: 3768})
	if err := m.MarkSnapshotDone(ctx, sc, 22, 3766); err != nil {
		t.Fatal(err)
	}
	if err := m.PutManifest(ctx, sc, 22, Manifest{Epoch: 22, MinTxID: 1, MaxTxID: 3770, Objects: []string{"cells/missing/L1/3770.snapshot"}}); err != nil {
		t.Fatal(err)
	}
	put(23, "3769-3769.ltx", ltx.Header{Kind: ltx.KindDelta, Epoch: 23, StartTxID: 3769, EndTxID: 3769})
	if _, err := m.MarkEpoch(ctx, sc, 23, 3768); err != nil {
		t.Fatal(err)
	}
	segs, err := m.RestoreChain(ctx, sc, 23)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("segments = %d, want snapshot + two deltas", len(segs))
	}
	if got, err := m.DurableWatermarkChain(ctx, sc, 23); err != nil || got != 3769 {
		t.Fatalf("watermark = %d, %v; want 3769", got, err)
	}
}

func TestRestoreChainHonorsRetiredEpochCut(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "retired-cut"}
	putSeg(t, b, sc, 1, "5.snapshot", ltx.KindSnapshot, 5, 5)
	putSeg(t, b, sc, 1, "6-7.ltx", ltx.KindDelta, 6, 7)
	putSeg(t, b, sc, 1, "8-9.ltx", ltx.KindDelta, 8, 9) // fenced late tail
	if _, err := m.MarkEpoch(ctx, sc, 2, 7); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkRetired(ctx, sc, 2, "test"); err != nil {
		t.Fatal(err)
	}
	segs, err := m.RestoreChain(ctx, sc, 2)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	watermark := uint64(0)
	for _, seg := range segs {
		if seg.Header.Kind == ltx.KindMarker {
			continue
		}
		if seg.Header.EndTxID > watermark {
			watermark = seg.Header.EndTxID
		}
		if seg.Header.Kind == ltx.KindDelta && seg.Header.StartTxID >= 8 {
			t.Fatalf("retired epoch cut was ignored: included %+v", seg.Header)
		}
	}
	if watermark != 7 {
		t.Fatalf("watermark = %d, want marker-bounded 7", watermark)
	}
}
