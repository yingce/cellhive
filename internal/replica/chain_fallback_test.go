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
	n, _ := m.RetireEpochs(context.Background(), sc, 23, 0)
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
	if n, _ := m.RetireEpochs(ctx, sc, 23, 0); n != 0 {
		t.Fatalf("GC removed %d objects after invalid chain", n)
	}
}
