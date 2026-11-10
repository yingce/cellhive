package replica

import (
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"testing"
)

func overlapFixture(t *testing.T) (*Manager, cell.Scope) {
	t.Helper()
	m := newManager(t)
	sc := cell.Scope{Namespace: "p0", Class: "__kv__", ID: "overlap"}
	putNativeEpochFile(t, m, sc, 22, 9, 1, 3766, false)
	putNativeEpochFile(t, m, sc, 22, 0, 3767, 3768, false)
	if err := m.AppendContinuation(t.Context(), sc, 23, ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 3769, MaxTXID: 3769, WALOffset: 32}); err != nil {
		t.Fatal(err)
	}
	putNativeEpochFile(t, m, sc, 23, 0, 3770, 3770, false)
	putNativeEpochFile(t, m, sc, 23, 1, 3770, 3772, false)
	return m, sc
}
func TestRestoreChainFallsBackAcrossEpochBoundary(t *testing.T) {
	m, sc := overlapFixture(t)
	segs, err := m.RestoreChain(t.Context(), sc, 23)
	if err != nil {
		t.Fatal(err)
	}
	var raws [][]byte
	for _, s := range segs {
		raws = append(raws, s.Raw)
	}
	ix, err := restore.IndexChain(raws)
	if err != nil || ix.Watermark != 3772 {
		t.Fatalf("restore=%+v %v", ix, err)
	}
}
func TestRetireEpochsPreservesRequiredFallback(t *testing.T) {
	m, sc := overlapFixture(t)
	n, err := m.RetireEpochsWithOptions(t.Context(), sc, 23, 0, RetireOptions{OfflineQuiescent: true, Grace: -1})
	if err != nil || n != 0 {
		t.Fatalf("retired required predecessor=%d %v", n, err)
	}
	keys, err := m.ListL0Segments(t.Context(), sc, 22)
	if err != nil || len(keys) == 0 {
		t.Fatalf("missing predecessor %v", err)
	}
}
func TestRestoreChainFallbackRejectsMissingPredecessorDelta(t *testing.T) {
	m, sc := overlapFixture(t)
	if err := m.resourceObjects().Delete(t.Context(), EpochPrefix(sc, 22)+"0000/0000000000000eb7-0000000000000eb8.ltx"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RestoreChain(t.Context(), sc, 23); err == nil {
		t.Fatal("missing cut accepted")
	}
	if n, err := m.RetireEpochsWithOptions(t.Context(), sc, 23, 0, RetireOptions{OfflineQuiescent: true, Grace: -1}); err == nil || n != 0 {
		t.Fatalf("invalid-chain GC %d %v", n, err)
	}
}
func TestNativeRestoreClipsHigherLevelDeadTailBeforePlanning(t *testing.T) {
	m, sc := overlapFixture(t)
	putNativeEpochFile(t, m, sc, 22, 9, 1, 3770, false)
	segs, err := m.RestoreChain(t.Context(), sc, 23)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if seg.Header.Epoch == 22 && seg.Header.EndTxID > 3768 {
			t.Fatalf("late snapshot selected: %s", seg.Key)
		}
	}
	if wm, err := m.DurableWatermarkChain(t.Context(), sc, 23); err != nil || wm != 3772 {
		t.Fatalf("watermark=%d %v", wm, err)
	}
}
