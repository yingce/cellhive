package compaction

import (
	"path"
	"testing"

	"cellhive/internal/replica"
)

func TestPruneSupersededL9KeepsHighestMax(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "prune-l9"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 9, 1, 5, 2, 1, 2)
	appendNativeAt(t, m, s, 1, 9, 1, 3, 3, 1, 2)

	n, err := PruneSupersededL9(t.Context(), m, s, 1, Options{})
	if err != nil || n != 0 {
		t.Fatalf("without IdleDeleteOK pruned=%d err=%v, want 0", n, err)
	}
	n, err = PruneSupersededL9(t.Context(), m, s, 1, Options{IdleDeleteOK: true})
	if err != nil || n != 2 {
		t.Fatalf("pruned=%d err=%v, want 2", n, err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 1, 9, 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if path.Base(keys[0]) != "0000000000000001-0000000000000005.ltx" {
		t.Fatalf("kept %s, want max=5", keys[0])
	}
}

func TestPruneSupersededL9NoopWhenSingle(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "prune-l9-one"
	appendNativeAt(t, m, s, 1, 9, 1, 4, 1, 1, 2)
	n, err := PruneSupersededL9(t.Context(), m, s, 1, Options{IdleDeleteOK: true})
	if err != nil || n != 0 {
		t.Fatalf("pruned=%d err=%v, want 0", n, err)
	}
}

func TestPruneSupersededL9RefusesWhenRetainedCorrupt(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "prune-l9-corrupt"
	good := appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	badKey := replica.EpochPrefix(s, 1) + "0009/0000000000000001-0000000000000005.ltx"
	if _, err := m.B.Put(t.Context(), badKey, []byte("not-an-ltx")); err != nil {
		t.Fatal(err)
	}
	n, err := PruneSupersededL9(t.Context(), m, s, 1, Options{IdleDeleteOK: true})
	if err == nil || n != 0 {
		t.Fatalf("pruned=%d err=%v, want refuse", n, err)
	}
	raw, err := m.ReadSegment(t.Context(), good)
	if err != nil || len(raw) == 0 {
		t.Fatalf("good L9 must remain: %v", err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 1, 9, 0)
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys=%v err=%v, want both L9 kept", keys, err)
	}
}

func TestFoldBaselinePrunesDominatedCurrentL9(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-prune-l9"
	appendNativeAt(t, m, s, 1, 9, 1, 4, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 5, 5, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 6, 6, 3, 1)

	res, err := FoldBaseline(t.Context(), m, s, 3, Options{})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	// Simulate a later lower-watermark L9 landing beside the folded baseline.
	appendNativeAt(t, m, s, 3, 9, 1, 2, 4, 1, 2)
	res, err = FoldBaseline(t.Context(), m, s, 3, Options{})
	if err != nil || res.Skipped != "already compacted" {
		t.Fatalf("second fold=%+v err=%v", res, err)
	}
	if res.PrunedL9 != 0 {
		t.Fatalf("PrunedL9=%d, want 0 without IdleDeleteOK", res.PrunedL9)
	}
	res, err = FoldBaseline(t.Context(), m, s, 3, Options{IdleDeleteOK: true})
	if err != nil || res.Skipped != "already compacted" {
		t.Fatalf("gated prune fold=%+v err=%v", res, err)
	}
	if res.PrunedL9 != 1 {
		t.Fatalf("PrunedL9=%d, want 1", res.PrunedL9)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 3, 9, 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if path.Base(keys[0]) != "0000000000000001-0000000000000004.ltx" {
		t.Fatalf("kept %s, want max=4 baseline", keys[0])
	}
}
