package compaction

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/replica"
)

func TestFoldEpochWaitsForCurrentOpener(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	r, err := FoldEpoch(t.Context(), m, s, 2, Options{})
	if err != nil || r.Skipped != "waiting for current epoch" {
		t.Fatalf("%+v %v", r, err)
	}
	epochs, err := m.Epochs(t.Context(), s, 0)
	if err != nil || len(epochs) != 1 || epochs[0] != 1 {
		t.Fatalf("published before opener: %v %v", epochs, err)
	}
}

func TestFoldEpochFullSnapshotFixedCutAndNoRefresh(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	calls := 0
	r, err := FoldEpoch(t.Context(), m, s, 2, Options{Verify: func(context.Context) error {
		calls++
		if calls == 2 {
			appendNativeAt(t, m, s, 2, 0, 4, 4, 3, 1)
		}
		return nil
	}})
	if err != nil || !r.Compacted || r.MinTxID != 1 || r.MaxTxID != 3 {
		t.Fatalf("%+v %v", r, err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(keys) != 1 || !strings.Contains(keys[0], "0000000000000001-0000000000000003") {
		t.Fatalf("%v %v", keys, err)
	}
	l2, err := m.ListLevelFrom(t.Context(), s, 2, 2, 0)
	if err != nil || len(l2) != 0 {
		t.Fatalf("small chain unexpectedly emitted L2: %v %v", l2, err)
	}
	if wm, err := m.DurableWatermarkChain(t.Context(), s, 2); err != nil || wm != 4 {
		t.Fatalf("lost concurrent tail: %d %v", wm, err)
	}
	// A new manager models a restart: completion is recognized from the chain.
	r, err = FoldEpoch(t.Context(), replica.New(m.B), s, 2, Options{})
	if err != nil || r.Compacted || r.Skipped != "already compacted" {
		t.Fatalf("refreshed same epoch: %+v %v", r, err)
	}
}

func TestFoldEpochLargeMultiEpochResume(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 4, 4, 3, 1)
	stop := errors.New("stop after newest chunk")
	_, err := FoldEpoch(t.Context(), m, s, 3, Options{SnapshotMaxBytes: 1, MaxInputs: 1, AfterChunk: func(Result) error { return stop }})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if wm, err := m.DurableWatermarkChain(t.Context(), s, 3); err != nil || wm != 4 {
		t.Fatalf("broken partial chain: %d %v", wm, err)
	}
	// Lose ownership before finishing e3; e4 must recover across the partial fold.
	appendNativeAt(t, m, s, 4, 0, 5, 5, 4, 2)
	r, err := FoldEpoch(t.Context(), m, s, 4, Options{SnapshotMaxBytes: 1, MaxInputs: 1})
	if err != nil || !r.Compacted || r.MaxTxID != 5 {
		t.Fatalf("%+v %v", r, err)
	}
	keys, err := m.RestoreChainKeys(t.Context(), s, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !strings.Contains(k, "/e4/") {
			t.Fatalf("predecessor still used: %s", k)
		}
	}
	if _, err := m.RestoreChain(t.Context(), s, 4); err != nil {
		t.Fatal(err)
	}
}

func TestFoldEpochStopsOnLostOwner(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	lost := errors.New("lost owner")
	calls := 0
	_, err := FoldEpoch(t.Context(), m, s, 2, Options{Verify: func(context.Context) error {
		calls++
		if calls > 1 {
			return lost
		}
		return nil
	}})
	if !errors.Is(err, lost) {
		t.Fatal(err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(keys) != 0 {
		t.Fatalf("published after loss: %v %v", keys, err)
	}
}

func TestFoldEpochKeepsPinnedPredecessorReader(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	old, err := m.NewPageFetcher(t.Context(), s, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	if _, err = FoldEpoch(t.Context(), m, s, 2, Options{}); err != nil {
		t.Fatal(err)
	}
	page, err := old.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{1}, 512)) {
		t.Fatalf("old reader changed: %v", err)
	}
	current, err := m.NewPageFetcher(t.Context(), s, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	page, err = current.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{2}, 512)) {
		t.Fatalf("new reader incorrect: %v", err)
	}
}

func TestFoldEpochLargeWithoutL9UsesL2Baseline(t *testing.T) {
	m, s := nativeManager(t)
	appendNativeAt(t, m, s, 1, 1, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 2)
	r, err := FoldEpoch(t.Context(), m, s, 2, Options{SnapshotMaxBytes: 1})
	if err != nil || !r.Compacted {
		t.Fatalf("%+v %v", r, err)
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(l9) != 0 {
		t.Fatalf("unexpected L9: %v %v", l9, err)
	}
	chain, err := m.RestoreChainKeys(t.Context(), s, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range chain {
		if !strings.Contains(k, "/e2/0002/") {
			t.Fatalf("not an independent L2 chain: %v", chain)
		}
	}
	fresh, err := m.NewPageFetcher(t.Context(), s, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	for pg, want := range map[uint32]byte{1: 1, 2: 2} {
		got, err := fresh.Page(t.Context(), pg)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{want}, 512)) {
			t.Fatalf("page %d: %v", pg, err)
		}
	}
}
