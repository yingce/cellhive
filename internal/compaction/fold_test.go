package compaction

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func appendNativeAt(t *testing.T, m *replica.Manager, s cell.Scope, epoch uint64, level uint16, min, max uint64, page byte, numbers ...uint32) string {
	t.Helper()
	pages := make([]ltx.WALPage, len(numbers))
	for i, pg := range numbers {
		pages[i] = ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{page}, 512)}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: min, MaxTXID: max}, pages)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendLevel(t.Context(), s, epoch, level, raw)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestFoldBaselineNewestFirstKeepsRestoreAfterEachChunk(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-newest-first"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	appendNativeAt(t, m, s, 2, 0, 4, 4, 3, 1)
	appendNativeAt(t, m, s, 3, 0, 5, 5, 4, 1)

	assertRestoreTo5 := func(t *testing.T) {
		t.Helper()
		if _, err := m.RestoreChainKeys(t.Context(), s, 3); err != nil {
			t.Fatalf("restore chain broken mid-fold: %v", err)
		}
		wm, err := m.DurableWatermarkChain(t.Context(), s, 3)
		if err != nil || wm != 5 {
			t.Fatalf("watermark=%d err=%v, want 5", wm, err)
		}
	}
	assertRestoreTo5(t)

	chunks := 0
	_, err := FoldBaseline(t.Context(), m, s, 3, Options{
		MaxInputs: 1,
		AfterChunk: func(r Result) error {
			chunks++
			if r.MinTxID == 0 || r.MaxTxID == 0 {
				t.Fatalf("chunk result missing range: %+v", r)
			}
			assertRestoreTo5(t)
			if chunks == 1 {
				return errors.New("inject crash after newest L2")
			}
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "inject crash") {
		t.Fatalf("expected injected crash, got %v", err)
	}
	if chunks != 1 {
		t.Fatalf("chunks=%d, want 1 before crash", chunks)
	}
	assertRestoreTo5(t)

	res, err := FoldBaseline(t.Context(), m, s, 3, Options{MaxInputs: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("resume fold=%+v err=%v", res, err)
	}
	if ok, err := epochSelfContained(t.Context(), m, s, 3); err != nil || !ok {
		t.Fatalf("self-contained=%v err=%v", ok, err)
	}
	assertRestoreTo5(t)
}

func TestFoldBaselineOverlappingPredecessorKeepsRestore(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-overlap"
	// The predecessor delta straddles the snapshot max: [3,10] vs L9[1,5].
	// Publishing it verbatim as a current-epoch L2 makes the chain start at 3
	// while the L9 (covering 1..5) has not been copied yet, so restore breaks.
	appendNativeAt(t, m, s, 1, 9, 1, 5, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 0, 3, 10, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 11, 11, 3, 1)

	assertRestore := func(t *testing.T) {
		t.Helper()
		wm, err := m.DurableWatermarkChain(t.Context(), s, 3)
		if err != nil || wm != 11 {
			t.Fatalf("watermark=%d err=%v, want 11", wm, err)
		}
	}
	assertRestore(t)

	chunks := 0
	res, err := FoldBaseline(t.Context(), m, s, 3, Options{
		MaxInputs: 1,
		AfterChunk: func(r Result) error {
			chunks++
			assertRestore(t)
			return nil
		},
	})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	if chunks == 0 {
		t.Fatal("fold published no L2 chunks")
	}
	if ok, err := epochSelfContained(t.Context(), m, s, 3); err != nil || !ok {
		t.Fatalf("self-contained=%v err=%v", ok, err)
	}
	assertRestore(t)
}

func TestFoldBaselineClipsChunkOverlapToThePreviousWatermark(t *testing.T) {
	old := spoolSpaceFn
	spoolSpaceFn = func(int64) error { return nil }
	t.Cleanup(func() { spoolSpaceFn = old })
	m, s := nativeManager(t)
	s.ID = "fold-chunk-overlap"
	// L9 [1,5], deltas [6,10] and [9,15], continuation [16,16]. A one-file
	// budget publishes [9,15] before [6,10]. Leaving it starting at 9 makes the
	// new epoch require a predecessor that ends at 8, which this chain cannot
	// satisfy. The chunk must start at 11, just after [6,10].
	appendNativeAt(t, m, s, 1, 9, 1, 5, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 0, 6, 10, 2, 1)
	appendNativeAt(t, m, s, 1, 0, 9, 15, 3, 1)
	appendNativeAt(t, m, s, 2, 0, 16, 16, 4, 1)

	var mins []uint64
	res, err := FoldBaseline(t.Context(), m, s, 2, Options{
		MaxInputs: 1,
		AfterChunk: func(r Result) error {
			mins = append(mins, r.MinTxID)
			if _, cerr := m.RestoreChainKeys(t.Context(), s, 2); cerr != nil {
				return cerr
			}
			wm, werr := m.DurableWatermarkChain(t.Context(), s, 2)
			if werr != nil || wm != 16 {
				t.Fatalf("watermark=%d err=%v, want 16", wm, werr)
			}
			return nil
		},
	})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v mins=%v", res, err, mins)
	}
	clipped := false
	for _, min := range mins {
		if min == 9 {
			t.Fatalf("published the overlap from tx 9: %v", mins)
		}
		if min == 11 {
			clipped = true
		}
	}
	if !clipped {
		t.Fatalf("mins=%v, want a chunk starting at 11", mins)
	}
	if ok, err := epochSelfContained(t.Context(), m, s, 2); err != nil || !ok {
		t.Fatalf("self-contained=%v err=%v", ok, err)
	}
}

func TestFoldBaselineOverlapRejectedWhenNotTrimmable(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-overlap-untrimmable"
	// A delta that starts inside the snapshot but ends before its max cannot be
	// trimmed into a post-snapshot range; the fold must refuse instead of
	// publishing a hole.
	appendNativeAt(t, m, s, 1, 9, 1, 10, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 0, 4, 8, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 11, 11, 3, 1)

	res, err := FoldBaseline(t.Context(), m, s, 3, Options{})
	if err == nil && !res.Compacted && res.Skipped == "" {
		t.Fatalf("fold reported neither success nor a skip: %+v", res)
	}
	wm, werr := m.DurableWatermarkChain(t.Context(), s, 3)
	if werr != nil || wm != 11 {
		t.Fatalf("restore broken after fold: watermark=%d err=%v", wm, werr)
	}
}

func TestFoldBaselineChecksSpoolBeforeCopyingL9(t *testing.T) {
	// A large L9 must not be downloaded before the spool budget is checked, and
	// the copied byte count must match the size the check used.
	old := spoolSpaceFn
	t.Cleanup(func() { spoolSpaceFn = old })

	var sized []int64
	spoolSpaceFn = func(n int64) error {
		sized = append(sized, n)
		if len(sized) == 1 {
			return errors.New("insufficient temporary disk")
		}
		return nil
	}

	m, s := nativeManager(t)
	s.ID = "fold-spool-order"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)

	if _, err := FoldBaseline(t.Context(), m, s, 2, Options{}); err == nil {
		t.Fatal("expected the spool budget check to stop the fold")
	}
	if len(sized) == 0 {
		t.Fatal("spool budget was never consulted")
	}
	if sized[0] <= 0 {
		t.Fatalf("object size was not passed to the spool check: %v", sized)
	}
}

func TestFoldBaselineCopiesExactL9Bytes(t *testing.T) {
	old := spoolSpaceFn
	t.Cleanup(func() { spoolSpaceFn = old })
	var sized []int64
	spoolSpaceFn = func(n int64) error { sized = append(sized, n); return nil }

	m, s := nativeManager(t)
	s.ID = "fold-spool-exact"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)

	if _, err := FoldBaseline(t.Context(), m, s, 2, Options{}); err != nil {
		t.Fatal(err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("copied L9 missing: %v %v", keys, err)
	}
	raw, err := m.ReadSegment(t.Context(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	last := sized[len(sized)-1]
	if last != int64(len(raw)) {
		t.Fatalf("spool check sized %d, copied object is %d bytes", last, len(raw))
	}
}

func TestFoldBaselineAbortsWhenVerifyFails(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-verify"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 4, 4, 3, 1)
	_, err := FoldBaseline(t.Context(), m, s, 3, Options{
		Verify: func(context.Context) error { return errors.New("epoch moved") },
	})
	if err == nil {
		t.Fatal("expected verify failure")
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 3, 9, 0)
	if err != nil || len(l9) != 0 {
		t.Fatalf("partial L9 must not land after verify abort: %v %v", l9, err)
	}
}

func TestFoldBaselineResumesAfterPostSnapshotCoverage(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-resume"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 4, 4, 3, 1)
	// Simulate crash after L2 landed but before L9 copy: current has coverage
	// after the old snapshot max, base still e1.
	appendNativeAt(t, m, s, 3, 2, 3, 4, 4, 1)
	if ok, err := epochSelfContained(t.Context(), m, s, 3); err != nil || ok {
		t.Fatalf("precondition self-contained=%v err=%v", ok, err)
	}
	res, err := FoldBaseline(t.Context(), m, s, 3, Options{})
	if err != nil || !res.Compacted {
		t.Fatalf("resume fold=%+v err=%v", res, err)
	}
	if ok, err := epochSelfContained(t.Context(), m, s, 3); err != nil || !ok {
		t.Fatalf("post fold self-contained=%v err=%v", ok, err)
	}
}

func TestFoldBaselineRehomesL9AndEnablesPredecessorGC(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	s := cell.Scope{Namespace: "fold", Class: "__kv__", ID: "rehome"}
	// e1 holds the only MinTXID=1 baseline; e2/e3 continue with deltas.
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)
	appendNativeAt(t, m, s, 3, 0, 4, 4, 3, 1)

	before, err := m.RestoreChainKeys(t.Context(), s, 3)
	if err != nil || len(before) < 2 {
		t.Fatalf("pre-fold chain=%v err=%v", before, err)
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 3, 9, 0)
	if err != nil || len(l9) != 0 {
		t.Fatalf("current must start without L9: %v %v", l9, err)
	}

	res, err := FoldBaseline(t.Context(), m, s, 3, Options{})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	l9, err = m.ListLevelFrom(t.Context(), s, 3, 9, 0)
	if err != nil || len(l9) == 0 {
		t.Fatalf("current L9 missing: %v %v", l9, err)
	}
	l2, err := m.ListLevelFrom(t.Context(), s, 3, 2, 0)
	if err != nil || len(l2) == 0 {
		t.Fatalf("post-snapshot tail missing from L2: %v %v", l2, err)
	}
	l3, err := m.ListLevelFrom(t.Context(), s, 3, 3, 0)
	if err != nil || len(l3) != 0 {
		t.Fatalf("fold published L3: %v %v", l3, err)
	}
	keys, err := m.RestoreChainKeys(t.Context(), s, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.Contains(key, "/ltx/e3/") {
			t.Fatalf("chain still depends on older epoch: %s", key)
		}
	}

	n, err := m.RetireEpochsWithOptions(t.Context(), s, 3, 0, replica.RetireOptions{
		OfflineQuiescent: true,
		Now:              func() time.Time { return time.Now().Add(2 * time.Hour) },
	})
	if err != nil || n == 0 {
		t.Fatalf("predecessor GC after fold deleted=%d err=%v", n, err)
	}
	left, err := b.List(t.Context(), replica.EpochPrefix(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("e1 should be gone after rehome+GC: %v", left)
	}
}

func TestFoldBaselineCopiesExistingL9UnderATinyBudget(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-copy-ignores-budget"
	appendNativeAt(t, m, s, 1, 9, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)

	res, err := FoldBaseline(t.Context(), m, s, 2, Options{SnapshotMaxBytes: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(l9) != 1 {
		t.Fatalf("existing L9 must be copied: %v %v", l9, err)
	}
	l2, err := m.ListLevelFrom(t.Context(), s, 2, 2, 0)
	if err != nil || len(l2) == 0 {
		t.Fatalf("increment missing from L2: %v %v", l2, err)
	}
	l3, err := m.ListLevelFrom(t.Context(), s, 2, 3, 0)
	if err != nil || len(l3) != 0 {
		t.Fatalf("fold published L3: %v %v", l3, err)
	}
}

func TestFoldBaselinePublishesL9AndL2WhenNewBaselineFits(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-l9-when-fit"
	appendNativeAt(t, m, s, 1, 1, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)

	res, err := FoldBaseline(t.Context(), m, s, 2, Options{})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(l9) != 1 {
		t.Fatalf("fitting baseline must be L9: %v %v", l9, err)
	}
	l2, err := m.ListLevelFrom(t.Context(), s, 2, 2, 0)
	if err != nil || len(l2) == 0 {
		t.Fatalf("increment missing from L2: %v %v", l2, err)
	}
	l3, err := m.ListLevelFrom(t.Context(), s, 2, 3, 0)
	if err != nil || len(l3) != 0 {
		t.Fatalf("fold published L3: %v %v", l3, err)
	}
}

func TestFoldBaselinePublishesL2WhenNewL9ExceedsBudget(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "fold-l2-over-budget"
	appendNativeAt(t, m, s, 1, 1, 1, 2, 1, 1, 2)
	appendNativeAt(t, m, s, 2, 0, 3, 3, 2, 1)

	res, err := FoldBaseline(t.Context(), m, s, 2, Options{SnapshotMaxBytes: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	l9, err := m.ListLevelFrom(t.Context(), s, 2, 9, 0)
	if err != nil || len(l9) != 0 {
		t.Fatalf("over-budget fold published L9: %v %v", l9, err)
	}
	l2, err := m.ListLevelFrom(t.Context(), s, 2, 2, 0)
	if err != nil || len(l2) == 0 {
		t.Fatalf("over-budget baseline missing from L2: %v %v", l2, err)
	}
	l3, err := m.ListLevelFrom(t.Context(), s, 2, 3, 0)
	if err != nil || len(l3) != 0 {
		t.Fatalf("fold published L3: %v %v", l3, err)
	}
	if ok, err := epochSelfContained(t.Context(), m, s, 2); err != nil || !ok {
		t.Fatalf("self-contained=%v err=%v", ok, err)
	}
	wm, err := m.DurableWatermarkChain(t.Context(), s, 2)
	if err != nil || wm != 3 {
		t.Fatalf("watermark=%d err=%v, want 3", wm, err)
	}
}
