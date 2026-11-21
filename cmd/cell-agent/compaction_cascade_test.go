package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/replica"
)

func TestCompactionNudgeKeepsEachEpoch(t *testing.T) {
	w := newCompactionWake()
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "cell"}
	w.Nudge(sc, 2)
	w.Nudge(sc, 2)
	w.Nudge(sc, 1)
	got := w.take()
	seen := map[uint64]int{}
	for _, n := range got {
		if n.scope != sc {
			t.Fatalf("scope=%s", n.scope)
		}
		seen[n.epoch]++
	}
	if len(got) != 2 {
		t.Fatalf("nudges=%d, want both epochs", len(got))
	}
	if seen[1] != 1 || seen[2] != 1 {
		t.Fatalf("epochs=%v", seen)
	}
}

func TestNonpositiveObjectCountUses256(t *testing.T) {
	if uploadSegmentCap(config.Config{}) != 256 {
		t.Fatal("upload cap must use 256")
	}
	p := promotePolicy(config.Config{})
	if p.MinObjects != 256 {
		t.Fatalf("L1 object bar=%d", p.MinObjects)
	}
	if !compaction.ShouldPromote(compaction.Cue{Objects: 256}, p) {
		t.Fatal("256 small objects must promote")
	}
	if compaction.ShouldPromote(compaction.Cue{Objects: 255, Bytes: 1}, p) {
		t.Fatal("255 objects under the byte bar must wait")
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "idle", Class: "__kv__", ID: "zero"}
	appendSegment(t, m, sc, 1, snapshotSegment(t, 1, 512, 1, map[uint32][]byte{1: bytes.Repeat([]byte{3}, 512)}))
	if idleL0Ready(m, sc, 1, 0) {
		t.Fatal("nonpositive idle bar must stay at 256")
	}
}

func TestUploadSegmentCapUsesTheL1ObjectCount(t *testing.T) {
	if uploadSegmentCap(config.Config{CompactionMinObjects: 8}) != 8 {
		t.Fatal("upload cap must follow the L1 object count")
	}
	if uploadSegmentCap(config.Config{}) != 256 {
		t.Fatal("nonpositive count must use 256")
	}
}

func TestIdleFoldUsesOwnerL0Tally(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "idle", Class: "__kv__", ID: "one"}
	if idleL0Ready(m, sc, 1, 256) {
		t.Fatal("empty tally must wait")
	}
	appendSegment(t, m, sc, 1, snapshotSegment(t, 1, 512, 1, map[uint32][]byte{1: bytes.Repeat([]byte{3}, 512)}))
	if idleL0Ready(m, sc, 1, 256) {
		t.Fatal("one L0 file must wait")
	}
	if !idleL0Ready(m, sc, 1, 1) {
		t.Fatal("one L0 file meets a one-file bar")
	}
}

func TestCompactScopeCascadeSkipsASmallTail(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, _ compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		return compaction.Result{Compacted: true}, nil
	}
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		if level == 1 {
			return compaction.Cue{Objects: 1, Segments: 1}, nil
		}
		return compaction.Cue{}, nil
	}
	cfg := config.Config{CompactionMinObjects: 256, CompactionMinBytes: 64 << 20}
	if _, err := compactScopeCascade(context.Background(), time.Second, cfg, cue, nil, compact); err != nil {
		t.Fatal(err)
	}
	if len(levels) != 0 {
		t.Fatalf("levels=%v, want none", levels)
	}
}

func TestCompactScopeCascadePromotesL1WithoutPullingASingleL2(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, _ compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		return compaction.Result{Compacted: true}, nil
	}
	l1Seen := 0
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		switch level {
		case 1:
			l1Seen++
			if l1Seen == 1 {
				return compaction.Cue{Objects: 1, Bytes: 64 << 20}, nil
			}
			return compaction.Cue{Objects: 1, Segments: 1}, nil
		default:
			t.Fatalf("level %d", level)
			return compaction.Cue{}, nil
		}
	}
	cfg := config.Config{CompactionMinObjects: 256, CompactionMinBytes: 64 << 20}
	if _, err := compactScopeCascade(context.Background(), time.Second, cfg, cue, nil, compact); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(levels); got != "[1]" {
		t.Fatalf("levels=%s, want [1]", got)
	}
}

func TestCompactScopeCascadeLeavesL1InPlace(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, opts compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		if opts.MaxInputBytes != -1 {
			t.Fatalf("budget=%d", opts.MaxInputBytes)
		}
		return compaction.Result{Compacted: true}, nil
	}
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		if level != 1 {
			t.Fatalf("level %d", level)
		}
		return compaction.Cue{}, nil
	}
	if _, err := compactScopeCascade(context.Background(), time.Second, config.Config{CompactionMinObjects: 256, CompactionMinBytes: 64 << 20}, cue, nil, compact); err != nil {
		t.Fatal(err)
	}
	if len(levels) != 0 {
		t.Fatalf("levels=%v, L1 must stay", levels)
	}
}

func TestCompactScopeCascadeLeavesOneFullL1(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, _ compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		return compaction.Result{Compacted: true}, nil
	}
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		if level != 1 {
			t.Fatalf("level %d", level)
		}
		return compaction.Cue{Objects: 255, Bytes: (64 << 20) - 1}, nil
	}
	cfg := config.Config{CompactionMinObjects: 256, CompactionMinBytes: 64 << 20}
	if _, err := compactScopeCascade(context.Background(), time.Second, cfg, cue, nil, compact); err != nil {
		t.Fatal(err)
	}
	if len(levels) != 0 {
		t.Fatalf("levels=%v, 255 objects under 64MiB must stay", levels)
	}
}

func TestCompactWhileStopsWhenTheCueIsEmpty(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, _ compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		return compaction.Result{Compacted: true}, nil
	}
	l1Seen := 0
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		if level != 1 {
			return compaction.Cue{}, nil
		}
		l1Seen++
		if l1Seen == 1 {
			return compaction.Cue{Objects: 1, Segments: 1}, nil
		}
		return compaction.Cue{}, nil
	}
	due := func(c compaction.Cue) bool {
		return c.Objects > 0 || c.Segments > 0 || c.Bytes > 0
	}
	if _, err := compactWhile(context.Background(), time.Second, 1, cue, nil, compact, due); err != nil {
		t.Fatal(err)
	}
	if _, err := compactWhile(context.Background(), time.Second, 2, cue, nil, compact, due); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(levels); got != "[1]" {
		t.Fatalf("levels=%s, want [1]", got)
	}
}

func TestCompactScopeCascadeDrainsTwoL1Batches(t *testing.T) {
	var levels []uint16
	compact := func(_ context.Context, level uint16, _ compaction.Options) (compaction.Result, error) {
		levels = append(levels, level)
		return compaction.Result{Compacted: true}, nil
	}
	left := 2
	cue := func(_ context.Context, level uint16) (compaction.Cue, error) {
		if level != 1 {
			return compaction.Cue{}, nil
		}
		if left == 0 {
			return compaction.Cue{Objects: 1}, nil
		}
		left--
		return compaction.Cue{Objects: 256}, nil
	}
	cfg := config.Config{CompactionMinObjects: 256, CompactionMinBytes: 64 << 20}
	if _, err := compactScopeCascade(context.Background(), time.Second, cfg, cue, nil, compact); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(levels); got != "[1 1]" {
		t.Fatalf("levels=%s, want [1 1]", got)
	}
}

func TestCompactWhileContinuesWhenTheBatchCapLeavesTheBarMet(t *testing.T) {
	n := 0
	compact := func(context.Context, uint16, compaction.Options) (compaction.Result, error) {
		n++
		return compaction.Result{Compacted: true}, nil
	}
	cue := func(context.Context, uint16) (compaction.Cue, error) {
		return compaction.Cue{Objects: 256}, nil
	}
	due := func(c compaction.Cue) bool { return c.Objects >= 256 }
	again, err := compactWhile(context.Background(), time.Second, 1, cue, nil, compact, due)
	if err != nil {
		t.Fatal(err)
	}
	if !again {
		t.Fatal("the bar is still met after the batch cap")
	}
	if n != compactionBatchLimit {
		t.Fatalf("batches=%d, want %d", n, compactionBatchLimit)
	}
}

func TestCompactWhileStopsWhenTheBarDropsOnTheLastCheck(t *testing.T) {
	cues := 0
	compact := func(context.Context, uint16, compaction.Options) (compaction.Result, error) {
		return compaction.Result{Compacted: true}, nil
	}
	cue := func(context.Context, uint16) (compaction.Cue, error) {
		cues++
		if cues <= compactionBatchLimit {
			return compaction.Cue{Objects: 256}, nil
		}
		return compaction.Cue{}, nil
	}
	due := func(c compaction.Cue) bool { return c.Objects >= 256 }
	again, err := compactWhile(context.Background(), time.Second, 1, cue, nil, compact, due)
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Fatal("the bar dropped; another pass is not due")
	}
}

func TestCompactionFollowUpRetriesTransientErrorsAndYieldsAfterTheCap(t *testing.T) {
	retry, delay := compactionFollowUp(false, errors.New("storage full"), 0)
	if !retry {
		t.Fatal("a storage error must be retried")
	}
	if delay != compactionRetryBase {
		t.Fatalf("delay=%s", delay)
	}
	retry, delay = compactionFollowUp(false, context.DeadlineExceeded, 3)
	if !retry {
		t.Fatal("a batch timeout must be retried")
	}
	if delay != 8*time.Second {
		t.Fatalf("delay=%s", delay)
	}
	retry, _ = compactionFollowUp(false, context.Canceled, 0)
	if retry {
		t.Fatal("shutdown must not retry")
	}
	retry, _ = compactionFollowUp(true, errCompactionOwnership, 0)
	if retry {
		t.Fatal("a lost owner must not retry")
	}
	retry, delay = compactionFollowUp(true, nil, 4)
	if !retry {
		t.Fatal("a full batch that is still due must continue")
	}
	if delay != compactionContinueDelay {
		t.Fatalf("delay=%s", delay)
	}
	retry, _ = compactionFollowUp(false, nil, 2)
	if retry {
		t.Fatal("a finished pass must not retry")
	}
	if compactionBackoff(20) != compactionRetryMax {
		t.Fatal("backoff did not reach the cap")
	}
}

func TestFailedCompactionIsQueuedAgainAndALostOwnerIsNot(t *testing.T) {
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "cell"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var delays []time.Duration
	old := scheduleCompactionRetry
	scheduleCompactionRetry = func(_ context.Context, delay time.Duration, nudge func()) {
		delays = append(delays, delay)
		nudge()
	}
	t.Cleanup(func() { scheduleCompactionRetry = old })

	wake := newCompactionWake()
	fail := func(compactionScope) (bool, error) { return false, errors.New("storage full") }
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: sc, epoch: 4}}, fail)
	pending := wake.take()
	if len(pending) != 1 || pending[0].epoch != 4 {
		t.Fatalf("pending=%v", pending)
	}
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: pending[0].scope, epoch: pending[0].epoch}}, fail)
	if len(delays) != 2 || delays[0] != time.Second || delays[1] != 2*time.Second {
		t.Fatalf("delays=%v", delays)
	}

	wake = newCompactionWake()
	delays = nil
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: sc, epoch: 4}}, fail)
	wake.take()
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: sc, epoch: 4}}, func(compactionScope) (bool, error) {
		return false, nil
	})
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: sc, epoch: 4}}, fail)
	if len(delays) != 2 || delays[1] != time.Second {
		t.Fatalf("attempt was not cleared, delays=%v", delays)
	}
	if got := wake.take(); len(got) != 1 || got[0].epoch != 4 {
		t.Fatalf("pending=%v", got)
	}

	before := len(delays)
	compactAndFollow(context.Background(), log, wake, []compactionScope{{scope: sc, epoch: 9}}, func(compactionScope) (bool, error) {
		return false, errCompactionOwnership
	})
	if len(delays) != before {
		t.Fatal("ownership change scheduled a retry")
	}
	if got := wake.take(); len(got) != 0 {
		t.Fatalf("pending=%v", got)
	}
}
