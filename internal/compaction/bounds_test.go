package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestBoundedPromotionContinuesContiguously(t *testing.T) {
	for _, byteLimit := range []int64{1, 1 << 20} {
		t.Run(fmt.Sprint(byteLimit), func(t *testing.T) {
			m, s := nativeManager(t)
			for tx := uint64(1); tx <= 5; tx++ {
				appendNative(t, m, s, 0, tx, tx, 1, 2)
			}
			wantInputs := 2
			if byteLimit == 1 {
				wantInputs = 1
			}
			var last uint64
			for last < 5 {
				r, err := Compact(t.Context(), m, s, 1, Options{MaxInputs: 2, MaxInputBytes: byteLimit})
				if err != nil || !r.Compacted || r.Inputs > wantInputs || r.MinTxID != last+1 {
					t.Fatalf("result=%+v err=%v previous=%d", r, err, last)
				}
				last = r.MaxTxID
			}
			r, err := Compact(t.Context(), m, s, 1, Options{})
			if err != nil || r.Skipped != "already compacted" {
				t.Fatalf("result=%+v err=%v", r, err)
			}
		})
	}
}

type snapshotReadGuard struct {
	bucket.Bucket
	payloads int
	headers  int
}

func (b *snapshotReadGuard) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		b.payloads++
		return nil, "", errors.New("unexpected payload")
	}
	return b.Bucket.Get(ctx, key)
}
func (b *snapshotReadGuard) OpenReader(context.Context, string) (io.ReadCloser, int64, string, error) {
	b.payloads++
	return nil, 0, "", errors.New("unexpected stream")
}
func (b *snapshotReadGuard) RangedGet(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		if off != 0 || n != ltx.NativeHeaderSize {
			b.payloads++
			return nil, "", errors.New("unexpected payload range")
		}
		b.headers++
	}
	return b.Bucket.RangedGet(ctx, key, off, n)
}

func TestSnapshotBudgetBeforePayloadRead(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	// Latest logical size, not the baseline, determines the budget.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 200000, MinTXID: 2, MaxTXID: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.AppendLevel(t.Context(), s, 1, 0, raw); err != nil {
		t.Fatal(err)
	}
	guard := &snapshotReadGuard{Bucket: m.B}
	r, err := Snapshot(t.Context(), replica.New(guard), s, 1, Options{})
	if err != nil || r.Skipped != "snapshot logical size exceeds budget" || guard.payloads != 0 || guard.headers != 1 {
		t.Fatalf("result=%+v err=%v payloads=%d headers=%d", r, err, guard.payloads, guard.headers)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 1, 9, 0)
	if err != nil || len(keys) != 0 {
		t.Fatalf("snapshot published: %v %v", keys, err)
	}
}

func TestSnapshotFanInRefusalBeforePayloadRead(t *testing.T) {
	m, s := nativeManager(t)
	for tx := uint64(1); tx <= 3; tx++ {
		appendNative(t, m, s, 0, tx, tx, 1, 2)
	}
	guard := &snapshotReadGuard{Bucket: m.B}
	r, err := Snapshot(t.Context(), replica.New(guard), s, 1, Options{MaxInputs: 2})
	if err != nil || r.Skipped != "snapshot input count exceeds budget" || guard.payloads != 0 {
		t.Fatalf("result=%+v err=%v payloads=%d", r, err, guard.payloads)
	}
	// Ordinary promotions can reduce fan-in without changing the restore chain.
	if _, err := Compact(t.Context(), m, s, 1, Options{}); err != nil {
		t.Fatal(err)
	}
	r, err = Snapshot(t.Context(), m, s, 1, Options{MaxInputs: 2})
	if err != nil || !r.Compacted {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}

func TestCompactionSlotCancellation(t *testing.T) {
	release, err := acquireJob(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = acquireJob(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestPromotionDefaultInputLimit(t *testing.T) {
	m, s := nativeManager(t)
	for tx := uint64(1); tx <= DefaultMaxInputs+1; tx++ {
		appendNative(t, m, s, 0, tx, tx, 1, 2)
	}
	r, err := Compact(t.Context(), m, s, 1, Options{})
	if err != nil || !r.Compacted || r.Inputs != DefaultMaxInputs || r.MaxTxID != DefaultMaxInputs {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	r, err = Compact(t.Context(), m, s, 1, Options{})
	if err != nil || !r.Compacted || r.Inputs != 1 || r.MinTxID != DefaultMaxInputs+1 {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}

func TestPromotionByteBudgetStopsBeforeNextSource(t *testing.T) {
	m, s := nativeManager(t)
	key := appendNative(t, m, s, 0, 1, 1, 1, 2)
	appendNative(t, m, s, 0, 2, 2, 1, 2)
	size, _, err := m.B.(bucket.Statter).Stat(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Compact(t.Context(), m, s, 1, Options{MaxInputBytes: size})
	if err != nil || r.Inputs != 1 || r.InputBytes != size || r.MaxTxID != 1 {
		t.Fatalf("result=%+v err=%v limit=%d", r, err, size)
	}
}

func TestSnapshotExplicitBudgetBoundary(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	r, err := Snapshot(t.Context(), m, s, 1, Options{SnapshotMaxBytes: 1023})
	if err != nil || r.Skipped != "snapshot logical size exceeds budget" {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	r, err = Snapshot(t.Context(), m, s, 1, Options{SnapshotMaxBytes: 1024})
	if err != nil || !r.Compacted {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}

func TestBatchLimitMakesProgressBelowConfiguredMinimumBytes(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	appendNative(t, m, s, 0, 2, 2, 2)
	r, err := CompactLevel(t.Context(), m, s, 1, 1, Options{MaxInputs: 1, MinBytes: 1 << 30})
	if err != nil || !r.Compacted || r.Inputs != 1 || r.MaxTxID != 1 {
		t.Fatalf("bounded progress: %+v %v", r, err)
	}
}
func TestSnapshotInputByteBudgetNeverPublishesPartialChain(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	appendNative(t, m, s, 0, 2, 2, 2)
	r, err := Snapshot(t.Context(), m, s, 1, Options{MaxInputBytes: 1})
	if err != nil || r.Compacted || r.Skipped != "snapshot input bytes exceed budget" {
		t.Fatalf("L9: %+v %v", r, err)
	}
	keys, err := m.ListLevelFrom(t.Context(), s, 1, 9, 0)
	if err != nil || len(keys) != 0 {
		t.Fatalf("partial snapshot published %v %v", keys, err)
	}
}
