package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestRecoveryBatchBoundaries(t *testing.T) {
	ctx := t.Context()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fs)
	b := recoveryBatch{rep: rep}
	sc := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "main"}
	other := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "other"}
	add := func(scope cell.Scope, epoch, tx uint64, kind ltx.Kind, payload int) {
		t.Helper()
		raw := ltx.Encode(ltx.Header{Kind: kind, Epoch: epoch, StartTxID: tx, EndTxID: tx}, make([]byte, payload))
		if err := b.add(ctx, scope, epoch, raw); err != nil {
			t.Fatal(err)
		}
	}
	// Two deltas per batch because a third would exceed 1MiB.
	for i := uint64(1); i <= 3; i++ {
		add(sc, 1, i, ltx.KindDelta, 400<<10)
	}
	add(other, 1, 1, ltx.KindDelta, 1)
	add(sc, 2, 1, ltx.KindDelta, 1)
	add(sc, 2, 3, ltx.KindDelta, 1) // gap must not be advertised as a contiguous batch
	add(sc, 2, 4, ltx.KindSnapshot, 1)
	add(sc, 2, 5, ltx.KindMarker, 0)
	if err := b.flush(ctx); err != nil {
		t.Fatal(err)
	}
	keys, err := fs.List(ctx, "cells/")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for _, key := range keys {
		if !strings.Contains(key, "/ltx/") {
			continue
		}
		count++
		raw, _, err := fs.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		parts, err := ltx.Split(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 1<<20 {
			t.Fatalf("oversize batch %s", key)
		}
		if len(parts) > 1 {
			for _, part := range parts {
				h, _, _ := ltx.Decode(part)
				if h.Kind != ltx.KindDelta {
					t.Fatalf("mixed baseline in %s", key)
				}
			}
		}
	}
	if count != 7 || b.uploaded != 8 {
		t.Fatalf("objects=%d uploaded=%d", count, b.uploaded)
	}
}

type failBatchBucket struct {
	bucket.Bucket
	fail bool
}

func (b *failBatchBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if b.fail && strings.Contains(key, "/ltx/") {
		return "", errors.New("injected upload failure")
	}
	return b.Bucket.ConditionalCreate(ctx, key, data)
}

func TestRecoveryBatchFailureRetainsPending(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	fb := &failBatchBucket{Bucket: fs, fail: true}
	b := recoveryBatch{rep: replica.New(fb)}
	sc := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "failure"}
	for i := uint64(1); i <= 2; i++ {
		raw := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte("data"))
		if err := b.add(t.Context(), sc, 1, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.flush(t.Context()); err == nil {
		t.Fatal("expected upload failure")
	}
	if b.uploaded != 0 || len(b.segments) != 2 {
		t.Fatal("failed upload discarded pending segments")
	}
	fb.fail = false
	if err := b.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if b.uploaded != 2 {
		t.Fatalf("uploaded=%d", b.uploaded)
	}
}
