package upload

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// TestEnqueueBatchWaitUploadsOneObject pins the snapshot batching path: several
// parts submitted together become one AppendBatch (one PUT), not N.
func TestEnqueueBatchWaitUploadsOneObject(t *testing.T) {
	sink := &fakeSink{}
	b := New(sink, nil, 64, 1<<20, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "snap"}
	segs := [][]byte{[]byte("part0"), []byte("part1"), []byte("part2"), []byte("part3")}
	if err := b.EnqueueBatchWait(ctx, sc, 1, segs); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.batches != 1 || sink.segments != len(segs) {
		t.Fatalf("batches=%d segments=%d, want 1/%d", sink.batches, sink.segments, len(segs))
	}
}
