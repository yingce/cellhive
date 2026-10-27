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

// A wake can be consumed before its item is dequeued (the channels are
// independent). The item itself must re-arm flushing, even with a long timer.
func TestWaitItemRearmsFlush(t *testing.T) {
	sink := &fakeSink{}
	b := New(sink, nil, 64, 1<<20, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer func() { cancel(); b.Stop() }()
	done := make(chan error, 1)
	b.shards[0].ch <- item{scope: cell.Scope{Namespace: "demo", Class: "__kv__", ID: "wake"}, epoch: 1, seg: []byte("x"), done: done}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait item stranded until periodic flush")
	}
}

func TestAsyncWindowCoalescesSpacedWrites(t *testing.T) {
	sink := &fakeSink{}
	b := New(sink, nil, 64, 1<<20, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer func() { cancel(); b.Stop() }()
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "spaced"}
	for i := 0; i < 4; i++ {
		b.Enqueue(ctx, sc, 1, []byte{byte(i)})
		time.Sleep(25 * time.Millisecond)
	}
	// A synchronous durability request must flush the preceding async writes
	// without waiting for the longer background timer.
	if err := b.EnqueueWait(ctx, sc, 1, []byte("sync")); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.batches != 1 || sink.segments != 5 {
		t.Fatalf("batches=%d segments=%d, want 1/5", sink.batches, sink.segments)
	}
}
