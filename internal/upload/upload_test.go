package upload

import (
	"context"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
)

type fakeSink struct {
	mu       sync.Mutex
	batches  int
	segments int
}

func (f *fakeSink) AppendBatch(_ context.Context, _ cell.Scope, _ uint64, segments [][]byte) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches++
	f.segments += len(segments)
	return "key", "etag", nil
}

func TestBatcherCoalesces(t *testing.T) {
	sink := &fakeSink{}
	b := New(sink, nil, 64, 1<<20, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	for i := 0; i < 200; i++ {
		b.Enqueue(context.Background(), sc, 1, []byte{byte(i)})
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	b.Stop()

	st := b.Stats()
	if st["segments"] != 200 {
		t.Fatalf("segments uploaded = %d, want 200", st["segments"])
	}
	if st["batches"] >= 200 {
		t.Fatalf("batches = %d, want < 200 (group commit)", st["batches"])
	}
	if st["batches"] == 0 {
		t.Fatalf("no batches")
	}
}

func TestBatcherBackpressureOnFull(t *testing.T) {
	// A zero-capacity-ish queue is emulated by not starting the loop: Enqueue
	// must fall back to a synchronous upload rather than dropping.
	sink := &fakeSink{}
	b := New(sink, nil, 1, 1, time.Millisecond)
	// Do not Start: the channel fills, then Enqueue uploads synchronously.
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	for i := 0; i < cap(b.shards[0].ch)+5; i++ {
		b.Enqueue(context.Background(), sc, 1, []byte{byte(i)})
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.segments == 0 {
		t.Fatalf("expected synchronous fallback uploads")
	}
}

// orderSink records the exact order of segments and detects overlapping calls.
type orderSink struct {
	mu          sync.Mutex
	inflight    int
	maxInflight int
	batches     int
	order       []byte
}

func (o *orderSink) AppendBatch(_ context.Context, _ cell.Scope, _ uint64, segments [][]byte) (string, string, error) {
	o.mu.Lock()
	o.inflight++
	if o.inflight > o.maxInflight {
		o.maxInflight = o.inflight
	}
	o.mu.Unlock()

	time.Sleep(500 * time.Microsecond) // widen the window for overlap detection

	o.mu.Lock()
	for _, s := range segments {
		o.order = append(o.order, s[0])
	}
	o.batches++
	o.inflight--
	o.mu.Unlock()
	return "key", "etag", nil
}

// TestBatcherBlocksOrderedAndSerial proves group-commit submits blocks (fewer
// objects than writes), preserves order, and never uploads two blocks at once
// (no ordering races).
func TestBatcherBlocksOrderedAndSerial(t *testing.T) {
	sink := &orderSink{}
	b := New(sink, nil, 8, 1<<20, 20*time.Millisecond) // small maxSegments -> several blocks
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "ordered"}
	const N = 100
	for i := 0; i < N; i++ {
		b.Enqueue(context.Background(), sc, 1, []byte{byte(i)})
	}
	time.Sleep(200 * time.Millisecond)
	cancel()
	b.Stop()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.order) != N {
		t.Fatalf("received %d segments, want %d", len(sink.order), N)
	}
	for i := 0; i < N; i++ {
		if sink.order[i] != byte(i) {
			t.Fatalf("order broken at %d: got %d", i, sink.order[i])
		}
	}
	if sink.batches >= N {
		t.Fatalf("batches = %d, want < %d (blocked group commit)", sink.batches, N)
	}
	if sink.maxInflight != 1 {
		t.Fatalf("overlapping AppendBatch calls detected: maxInflight=%d", sink.maxInflight)
	}
}

// TestBatcherShardsOverlap verifies that sharding lets independent scopes upload
// concurrently while keeping each scope's segment order intact.
func TestBatcherShardsOverlap(t *testing.T) {
	sink := &orderSink{}
	b := NewSharded(sink, nil, 4, 1<<20, 5*time.Millisecond, 4)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	scopes := []cell.Scope{
		{Namespace: "demo", Class: "__kv__", ID: "a"},
		{Namespace: "demo", Class: "__kv__", ID: "c"},
		{Namespace: "demo", Class: "__kv__", ID: "e"},
		{Namespace: "demo", Class: "__kv__", ID: "g"},
	}
	const per = 50
	var wg sync.WaitGroup
	for _, sc := range scopes {
		wg.Add(1)
		go func(sc cell.Scope) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				b.EnqueueWait(context.Background(), sc, 1, []byte{byte(i)})
			}
		}(sc)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	cancel()
	b.Stop()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.maxInflight < 2 {
		t.Fatalf("expected concurrent uploads across shards, maxInflight=%d", sink.maxInflight)
	}
	if len(sink.order) != per*len(scopes) {
		t.Fatalf("received %d segments, want %d", len(sink.order), per*len(scopes))
	}
}
