package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/peer"
)

// TestOrderedDispatcherDispatchesInTxidOrder verifies that commits arriving out
// of order are still dispatched (frame-written) in ascending start_txid order,
// while each caller resolves independently.
func TestOrderedDispatcherDispatchesInTxidOrder(t *testing.T) {
	d := newOrderedDispatcher()
	defer d.Close()

	var mu sync.Mutex
	var order []uint64
	dispatch := func(start uint64) func() (<-chan peer.CommitAck, error) {
		return func() (<-chan peer.CommitAck, error) {
			mu.Lock()
			order = append(order, start)
			mu.Unlock()
			ch := make(chan peer.CommitAck, 1)
			ch <- peer.CommitAck{}
			return ch, nil
		}
	}
	resolve := func(ch <-chan peer.CommitAck) {
		if ack := <-ch; ack.Err != nil {
			t.Errorf("future: %v", ack.Err)
		}
	}

	ctx := context.Background()
	// Establish the baseline synchronously, then submit the rest out of order.
	base, err := d.Do(ctx, "scope", 10, 10, 19, dispatch(10))
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	resolve(base)

	var wg sync.WaitGroup
	for _, r := range []struct{ start, end uint64 }{{30, 39}, {40, 49}, {20, 29}} {
		wg.Add(1)
		go func(start, end uint64) {
			defer wg.Done()
			future, err := d.Do(ctx, "scope", 10, start, end, dispatch(start))
			if err != nil {
				t.Errorf("do %d: %v", start, err)
				return
			}
			resolve(future)
		}(r.start, r.end)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	want := []uint64{10, 20, 30, 40}
	if len(order) != len(want) {
		t.Fatalf("dispatch order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("dispatch order = %v, want %v", order, want)
		}
	}
}

func TestOrderedDispatcherFailsScopeOnGapTimeout(t *testing.T) {
	d := newOrderedDispatcher()
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ok := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		ch <- peer.CommitAck{}
		return ch, nil
	}
	blocking := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		go func() { time.Sleep(time.Hour); ch <- peer.CommitAck{} }()
		return ch, nil
	}
	base, err := d.Do(ctx, "gap", 1, 1, 1, ok)
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	<-base
	// txid 6 arrives while 2..5 never do; it must fail when ctx expires rather
	// than stall forever.
	if _, err := d.Do(ctx, "gap", 1, 6, 6, blocking); err == nil {
		t.Fatalf("expected timeout for out-of-order commit with missing predecessor")
	}
}

func TestOrderedDispatcherEvictsIdleScope(t *testing.T) {
	d := newOrderedDispatcher()
	d.ttl = 20 * time.Millisecond
	defer d.Close()

	ok := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		ch <- peer.CommitAck{}
		return ch, nil
	}
	blocking := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		return ch, nil // never resolves
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, _ := d.Do(ctx, "evict", 1, 1, 1, ok)
	<-base
	// txid 6 buffered waiting for 2..5 that never arrive.
	done := make(chan error, 1)
	go func() {
		_, err := d.Do(ctx, "evict", 1, 6, 6, blocking)
		done <- err
	}()
	// Trigger the sweep directly after the TTL elapses.
	time.Sleep(30 * time.Millisecond)
	d.sweep(time.Now())
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected buffered request to be evicted with an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("buffered request was not evicted")
	}
}

// TestOrderedDispatcherDoSweepsIdleScope verifies the production sweep: a
// request for another scope triggers eviction of an idle scope whose buffered
// out-of-order request is waiting on a gap that never arrives (ADR-170).
func TestOrderedDispatcherDoSweepsIdleScope(t *testing.T) {
	d := newOrderedDispatcher()
	d.ttl = 20 * time.Millisecond
	defer d.Close()

	ok := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		ch <- peer.CommitAck{}
		return ch, nil
	}
	blocking := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		return ch, nil // never resolves
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	base, _ := d.Do(ctx, "sweep", 1, 1, 1, ok)
	<-base
	done := make(chan error, 1)
	go func() {
		_, err := d.Do(ctx, "sweep", 1, 6, 6, blocking) // waits for 2..5
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)

	// A request for another scope drives the opportunistic sweep.
	if _, err := d.Do(ctx, "other", 1, 1, 1, ok); err != nil {
		t.Fatalf("other scope: %v", err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "evicted") {
			t.Fatalf("gap error = %v, want idle eviction", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("buffered request was not evicted by the Do-triggered sweep")
	}
}

// TestOrderedDispatcherRefreshesLastUsed is the regression for lastUsed being
// read by sweep but never written: every scope was treated as infinitely idle.
func TestOrderedDispatcherRefreshesLastUsed(t *testing.T) {
	d := newOrderedDispatcher()
	d.ttl = 50 * time.Millisecond
	defer d.Close()

	ok := func() (<-chan peer.CommitAck, error) {
		ch := make(chan peer.CommitAck, 1)
		ch <- peer.CommitAck{}
		return ch, nil
	}
	ctx := context.Background()
	if _, err := d.Do(ctx, "live", 1, 1, 1, ok); err != nil {
		t.Fatalf("first: %v", err)
	}
	d.mu.Lock()
	q := d.scopes["live"]
	d.mu.Unlock()
	q.mu.Lock()
	first := q.lastUsed
	q.mu.Unlock()
	if first.IsZero() {
		t.Fatalf("lastUsed was not set on first use")
	}

	time.Sleep(10 * time.Millisecond)
	if _, err := d.Do(ctx, "live", 1, 2, 2, ok); err != nil {
		t.Fatalf("second: %v", err)
	}
	q.mu.Lock()
	second := q.lastUsed
	q.mu.Unlock()
	if !second.After(first) {
		t.Fatalf("lastUsed not refreshed: %v -> %v", first, second)
	}

	// An active scope (used within the TTL) must survive a sweep.
	d.mu.Lock()
	d.lastSweep = time.Time{}
	d.mu.Unlock()
	d.sweep(time.Now())
	d.mu.Lock()
	_, present := d.scopes["live"]
	d.mu.Unlock()
	if !present {
		t.Fatalf("active scope was evicted")
	}
}
