package lease

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type nodeLookupBucket struct {
	bucket.Bucket
	lists atomic.Int32
	gets  atomic.Int32
}

func (b *nodeLookupBucket) List(context.Context, string) ([]string, error) {
	b.lists.Add(1)
	return nil, errors.New("node lookup must not enumerate fleet")
}
func (b *nodeLookupBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, key)
}

func TestNodeLeaseColdLookupDoesNotSampleFleet(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer := NewManager(base, "peer", "session", "addr", "", time.Minute)
	if err := peer.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	b := &nodeLookupBucket{Bucket: base}
	m := NewManager(b, "self", "session", "addr", "", time.Minute)
	l, ok, err := m.NodeLease(t.Context(), "peer")
	if err != nil || !ok || l.Session != "session" {
		t.Fatalf("lookup: %+v %v %v", l, ok, err)
	}
	if b.lists.Load() != 0 || b.gets.Load() != 1 {
		t.Fatalf("single-node lookup: lists=%d gets=%d; want 0/1", b.lists.Load(), b.gets.Load())
	}
}

func TestSampleCachedColdDoesNotWaitForFleetScan(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	b := &probeBucket{Bucket: base, probe: func() { entered <- struct{}{}; <-release }}
	m := NewManager(b, "self", "session", "addr", "", time.Minute)
	returned := make(chan []NodeLease, 1)
	go func() { returned <- m.SampleCached(context.Background(), time.Minute) }()
	<-entered
	select {
	case got := <-returned:
		if got != nil {
			t.Errorf("cold snapshot=%v; expected nil until complete", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("first caller blocked on fleet enumeration")
	}
	close(release)
	// Wait for the refresh before the test bucket directory is removed.
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		done := !m.refreshing && m.cached != nil
		m.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}
