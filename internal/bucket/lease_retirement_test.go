package bucket

import (
	"context"
	"testing"
	"time"
)

type blockedRetirement struct {
	*segmentMem
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (b *blockedRetirement) Put(ctx context.Context, k string, v []byte) (string, error) {
	close(b.entered)
	defer close(b.done)
	select {
	case <-b.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return b.segmentMem.Put(ctx, k, v)
}
func TestNodePublicationDoesNotWaitForRetirement(t *testing.T) {
	mem := &blockedRetirement{segmentMem: newSegmentMem(), entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	ctx := WithDeferredAuthorityRetirement(t.Context())
	key := "nodes/deferred.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	published := make(chan error, 1)
	go func() { _, err := auth.CAS(ctx, key, []byte("new"), token); published <- err }()
	select {
	case <-mem.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	defer func() { close(mem.release); <-mem.done }()
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked publication")
	}
	// A fresh reader must see the committed successor before old storage shrinks.
	fresh := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	value, _, err := fresh.Get(t.Context(), key)
	if err != nil || string(value) != "new" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	// Saturation must not launch more goroutines or block another rollover.
	clock.Advance(time.Minute)
	if _, err := auth.Put(ctx, key, []byte("newer")); err != nil {
		t.Fatal(err)
	}
}
