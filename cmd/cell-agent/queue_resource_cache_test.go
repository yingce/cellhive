package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/control"
)

type resourceCacheSource struct {
	key   string
	rows  []control.ResourceRef
	err   error
	calls atomic.Int32
	load  func(context.Context) ([]control.ResourceRef, string, error)
}

func (s *resourceCacheSource) AuthorityKey() string { return s.key }
func (s *resourceCacheSource) ResourcesByKindWithAuthority(ctx context.Context, kind string) ([]control.ResourceRef, string, error) {
	s.calls.Add(1)
	if s.load != nil {
		return s.load(ctx)
	}
	return s.rows, s.key, s.err
}
func TestQueueResourceCachePollBudgetAndExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	s := &resourceCacheSource{key: "a", rows: []control.ResourceRef{{Name: "jobs", Scope: "acme/__queue__/one"}}}
	c := &queueResourceCache{src: s, now: func() time.Time { return now }}
	for i := 0; i < 30; i++ {
		rows, err := c.Resources(t.Context())
		if err != nil || len(rows) != 1 || rows[0].Name != "jobs" {
			t.Fatalf("rows=%v err=%v", rows, err)
		}
		rows[0].Name = "caller-mutated"
		now = now.Add(time.Second)
	}
	if s.calls.Load() != 6 {
		t.Fatalf("30 one-second polls fetched %d times, want 6", s.calls.Load())
	}
}
func TestQueueResourceCacheDeletionRecreationAndFailedRefresh(t *testing.T) {
	now := time.Unix(100, 0)
	s := &resourceCacheSource{key: "a", rows: []control.ResourceRef{{Namespace: "acme", Kind: "queue", Name: "jobs", Scope: "acme/__queue__/one"}}}
	c := &queueResourceCache{src: s, now: func() time.Time { return now }}
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Empty discovery is a valid deletion result and must replace the old map.
	s.rows = nil
	now = now.Add(5 * time.Second)
	rows, err := c.Resources(t.Context())
	if err != nil || len(rows) != 0 {
		t.Fatalf("deleted resource retained: %v %v", rows, err)
	}
	refs, err := resolveQueueRefs(rows, []control.QueueTarget{{Namespace: "acme", Queue: "jobs"}})
	if err == nil || len(refs) != 0 {
		t.Fatal("deleted mapping accepted")
	}
	s.rows = []control.ResourceRef{{Namespace: "acme", Kind: "queue", Name: "jobs", Scope: "acme/__queue__/two"}}
	now = now.Add(5 * time.Second)
	rows, err = c.Resources(t.Context())
	if err != nil || rows[0].Scope != "acme/__queue__/two" {
		t.Fatalf("recreation not refreshed: %v %v", rows, err)
	}
	now = now.Add(5 * time.Second)
	s.err = errors.New("owner unavailable")
	rows, err = c.Resources(t.Context())
	if !errors.Is(err, s.err) || len(rows) != 0 {
		t.Fatalf("expired fallback: %v %v", rows, err)
	}
	s.err = nil
	if _, err = c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestQueueResourceCacheObservedAuthorityInvalidation(t *testing.T) {
	s := &resourceCacheSource{key: "a"}
	c := &queueResourceCache{src: s}
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.key = "b"
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() != 2 {
		t.Fatal("known owner change reused cache")
	}
	s.key = "failure-generation"
	s.err = errors.New("lost authority")
	if _, err := c.Resources(t.Context()); !errors.Is(err, s.err) {
		t.Fatalf("known error hidden: %v", err)
	}
}
func TestQueueResourceCacheIdentityAndSlowRead(t *testing.T) {
	now := time.Unix(100, 0)
	s := &resourceCacheSource{key: "b"}
	s.load = func(context.Context) ([]control.ResourceRef, string, error) {
		now = now.Add(4 * time.Second)
		return nil, "a", nil
	}
	c := &queueResourceCache{src: s, now: func() time.Time { return now }}
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The result belongs to a, not the unrelated latest observed b.
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() != 2 {
		t.Fatal("tagged old response with latest identity")
	}
	s.key = "a"
	now = now.Add(time.Second)
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() != 3 {
		t.Fatal("slow read extended TTL past start + 5s")
	}
}
func TestQueueResourceCacheCoalescesAndHonorsCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s := &resourceCacheSource{key: "a"}
	s.load = func(ctx context.Context) ([]control.ResourceRef, string, error) {
		if s.calls.Load() == 1 {
			close(entered)
		}
		select {
		case <-release:
			return []control.ResourceRef{{Name: "jobs"}}, "a", nil
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	c := &queueResourceCache{src: s}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := c.Resources(t.Context())
			if e != nil || len(r) != 1 {
				t.Errorf("%v %v", r, e)
			}
		}()
	}
	<-entered
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Resources(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if s.calls.Load() != 1 {
		t.Fatalf("refreshes=%d", s.calls.Load())
	}
	if _, err := c.Resources(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled caller received warm cache")
	}
}
func TestQueueResourceCacheCanceledLeaderCanRetry(t *testing.T) {
	s := &resourceCacheSource{key: "a"}
	c := &queueResourceCache{src: s}
	ctx, cancel := context.WithCancel(t.Context())
	s.load = func(context.Context) ([]control.ResourceRef, string, error) { cancel(); return nil, "a", nil }
	if _, err := c.Resources(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.load = nil
	if _, err := c.Resources(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() != 2 {
		t.Fatal("canceled response cached")
	}
}
