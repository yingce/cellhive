package bucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// segmentMem implements both PositionAppender and segmentData for unit tests.
type segmentMem struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newSegmentMem() *segmentMem {
	return &segmentMem{objects: map[string][]byte{}}
}

func (m *segmentMem) Read(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *segmentMem) Append(_ context.Context, key string, position int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int64(len(m.objects[key])) != position {
		return ErrPrecondition
	}
	m.objects[key] = append(m.objects[key], data...)
	return nil
}

func (m *segmentMem) TailGet(_ context.Context, key string, n int) ([]byte, int64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[key]
	if !ok {
		return nil, 0, "", ErrNotFound
	}
	if n <= 0 || n >= len(v) {
		return append([]byte(nil), v...), int64(len(v)), fmt.Sprintf("%d", len(v)), nil
	}
	return append([]byte(nil), v[len(v)-n:]...), int64(len(v)), fmt.Sprintf("%d", len(v)), nil
}

func (m *segmentMem) RangedGet(_ context.Context, key string, off, length int64) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	if off < 0 || length < 0 || off > int64(len(v)) || length > int64(len(v))-off {
		return nil, "", fmt.Errorf("range out of bounds")
	}
	return append([]byte(nil), v[off:off+length]...), fmt.Sprintf("%d", len(v)), nil
}

func (m *segmentMem) Stat(_ context.Context, key string) (int64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[key]
	if !ok {
		return 0, "", ErrNotFound
	}
	return int64(len(v)), fmt.Sprintf("%d", len(v)), nil
}

func (m *segmentMem) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}

func (m *segmentMem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *segmentMem) count(prefix string) (n int, total int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, value := range m.objects {
		if strings.HasPrefix(key, prefix) {
			n++
			total += int64(len(value))
		}
	}
	return n, total
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestSegmented(t *testing.T) (*SegmentedAuthority, *segmentMem, *testClock) {
	t.Helper()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{
		Segment:         time.Minute,
		MaxSegmentBytes: 1 << 20,
		Now:             clock.Now,
	})
	return auth, mem, clock
}

func TestSegmentedAuthorityRotatesAndCollects(t *testing.T) {
	ctx := context.Background()
	auth, mem, clock := newTestSegmented(t)
	key := "cells/acme/__kv__/main/owner.json"

	token, err := auth.ConditionalCreate(ctx, key, []byte("v0"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		value := []byte(fmt.Sprintf("renew-%d", i))
		token, err = auth.CAS(ctx, key, value, token)
		if err != nil {
			t.Fatalf("cas %d: %v", i, err)
		}
	}
	if n, total := mem.count(key); n != 1 {
		t.Fatalf("same-window segments = %d, want 1 (bytes %d)", n, total)
	}
	if total := func() int64 { _, n := mem.count(key); return n }(); total > 4096 {
		t.Fatalf("same-window log grew to %d bytes", total)
	}

	// Cross one rollover: the first write creates a fresh current window and
	// keeps the previous one for readers.
	clock.Advance(2 * time.Minute)
	value, gotToken, err := auth.Get(ctx, key)
	if err != nil || string(value) != "renew-49" {
		t.Fatalf("get before rollover write = %q, %v", value, err)
	}
	if _, err := auth.CAS(ctx, key, []byte("v-roll"), gotToken); err != nil {
		t.Fatalf("rollover cas: %v", err)
	}
	value, token, err = auth.Get(ctx, key)
	if err != nil || string(value) != "v-roll" {
		t.Fatalf("get after rollover = %q, %v", value, err)
	}
	if n, _ := mem.count(key); n > 2 {
		t.Fatalf("physical segments after rollover = %d, want <=2", n)
	}

	// Several windows later the old segments must be physically gone.
	clock.Advance(5 * time.Minute)
	if _, err := auth.Put(ctx, key, []byte("latest")); err != nil {
		t.Fatal(err)
	}
	if n, _ := mem.count(key); n > 2 {
		t.Fatalf("physical segments after gc = %d, want <=2", n)
	}
	value, _, err = auth.Get(ctx, key)
	if err != nil || string(value) != "latest" {
		t.Fatalf("final get = %q, %v", value, err)
	}
	_ = token
}

func TestSegmentedAuthorityMigratesLegacyLog(t *testing.T) {
	ctx := context.Background()
	mem := newSegmentMem()
	legacy := NewAppendAuthority(mem)
	key := "cells/acme/__kv__/main/owner.json"
	token, err := legacy.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		token, err = legacy.CAS(ctx, key, []byte(fmt.Sprintf("old-%d", i)), token)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, total := mem.count(key); total < 20*frameHeader {
		t.Fatalf("legacy log too small: %d", total)
	}

	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{
		Segment: time.Minute, MaxSegmentBytes: 1 << 20, Now: clock.Now,
	})
	value, legacyToken, err := auth.Get(ctx, key)
	if err != nil || string(value) != "old-19" {
		t.Fatalf("legacy get = %q, %v", value, err)
	}
	if _, err := auth.CAS(ctx, key, []byte("new"), legacyToken); err != nil {
		t.Fatalf("legacy cas migration: %v", err)
	}
	if _, err := mem.Read(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("legacy key still exists: %v", err)
	}
	value, _, err = auth.Get(ctx, key)
	if err != nil || string(value) != "new" {
		t.Fatalf("post-migration get = %q, %v", value, err)
	}
	if n, total := mem.count(key); n != 1 || total > 1024 {
		t.Fatalf("post-migration segments=%d bytes=%d", n, total)
	}
}

func TestSegmentedAuthorityHardCapFailsClosed(t *testing.T) {
	ctx := context.Background()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1_700_000_000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{
		Segment: time.Hour, MaxSegmentBytes: 128, Now: clock.Now,
	})
	token, err := auth.ConditionalCreate(ctx, "fleet/waker.json", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	var full bool
	for i := 0; i < 20; i++ {
		token, err = auth.CAS(ctx, "fleet/waker.json", []byte(strings.Repeat("y", 32)), token)
		if errors.Is(err, ErrAuthoritySegmentFull) {
			full = true
			break
		}
		if err != nil {
			t.Fatalf("cas: %v", err)
		}
	}
	if !full {
		t.Fatal("expected ErrAuthoritySegmentFull")
	}
	if _, total := mem.count("fleet/waker.json"); total > 128 {
		t.Fatalf("segment exceeded cap: %d", total)
	}
}

func TestSegmentedAuthorityTombstoneAndRecreate(t *testing.T) {
	ctx := context.Background()
	auth, _, _ := newTestSegmented(t)
	key := "nodes/n1.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("live"))
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.ConditionalDelete(ctx, key, token); err != nil {
		t.Fatal(err)
	}
	if _, _, err := auth.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete = %v, want not found", err)
	}
	if err := auth.ConditionalDelete(ctx, key, token); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale delete = %v, want precondition", err)
	}
	next, err := auth.ConditionalCreate(ctx, key, []byte("again"))
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := auth.Get(ctx, key)
	if err != nil || string(value) != "again" {
		t.Fatalf("recreate get = %q, %v", value, err)
	}
	if _, err := auth.CAS(ctx, key, []byte("renewed"), next); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentedAuthorityConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	auth, _, _ := newTestSegmented(t)
	key := "cells/acme/__queue__/jobs/owner.json"
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := auth.ConditionalCreate(ctx, key, []byte(fmt.Sprintf("owner-%d", i)))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrPrecondition):
		default:
			t.Fatalf("unexpected create error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("winners = %d, want 1", wins)
	}
}

func TestParsePhysicalAuthorityKey(t *testing.T) {
	for _, key := range []string{
		"cells/acme/__kv__/main/owner.json",
		"cells/acme/__kv__/main/owner-gen",
		"nodes/n1.json",
		"node-logs/n1/s1.json",
		"fleet/waker.json",
		"fleet/drain-token.json",
	} {
		physical := key + ".w123"
		base, window, ok := ParsePhysicalAuthorityKey(physical)
		if !ok || base != key || window != 123 {
			t.Fatalf("parse %q = %q/%d/%v", physical, base, window, ok)
		}
		if _, _, ok := ParsePhysicalAuthorityKey(key); ok {
			t.Fatalf("logical key parsed as physical: %q", key)
		}
	}
	if _, _, ok := ParsePhysicalAuthorityKey("cells/acme/__kv__/main/ltx/e1/1.ltx.w1"); ok {
		t.Fatal("non-authority physical key parsed")
	}
}

func TestSegmentedAuthorityFindsSegmentAfterLongIdle(t *testing.T) {
	ctx := context.Background()
	auth, _, clock := newTestSegmented(t)
	key := "cells/acme/__kv__/main/owner-gen"
	if _, err := auth.ConditionalCreate(ctx, key, []byte("41")); err != nil {
		t.Fatal(err)
	}
	// No write for many segments: the last segment must still be discoverable
	// so a monotonic epoch fence is never reset to zero.
	clock.Advance(10 * time.Minute)
	value, token, err := auth.Get(ctx, key)
	if err != nil || string(value) != "41" {
		t.Fatalf("long-idle get = %q, %v", value, err)
	}
	next, err := auth.CAS(ctx, key, []byte("42"), token)
	if err != nil {
		t.Fatalf("long-idle cas: %v", err)
	}
	if _, err := auth.CAS(ctx, key, []byte("43"), next); err != nil {
		t.Fatal(err)
	}
	value, _, err = auth.Get(ctx, key)
	if err != nil || string(value) != "43" {
		t.Fatalf("long-idle final = %q, %v", value, err)
	}
}

func TestSegmentedAuthorityClockSkewWithinBoundaryGuard(t *testing.T) {
	ctx := context.Background()
	mem := newSegmentMem()
	// Two clocks one second apart, close to the next window. The default guard
	// is 10% of the segment; a one-second skew must stay safe.
	base := time.Unix(1_700_000_000, 0).Add(58 * time.Second)
	clockA := &fixedClock{t: base.Add(time.Second)}
	clockB := &fixedClock{t: base}
	authA := NewSegmentedAuthority(mem, mem, AuthorityOptions{Segment: time.Minute, MaxSegmentBytes: 1 << 20, Now: clockA.Now})
	authB := NewSegmentedAuthority(mem, mem, AuthorityOptions{Segment: time.Minute, MaxSegmentBytes: 1 << 20, Now: clockB.Now})
	key := "cells/acme/__kv__/main/owner.json"
	if _, err := authA.ConditionalCreate(ctx, key, []byte("a")); err != nil {
		t.Fatal(err)
	}
	value, _, err := authB.Get(ctx, key)
	if err != nil || string(value) != "a" {
		t.Fatalf("B behind by 1s failed to see A: %q %v", value, err)
	}
	if _, err := authB.ConditionalCreate(ctx, key, []byte("b")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("B should not create over A: %v", err)
	}
}

type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }
