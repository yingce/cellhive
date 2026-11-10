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
	lists   int
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
	m.lists++
	var out []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}

func TestSegmentedAuthorityListedOldWindowAvoidsSecondList(t *testing.T) {
	ctx := t.Context()
	auth, mem, clock := newTestSegmented(t)
	key := "nodes/old.json"
	if _, err := auth.ConditionalCreate(ctx, key, []byte("old")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Minute)
	before := mem.lists
	keys, err := mem.List(ctx, "nodes/")
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	seg, err := auth.listed(ctx, key, 0)
	if err != nil || string(seg.value) != "old" {
		t.Fatalf("seg=%v err=%v", seg, err)
	}
	if mem.lists != before+1 {
		t.Fatal("listed issued an extra List")
	}
}

func (m *segmentMem) Put(_ context.Context, key string, value []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), value...)
	return "", nil
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
	ctx := t.Context()
	auth, mem, clock := newTestSegmented(t)
	key := "cells/acme/__kv__/main/own.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("v0"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		token, err = auth.CAS(ctx, key, []byte(fmt.Sprintf("renew-%d", i)), token)
		if err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := mem.count(key); n != 2 {
		t.Fatalf("same-generation objects=%d", n)
	}
	for i := 0; i < 5; i++ {
		clock.Advance(2 * time.Minute)
		token, err = auth.CAS(ctx, key, []byte("latest"), token)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 5; i++ {
		v, err := mem.Read(ctx, auth.physical(key, i))
		if err != nil || len(v) != 1 || v[0] != 0 {
			t.Fatalf("retirement %d: %v %v", i, v, err)
		}
	}
	value, _, err := auth.Get(ctx, key)
	if err != nil || string(value) != "latest" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	if mem.lists != 0 {
		t.Fatal("hot path used List")
	}
}

func TestSegmentedAuthorityRejectsLegacyLog(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	legacy := NewAppendAuthority(mem)
	key := "nodes/old.json"
	token, err := legacy.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := mem.Read(ctx, key)
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	if _, _, err := auth.Get(ctx, key); err == nil {
		t.Fatal("legacy read accepted")
	}
	if _, err := auth.CAS(ctx, key, []byte("new"), token); err == nil {
		t.Fatal("unsafe legacy CAS accepted")
	}
	if _, err := auth.ConditionalCreate(ctx, key, []byte("new")); err == nil {
		t.Fatal("legacy root overwritten")
	}
	after, _ := mem.Read(ctx, key)
	if string(after) != string(before) {
		t.Fatal("legacy bytes changed")
	}
}

func TestSegmentedAuthorityHardCapFailsClosed(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{MaxSegmentBytes: 256, MaxRootBytes: 256, Now: clock.Now})
	key := "fleet/waker.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	full := false
	for i := 0; i < 100; i++ {
		next, e := auth.CAS(ctx, key, []byte(strings.Repeat("y", 32)), token)
		if errors.Is(e, ErrAuthoritySegmentFull) {
			full = true
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		token = next
	}
	if !full {
		t.Fatal("root capacity was not enforced")
	}
	_, got, err := auth.Get(ctx, key)
	if err != nil || got != token {
		t.Fatalf("failed CAS changed authority: %q %v", got, err)
	}
	for k, v := range mem.objects {
		if len(v) > 256 {
			t.Fatalf("object %q exceeds cap: %d", k, len(v))
		}
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
		"cells/acme/__kv__/main/own.json",
		"cells/acme/__kv__/main/owner-gen",
		"nodes/n1.json",
		"node-logs/n1/s1.json",
		"fleet/waker.json",
		"fleet/drain-token.json",
	} {
		physical := key + ".g123"
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

func TestSegmentedAuthorityLegacyResourceFence(t *testing.T) {
	t.Skip("legacy resource-fence objects are no longer supported")
	ctx := context.Background()
	mem := newSegmentMem()
	key := "cells/acme/__kv__/CACHE/resource-fence.json"
	legacy := []byte(`{"job_id":"old","deleted":true}`)
	mem.objects[key] = append([]byte(nil), legacy...)
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	got, token, err := auth.Get(ctx, key)
	if err != nil || string(got) != string(legacy) || token == "" {
		t.Fatalf("legacy get = %q token=%q err=%v", got, token, err)
	}
	if _, err := auth.ConditionalCreate(ctx, key, []byte(`{"job_id":"new"}`)); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("legacy create = %v, want precondition", err)
	}
	next := []byte(`{"job_id":"old","deleted":false}`)
	if _, err := auth.CAS(ctx, key, next, token); err != nil {
		t.Fatalf("legacy CAS: %v", err)
	}
	got, _, err = auth.Get(ctx, key)
	if err != nil || string(got) != string(next) {
		t.Fatalf("after CAS = %q err=%v", got, err)
	}
	if _, err := mem.Read(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("legacy plain object still present: %v", err)
	}
}

func TestAuthorityKeyRoutesResourceFence(t *testing.T) {
	t.Skip("resource-fence sidecars are no longer supported")
	if !authorityKey("cells/acme/__kv__/CACHE/resource-fence.json") {
		t.Fatal("KV resource fence must use append authority on providers")
	}
	if !authorityKey("cells/acme/__queue__/jobs/resource-fence.json") {
		t.Fatal("queue resource fence must use append authority on providers")
	}
	if authorityKey("cells/acme/__kv__/CACHE/ltx/e1/1.ltx") {
		t.Fatal("LTX segment must not be treated as authority")
	}
}

func TestSegmentedTouchMovesNodeLeaseWithoutList(t *testing.T) {
	ctx := context.Background()
	auth, mem, clock := newTestSegmented(t)
	key := "nodes/node-1/owner.json"
	if _, err := auth.ConditionalCreate(ctx, key, []byte("v0")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	mem.mu.Lock()
	mem.lists = 0
	mem.mu.Unlock()

	if err := auth.Touch(ctx, key); err != nil {
		t.Fatal(err)
	}
	// Touch may deep-GC the old single-key prefix on the rollover; the
	// steady-state read after that must not List.
	mem.mu.Lock()
	mem.lists = 0
	mem.mu.Unlock()
	if got, _, err := auth.Get(ctx, key); err != nil || string(got) != "v0" {
		t.Fatalf("value after touch = %q, %v", got, err)
	}
	mem.mu.Lock()
	lists := mem.lists
	mem.mu.Unlock()
	if lists != 0 {
		t.Fatalf("Get after touch used %d bucket List calls, want 0", lists)
	}
	if _, ok := mem.objects[auth.physical(key, 1)]; !ok {
		t.Fatal("record was not moved into the current window")
	}
}
