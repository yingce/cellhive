package bucket

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type authorityReadCounter struct {
	*segmentMem
	tails atomic.Int32
}

func (m *authorityReadCounter) TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error) {
	m.tails.Add(1)
	return m.segmentMem.TailGet(ctx, key, n)
}
func TestAuthorityWarmReadUsesOneTailAndFollowsRollover(t *testing.T) {
	mem := newSegmentMem()
	count := &authorityReadCounter{segmentMem: mem}
	now := time.Unix(360000, 0)
	opts := AuthorityOptions{Now: func() time.Time { return now }}
	writer := NewSegmentedAuthority(mem, mem, opts)
	reader := NewSegmentedAuthority(mem, count, opts)
	key := "cells/cache/__kv__/a/own.json"
	token, err := writer.ConditionalCreate(t.Context(), key, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if v, _, err := reader.Get(t.Context(), key); err != nil || string(v) != "one" {
		t.Fatal(err)
	}
	count.tails.Store(0)
	if v, _, err := reader.Get(t.Context(), key); err != nil || string(v) != "one" {
		t.Fatal(err)
	}
	if n := count.tails.Load(); n != 1 {
		t.Fatalf("warm read tails=%d want1", n)
	}
	now = now.Add(time.Hour)
	token, err = writer.CAS(t.Context(), key, []byte("two"), token)
	if err != nil {
		t.Fatal(err)
	}
	if v, _, err := reader.Get(t.Context(), key); err != nil || string(v) != "two" {
		t.Fatalf("rollover value=%q err=%v", v, err)
	}
	if err := writer.ConditionalDelete(t.Context(), key, token); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(t.Context(), key); err != ErrNotFound {
		t.Fatalf("deleted=%v", err)
	}
	if _, err := writer.ConditionalCreate(t.Context(), key, []byte("three")); err != nil {
		t.Fatal(err)
	}
	if v, _, err := reader.Get(t.Context(), key); err != nil || string(v) != "three" {
		t.Fatalf("recreated value=%q err=%v", v, err)
	}
}

func TestAuthorityReadLocatorCacheIsBoundedAndDropsPayload(t *testing.T) {
	mem := newSegmentMem()
	s := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	for i := 0; i < 4100; i++ {
		key := fmt.Sprintf("nodes/cache-%d", i)
		if _, err := s.ConditionalCreate(t.Context(), key, []byte("payload")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Get(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if len(s.readRoots) > 4096 {
		t.Fatalf("cache entries=%d", len(s.readRoots))
	}
	for key, r := range s.readRoots {
		if len(r.seed) != 0 {
			t.Fatalf("retained bootstrap payload for %s", key)
		}
	}
}

func TestAuthorityCachedReadersDuringRollover(t *testing.T) {
	mem := newSegmentMem()
	var clock atomic.Int64
	clock.Store(360000)
	opts := AuthorityOptions{Now: func() time.Time { return time.Unix(clock.Load(), 0) }}
	writer := NewSegmentedAuthority(mem, mem, opts)
	reader := NewSegmentedAuthority(mem, mem, opts)
	key := "nodes/readers"
	token, err := writer.ConditionalCreate(t.Context(), key, []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := 0
			for j := 0; j < 100; j++ {
				value, _, err := reader.Get(t.Context(), key)
				n, parseErr := strconv.Atoi(string(value))
				if err != nil || parseErr != nil || n < last || n > 20 {
					t.Errorf("read value=%q err=%v", value, err)
					return
				}
				last = n
			}
		}()
	}
	for i := 0; i < 20; i++ {
		clock.Add(3600)
		token, err = writer.CAS(t.Context(), key, []byte(strconv.Itoa(i+1)), token)
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestAuthorityWarmReaderRecoversCommittedSeal(t *testing.T) {
	mem := newSegmentMem()
	store := &authorityFaultStore{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	opts := AuthorityOptions{Segment: time.Minute, Now: clock.Now}
	writer := NewSegmentedAuthority(store, store, opts)
	reader := NewSegmentedAuthority(mem, mem, opts)
	key := "nodes/warm-seal"
	token, err := writer.ConditionalCreate(t.Context(), key, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	store.after = func(_ string, _ int64, b []byte) error {
		op, _, _, err := decodeAuthorityFrame(b)
		if err != nil {
			return err
		}
		if op == authoritySeal {
			return errInjectedAuthorityCrash
		}
		return nil
	}
	if _, err := writer.CAS(t.Context(), key, []byte("after"), token); !errors.Is(err, errInjectedAuthorityCrash) {
		t.Fatal(err)
	}
	value, _, err := reader.Get(t.Context(), key)
	if err != nil || string(value) != "after" {
		t.Fatalf("warm sealed read=%q err=%v", value, err)
	}
}

func TestAuthorityWarmReaderRejectsCurrentCorruption(t *testing.T) {
	mem := newSegmentMem()
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	key := "nodes/warm-corruption"
	if _, err := auth.ConditionalCreate(t.Context(), key, []byte("valid")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := auth.Get(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Put(t.Context(), auth.physical(key, 0), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if value, _, err := auth.Get(t.Context(), key); err == nil {
		t.Fatalf("corruption returned value=%q", value)
	}
}
