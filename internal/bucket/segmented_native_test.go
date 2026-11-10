package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var errInjectedAuthorityCrash = errors.New("injected authority crash")

type authorityFaultStore struct {
	*segmentMem
	before func(string, int64, []byte) error
	after  func(string, int64, []byte) error
}

func (m *authorityFaultStore) Append(ctx context.Context, key string, pos int64, data []byte) error {
	if m.before != nil {
		if err := m.before(key, pos, data); err != nil {
			return err
		}
	}
	if err := m.segmentMem.Append(ctx, key, pos, data); err != nil {
		return err
	}
	if m.after != nil {
		return m.after(key, pos, data)
	}
	return nil
}
func (m *authorityFaultStore) Read(context.Context, string) ([]byte, error) {
	panic("authority must not download complete history")
}

func TestSegmentedRecoveryAtEveryPublicationStage(t *testing.T) {
	for _, stage := range []string{"bootstrap", "seal", "genesis", "descriptor"} {
		t.Run(stage, func(t *testing.T) {
			ctx := t.Context()
			mem := newSegmentMem()
			store := &authorityFaultStore{segmentMem: mem}
			clock := &testClock{t: time.Unix(1700000000, 0)}
			auth := NewSegmentedAuthority(store, store, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
			key := "nodes/recovery.json"
			if stage == "bootstrap" {
				store.after = func(k string, _ int64, _ []byte) error {
					if k == key {
						return errInjectedAuthorityCrash
					}
					return nil
				}
				if _, err := auth.ConditionalCreate(ctx, key, []byte("new")); !errors.Is(err, errInjectedAuthorityCrash) {
					t.Fatal(err)
				}
			} else {
				token, err := auth.ConditionalCreate(ctx, key, []byte("old"))
				if err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Minute)
				store.after = func(k string, _ int64, b []byte) error {
					op, _, _, e := decodeAuthorityFrame(b)
					if e != nil {
						return e
					}
					if stage == "seal" && op == authoritySeal || stage == "genesis" && k == auth.physical(key, 1) || stage == "descriptor" && op == authorityDescriptor {
						return errInjectedAuthorityCrash
					}
					return nil
				}
				if _, err := auth.CAS(ctx, key, []byte("new"), token); !errors.Is(err, errInjectedAuthorityCrash) {
					t.Fatalf("crash=%v", err)
				}
			}
			store.after = nil
			got, token, err := auth.Get(ctx, key)
			if err != nil || string(got) != "new" {
				t.Fatalf("recovery=%q %v", got, err)
			}
			if _, err := auth.CAS(ctx, key, []byte("continued"), token); err != nil {
				t.Fatal(err)
			}
			if mem.lists != 0 {
				t.Fatal("recovery used List")
			}
		})
	}
}

func TestSegmentedConcurrentSealHelpers(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	store := &authorityFaultStore{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(store, store, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	key := "nodes/helpers.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	rootBefore, _ := mem.Read(ctx, key)
	clock.Advance(time.Minute)
	store.after = func(_ string, _ int64, b []byte) error {
		if b[4] == authoritySeal {
			return errInjectedAuthorityCrash
		}
		return nil
	}
	if _, err = auth.CAS(ctx, key, []byte("sealed"), token); !errors.Is(err, errInjectedAuthorityCrash) {
		t.Fatal(err)
	}
	store.after = nil
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, e := auth.Get(ctx, key)
			if e == nil && string(v) != "sealed" {
				e = fmt.Errorf("value=%q", v)
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	rootAfter, _ := mem.Read(ctx, key)
	if len(rootAfter)-len(rootBefore) != rootDescriptorBytes {
		t.Fatal("helpers advanced root more than once")
	}
	if _, err := auth.CAS(ctx, key, []byte("stale"), token); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("old CAS=%v", err)
	}
}

func TestSegmentedDelayedGenesisCannotResurrectRetiredGeneration(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	store := &authorityFaultStore{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(store, store, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	helper := NewSegmentedAuthority(mem, mem, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	key := "nodes/aba.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("zero"))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	fired := false
	store.before = func(k string, pos int64, _ []byte) error {
		if !fired && k == auth.physical(key, 1) && pos == 0 {
			fired = true
			v, tok, e := helper.Get(ctx, key)
			if e != nil || string(v) != "one" {
				t.Fatalf("help=%q %v", v, e)
			}
			clock.Advance(time.Minute)
			if _, e = helper.CAS(ctx, key, []byte("two"), tok); e != nil {
				t.Fatal(e)
			}
		}
		return nil
	}
	if _, err := auth.CAS(ctx, key, []byte("one"), token); err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("hook not reached")
	}
	retired, _ := mem.Read(ctx, auth.physical(key, 1))
	if !bytes.Equal(retired, []byte{0}) {
		t.Fatalf("retired generation resurrected: %x", retired)
	}
	v, _, err := auth.Get(ctx, key)
	if err != nil || string(v) != "two" {
		t.Fatalf("value=%q err=%v", v, err)
	}
}

func TestSegmentedDelayedOldPositionCannotWriteRetiredGeneration(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	store := &authorityFaultStore{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(store, store, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	helper := NewSegmentedAuthority(mem, mem, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	key := "nodes/old-position.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("zero"))
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	store.before = func(k string, pos int64, _ []byte) error {
		if !fired && k == auth.physical(key, 0) && pos > 0 {
			fired = true
			clock.Advance(time.Minute)
			if _, e := helper.CAS(ctx, key, []byte("winner"), token); e != nil {
				t.Fatal(e)
			}
		}
		return nil
	}
	if _, err := auth.CAS(ctx, key, []byte("stale"), token); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale append=%v", err)
	}
	if !fired {
		t.Fatal("hook not reached")
	}
	v, _, err := auth.Get(ctx, key)
	if err != nil || string(v) != "winner" {
		t.Fatalf("value=%q err=%v", v, err)
	}
}

func TestSegmentedDeleteAndRecreateAcrossGenerations(t *testing.T) {
	ctx := t.Context()
	auth, mem, clock := newTestSegmented(t)
	key := "nodes/tombstone.json"
	first, err := auth.ConditionalCreate(ctx, key, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := auth.ConditionalDelete(ctx, key, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := auth.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	rootBefore, _ := mem.Read(ctx, key)
	clock.Advance(time.Minute)
	second, err := auth.ConditionalCreate(ctx, key, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("reused token")
	}
	if _, err := auth.CAS(ctx, key, []byte("stale"), first); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	if err := auth.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	rootAfter, _ := mem.Read(ctx, key)
	if len(rootAfter) <= len(rootBefore) {
		t.Fatal("logical delete removed/reset root")
	}
	if _, err := auth.ConditionalCreate(ctx, key, []byte("third")); err != nil {
		t.Fatal(err)
	}
	if mem.lists != 0 {
		t.Fatal("logical delete used List")
	}
}

type authorityReadMeter struct {
	*segmentMem
	tailBytes, rangeBytes, tailCalls int
}

func (m *authorityReadMeter) TailGet(ctx context.Context, k string, n int) ([]byte, int64, string, error) {
	b, total, t, e := m.segmentMem.TailGet(ctx, k, n)
	m.tailCalls++
	m.tailBytes += len(b)
	return b, total, t, e
}
func (m *authorityReadMeter) RangedGet(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	b, t, e := m.segmentMem.RangedGet(ctx, k, o, n)
	m.rangeBytes += len(b)
	return b, t, e
}
func TestSegmentedColdReadsBoundedAfterManyEpochs(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	meter := &authorityReadMeter{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, meter, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
	key := "nodes/many.json"
	token, err := auth.ConditionalCreate(ctx, key, []byte("start"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		clock.Advance(time.Minute)
		token, err = auth.CAS(ctx, key, []byte("value"), token)
		if err != nil {
			t.Fatal(err)
		}
	}
	meter.tailBytes = 0
	meter.rangeBytes = 0
	meter.tailCalls = 0
	clock.Advance(365 * 24 * time.Hour)
	v, _, err := auth.Get(ctx, key)
	if err != nil || string(v) != "value" {
		t.Fatalf("value=%q err=%v", v, err)
	}
	if meter.tailCalls != 2 || meter.tailBytes > 2*segProbeBytes || meter.rangeBytes != 0 || mem.lists != 0 {
		t.Fatalf("I/O calls=%d tail=%d range=%d lists=%d", meter.tailCalls, meter.tailBytes, meter.rangeBytes, mem.lists)
	}
}

func TestSegmentedLargeValueUsesBoundedRangeRead(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	meter := &authorityReadMeter{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, meter, AuthorityOptions{Now: clock.Now})
	key := "nodes/large.json"
	value := bytes.Repeat([]byte("a"), authorityMaxValue)
	token, err := auth.ConditionalCreate(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if _, err := auth.CAS(ctx, key, value, token); err != nil {
		t.Fatal(err)
	}
	meter.tailBytes = 0
	meter.rangeBytes = 0
	meter.tailCalls = 0
	got, _, err := auth.Get(ctx, key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("large value length=%d err=%v", len(got), err)
	}
	if meter.tailCalls != 2 || meter.tailBytes > 2*segProbeBytes || meter.rangeBytes != len(value)+segOverhead {
		t.Fatalf("I/O tails=%d bytes=%d range=%d", meter.tailCalls, meter.tailBytes, meter.rangeBytes)
	}
	before, _ := mem.Read(ctx, key)
	if _, err := auth.Put(ctx, key, append(value, 'b')); !errors.Is(err, ErrAuthoritySegmentFull) {
		t.Fatalf("overlarge write=%v", err)
	}
	after, _ := mem.Read(ctx, key)
	if !bytes.Equal(before, after) {
		t.Fatal("oversized mutation changed root")
	}
}

type authoritySnapshotHook struct {
	*segmentMem
	after func(string)
}

func (m *authoritySnapshotHook) TailGet(ctx context.Context, k string, n int) ([]byte, int64, string, error) {
	b, total, t, e := m.segmentMem.TailGet(ctx, k, n)
	if m.after != nil {
		m.after(k)
	}
	return b, total, t, e
}
func TestSegmentedReaderRetriesAfterSelectedGenerationRetired(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	hook := &authoritySnapshotHook{segmentMem: mem}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	writer := NewSegmentedAuthority(mem, mem, AuthorityOptions{Now: clock.Now})
	reader := NewSegmentedAuthority(mem, hook, AuthorityOptions{Now: clock.Now})
	key := "nodes/read-retire.json"
	token, err := writer.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	hook.after = func(k string) {
		if !fired && k == key {
			fired = true
			clock.Advance(time.Hour)
			if _, e := writer.CAS(ctx, key, []byte("new"), token); e != nil {
				t.Fatal(e)
			}
		}
	}
	v, _, err := reader.Get(ctx, key)
	if err != nil || string(v) != "new" {
		t.Fatalf("read=%q err=%v", v, err)
	}
	if !fired {
		t.Fatal("snapshot hook not reached")
	}
}
