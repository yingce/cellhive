package bucket

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func reclaimTestAuthority(mem *segmentMem, clock *testClock) *SegmentedAuthority {
	return NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: clock.Now})
}

func TestReclaimReaderRejectsResurrectedGeneration(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	writer := reclaimTestAuthority(mem, clock)
	reader := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-reader.json"
	tok, err := writer.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	if _, _, err = reader.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err = writer.CAS(ctx, key, []byte("new"), tok); err != nil {
		t.Fatal(err)
	}
	// A canceled create operation arrives only after physical reclamation.
	if err = mem.Delete(ctx, key+".g0"); err != nil {
		t.Fatal(err)
	}
	if err = mem.Append(ctx, key+".g0", 0, genesis); err != nil {
		t.Fatal(err)
	}
	got, _, err := reader.Get(ctx, key)
	if err != nil || string(got) != "new" {
		t.Fatalf("returned retired authority: %q %v", got, err)
	}
}

func TestReclaimDelayedWriteCannotAcknowledgeRetiredGeneration(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-writer.json"
	tok, err := auth.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	old, err := auth.latest(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	clock.Advance(time.Minute)
	if _, err = auth.CAS(ctx, key, []byte("new"), tok); err != nil {
		t.Fatal(err)
	}
	if err = mem.Delete(ctx, key+".g0"); err != nil {
		t.Fatal(err)
	}
	if err = mem.Append(ctx, key+".g0", 0, genesis); err != nil {
		t.Fatal(err)
	}
	// The suspended old writer still has its original read result and clock.
	delayed := NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true, Now: func() time.Time { return time.Unix(1700000000, 0) }, Segment: time.Minute})
	_, err = delayed.mutate(ctx, old, []byte("stale"), false)
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("acknowledged stale write: %v", err)
	}
}

func TestReclaimRotationsKeepOnlyCurrentSegment(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-bounded.json"
	tok, err := auth.ConditionalCreate(ctx, key, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 105; i++ {
		clock.Advance(time.Minute)
		tok, err = auth.CAS(ctx, key, []byte("v"), tok)
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := mem.count(key); n != 2 {
			t.Fatalf("generation %d: %d objects, want root+current", i+1, n)
		}
	}
	if mem.lists != 0 {
		t.Fatal("request path used List")
	}
	root, err := auth.readRoot(ctx, key)
	if err != nil || root.generation != 105 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
}

func TestReclaimCollectorRemovesOnlyRetiredExactKeys(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-collect.json"
	tok, err := auth.ConditionalCreate(ctx, key, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	for i := 0; i < 3; i++ {
		clock.Advance(time.Minute)
		tok, err = auth.CAS(ctx, key, []byte("latest"), tok)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{key + ".g0", key + ".g1", key + ".g4", key + ".g0-other"} {
		if err := mem.Append(ctx, k, 0, genesis); err != nil {
			t.Fatal(err)
		}
	}
	if n, _, err := auth.CollectRetiredPage(ctx, key, "", 1); err != nil || n != 1 {
		t.Fatalf("batch1 %d %v", n, err)
	}
	if n, _, err := auth.CollectRetiredPage(ctx, key, "", 8); err != nil || n != 1 {
		t.Fatalf("batch2 %d %v", n, err)
	}
	for _, k := range []string{key, key + ".g3", key + ".g4", key + ".g0-other"} {
		if _, err := mem.Read(ctx, k); err != nil {
			t.Fatal(k, err)
		}
	}
	if n, _, err := auth.CollectRetiredPage(ctx, key, "", 8); err != nil || n != 0 {
		t.Fatalf("repeat %d %v", n, err)
	}
	legacy := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	if _, _, err := legacy.CollectRetiredPage(ctx, key, "", 8); !errors.Is(err, ErrNotSupported) {
		t.Fatal("default enabled GC", err)
	}
}

func TestReclaimReadChecksRootAfterSegmentRead(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	writer := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-read-race.json"
	tok, err := writer.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	hooked := &rolloverReadHook{segmentMem: mem}
	reader := NewSegmentedAuthority(mem, hooked, AuthorityOptions{ReclaimRetired: true, Now: clock.Now})
	fired := false
	hooked.hook = func(k string) {
		if !fired && k == key+".g0" {
			fired = true
			clock.Advance(time.Minute)
			if _, err := writer.CAS(ctx, key, []byte("new"), tok); err != nil {
				t.Fatal(err)
			}
			if err := mem.Append(ctx, key+".g0", 0, genesis); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, _, err := reader.Get(ctx, key)
	if !fired || err != nil || string(got) != "new" {
		t.Fatalf("race returned %q %v", got, err)
	}
}

func TestReclaimDelayedSealCannotPublishOldToken(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-seal.json"
	tok, err := auth.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	old, err := auth.latest(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	clock.Advance(time.Minute)
	tok, err = auth.CAS(ctx, key, []byte("new"), tok)
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Append(ctx, key+".g0", 0, genesis); err != nil {
		t.Fatal(err)
	}
	// Old writer now appends a different seal into the resurrected predecessor.
	if _, err := auth.mutate(ctx, old, []byte("stale-seal"), false); !errors.Is(err, ErrPrecondition) {
		t.Fatal("stale seal acknowledged", err)
	}
	got, current, err := auth.Get(ctx, key)
	if err != nil || string(got) != "new" || current != tok {
		t.Fatalf("current=%q %s %v", got, current, err)
	}
}

func TestReclaimDelayedInitialCreateCannotReturnOldToken(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	helper := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-bootstrap.json"
	hooked := &rolloverAppendHook{segmentMem: mem}
	creator := NewSegmentedAuthority(hooked, mem, AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: clock.Now})
	fired := false
	hooked.hook = func(k string) {
		if !fired && k == key+".g0" {
			fired = true
			_, tok, err := helper.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Minute)
			if _, err := helper.CAS(ctx, key, []byte("new"), tok); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := creator.ConditionalCreate(ctx, key, []byte("old")); !errors.Is(err, ErrPrecondition) {
		t.Fatal("obsolete bootstrap accepted", err)
	}
	got, _, err := helper.Get(ctx, key)
	if err != nil || string(got) != "new" {
		t.Fatalf("value=%q err=%v", got, err)
	}
	if n, _, err := helper.CollectRetiredPage(ctx, key, "", 8); err != nil || n != 1 {
		t.Fatalf("collect recreated genesis %d %v", n, err)
	}
}

func TestReclaimWarmReadUsesSegmentThenRoot(t *testing.T) {
	mem := newSegmentMem()
	counter := &authorityReadCounter{segmentMem: mem}
	auth := NewSegmentedAuthority(mem, counter, AuthorityOptions{ReclaimRetired: true})
	if _, err := auth.ConditionalCreate(t.Context(), "nodes/reclaim-cost.json", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := auth.Get(t.Context(), "nodes/reclaim-cost.json"); err != nil {
		t.Fatal(err)
	}
	counter.tails.Store(0)
	if _, _, err := auth.Get(t.Context(), "nodes/reclaim-cost.json"); err != nil {
		t.Fatal(err)
	}
	if n := counter.tails.Load(); n != 2 {
		t.Fatalf("read operations=%d want2", n)
	}
}

func TestReclaimConcurrentReadersAndCollector(t *testing.T) {
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	writer := reclaimTestAuthority(mem, clock)
	reader := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-concurrent.json"
	tok, err := writer.ConditionalCreate(t.Context(), key, []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := 0
			for j := 0; j < 100; j++ {
				got, _, err := reader.Get(t.Context(), key)
				if errors.Is(err, ErrPrecondition) {
					continue
				} // Bounded retry exhaustion under churn.
				n, e := strconv.Atoi(string(got))
				if err != nil || e != nil || n < last {
					t.Errorf("nonmonotonic read %q after %d: %v", got, last, err)
					return
				}
				last = n
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, _, err := writer.CollectRetiredPage(t.Context(), key, "", 8); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 1; i <= 50; i++ {
		clock.Advance(time.Minute)
		tok, err = writer.CAS(t.Context(), key, []byte(strconv.Itoa(i)), tok)
		if err != nil {
			// Readers can publish the same committed seal first. Resolve only its
			// exact attempted value, as a production upper-layer caller must do.
			got, current, e := reader.Get(t.Context(), key)
			if e != nil || string(got) != strconv.Itoa(i) {
				t.Errorf("write %d: %v confirm %q %v", i, err, got, e)
				break
			}
			tok = current
		}
	}
	wg.Wait()
	if _, _, err := writer.CollectRetiredPage(t.Context(), key, "", 100); err != nil {
		t.Fatal(err)
	}
	if n, _ := mem.count(key); n != 2 {
		t.Fatalf("remaining objects=%d", n)
	}
}

func TestReclaimWarmReadValidatesAfterSegment(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	writer := reclaimTestAuthority(mem, clock)
	key := "nodes/reclaim-warm-race.json"
	tok, err := writer.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	hooked := &rolloverReadHook{segmentMem: mem}
	reader := NewSegmentedAuthority(mem, hooked, AuthorityOptions{ReclaimRetired: true, Now: clock.Now})
	if _, _, err := reader.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	fired := false
	hooked.hook = func(k string) {
		if fired || k != key+".g0" {
			return
		}
		fired = true
		clock.Advance(time.Minute)
		if _, err := writer.CAS(ctx, key, []byte("new"), tok); err != nil {
			t.Fatal(err)
		}
		if err := mem.Append(ctx, key+".g0", 0, genesis); err != nil {
			t.Fatal(err)
		}
	}
	got, _, err := reader.Get(ctx, key)
	if !fired || err != nil || string(got) != "new" {
		t.Fatalf("warm race returned %q %v", got, err)
	}
}

func TestReclaimWarmReadCannotUseValueWithoutRoot(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			mem := newSegmentMem()
			auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true})
			key := "nodes/reclaim-root-required.json"
			if _, err := auth.ConditionalCreate(t.Context(), key, []byte("cached")); err != nil {
				t.Fatal(err)
			}
			if _, _, err := auth.Get(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			// An unavailable/corrupt root cannot grant authority via the locator.
			if missing {
				_ = mem.Delete(t.Context(), key)
			} else {
				_, _ = mem.Put(t.Context(), key, []byte("broken"))
			}
			got, _, err := auth.Get(t.Context(), key)
			if err == nil || got != nil {
				t.Fatalf("unvalidated value %q, error %v", got, err)
			}
		})
	}
}
