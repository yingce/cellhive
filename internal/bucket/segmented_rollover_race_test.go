package bucket

import (
	"context"
	"errors"
	"testing"
	"time"
)

type rolloverReadHook struct {
	*segmentMem
	hook func(string)
}

func (m *rolloverReadHook) TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error) {
	if m.hook != nil {
		m.hook(key)
	}
	return m.segmentMem.TailGet(ctx, key, n)
}

// A writer that read the old head before rollover must not overwrite an
// intervening successful CAS merely because it appends into a different object.
func TestSegmentedRolloverRejectsInterveningOldWindowWrite(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	hooked := &rolloverAppendHook{segmentMem: mem}
	boundary := time.Unix(360000, 0)
	old := NewSegmentedAuthority(mem, mem, AuthorityOptions{Now: func() time.Time { return boundary.Add(-time.Millisecond) }})
	next := NewSegmentedAuthority(hooked, mem, AuthorityOptions{Now: func() time.Time { return boundary.Add(time.Millisecond) }})
	key := "cells/race/__kv__/a/own.json"
	token, err := old.ConditionalCreate(ctx, key, []byte("initial"))
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	hooked.hook = func(k string) {
		_, generation, ok := ParsePhysicalAuthorityKey(k)
		if !fired && ok && generation == 0 {
			fired = true
			if _, err := old.CAS(ctx, key, []byte("acknowledged"), token); err != nil {
				t.Fatalf("intervening CAS: %v", err)
			}
		}
	}
	_, err = next.CAS(ctx, key, []byte("stale"), token)
	if !fired {
		t.Fatal("race hook not reached")
	}
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale rollover CAS=%v; must reject", err)
	}
	value, _, err := next.Get(ctx, key)
	if err != nil || string(value) != "acknowledged" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

type rolloverAppendHook struct {
	*segmentMem
	hook func(string)
}

func (m *rolloverAppendHook) Append(ctx context.Context, key string, pos int64, data []byte) error {
	if m.hook != nil {
		m.hook(key)
	}
	return m.segmentMem.Append(ctx, key, pos, data)
}

func TestSegmentedInitialCreateAcrossBoundaryHasOneWinner(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	hooked := &rolloverAppendHook{segmentMem: mem}
	boundary := time.Unix(360000, 0)
	old := NewSegmentedAuthority(mem, mem, AuthorityOptions{Now: func() time.Time { return boundary.Add(-time.Millisecond) }})
	next := NewSegmentedAuthority(hooked, mem, AuthorityOptions{Now: func() time.Time { return boundary.Add(time.Millisecond) }})
	key := "cells/race/__kv__/create/own.json"
	fired := false
	hooked.hook = func(k string) {
		if !fired && k == key {
			fired = true
			if _, err := old.ConditionalCreate(ctx, key, []byte("first-owner")); err != nil {
				t.Fatalf("first create: %v", err)
			}
		}
	}
	_, err := next.ConditionalCreate(ctx, key, []byte("second-owner"))
	if !fired {
		t.Fatal("race hook not reached")
	}
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second create=%v; two owners accepted", err)
	}
}
