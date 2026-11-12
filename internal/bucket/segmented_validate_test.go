package bucket

import (
	"errors"
	"testing"
	"time"
)

func TestValidateModeCoexistsWithReclaim(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	opts := AuthorityOptions{ValidateRoots: true, Segment: time.Minute, Now: clock.Now}
	reader := NewSegmentedAuthority(mem, mem, opts)
	key := "nodes/validate-rollout.json"
	tok, err := reader.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := mem.Read(ctx, key+".g0")
	clock.Advance(time.Minute)
	tok, err = reader.CAS(ctx, key, []byte("stage1"), tok)
	if err != nil {
		t.Fatal(err)
	}
	stub, err := mem.Read(ctx, key+".g0")
	if err != nil || len(stub) != 1 {
		t.Fatalf("validate stage must retain stub: %v %d", err, len(stub))
	}
	if _, _, err = reader.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	opts.ReclaimRetired = true
	writer := NewSegmentedAuthority(mem, mem, opts)
	clock.Advance(time.Minute)
	if _, err = writer.CAS(ctx, key, []byte("stage2"), tok); err != nil {
		t.Fatal(err)
	}
	if _, err = mem.Read(ctx, key+".g1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old generation not deleted: %v", err)
	}
	// Recreate the reader's cached segment with stale bytes; it must validate root.
	if err = mem.Append(ctx, key+".g1", 0, genesis); err != nil {
		t.Fatal(err)
	}
	got, _, err := reader.Get(ctx, key)
	if err != nil || string(got) != "stage2" {
		t.Fatalf("stale authority %q %v", got, err)
	}
}
