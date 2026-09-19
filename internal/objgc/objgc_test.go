package objgc

import (
	"context"
	"testing"
	"time"
)

type fakeItems struct {
	ids     []string
	deleted []string
}

func (f *fakeItems) List(context.Context) ([]string, error) { return f.ids, nil }
func (f *fakeItems) Delete(_ context.Context, id string) error {
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeRefs struct {
	refs  map[string]bool
	marks map[string]int64
}

func (f *fakeRefs) Referenced(context.Context) (map[string]bool, error) { return f.refs, nil }
func (f *fakeRefs) Mark(_ context.Context, id string) (int64, bool, error) {
	ms, ok := f.marks[id]
	return ms, ok, nil
}
func (f *fakeRefs) SetMark(_ context.Context, id string, ms int64) error {
	f.marks[id] = ms
	return nil
}
func (f *fakeRefs) ClearMark(_ context.Context, id string) error {
	delete(f.marks, id)
	return nil
}

// TestPassMarksThenDeletesAfterGrace covers the two-phase GC: an unreferenced
// item is marked first, kept through the grace period, then deleted; a
// referenced item is never touched.
func TestPassMarksThenDeletesAfterGrace(t *testing.T) {
	ctx := context.Background()
	it := &fakeItems{ids: []string{"ref", "orphan"}}
	r := &fakeRefs{refs: map[string]bool{"ref": true}, marks: map[string]int64{}}
	t0 := time.Unix(1_700_000_000, 0)
	clock := t0
	g := &GC{Items: it, Refs: r, Grace: time.Hour, Now: func() time.Time { return clock }}

	res, err := g.Pass(ctx)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Total != 2 || res.Referenced != 1 || res.Marked != 1 || res.Deleted != 0 {
		t.Fatalf("pass 1 = %+v", res)
	}
	if len(it.deleted) != 0 {
		t.Fatalf("nothing should be deleted on the marking pass: %v", it.deleted)
	}
	if _, ok := r.marks["orphan"]; !ok {
		t.Fatal("orphan was not marked")
	}

	// Within the grace period: still kept.
	clock = t0.Add(30 * time.Minute)
	if res, _ = g.Pass(ctx); res.Deleted != 0 {
		t.Fatalf("deleted inside grace: %+v", res)
	}

	// Past the grace period: deleted and mark cleared.
	clock = t0.Add(2 * time.Hour)
	res, err = g.Pass(ctx)
	if err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("pass 3 = %+v, want 1 deleted", res)
	}
	if len(it.deleted) != 1 || it.deleted[0] != "orphan" {
		t.Fatalf("deleted = %v", it.deleted)
	}
	if _, ok := r.marks["orphan"]; ok {
		t.Fatal("mark not cleared after delete")
	}
}

// TestPassKeepsReferencedItemWithStaleMark covers an item that was marked (e.g.
// an upload that raced a deploy) and is now referenced: the mark is cleared and
// it is never deleted, even with no grace period.
func TestPassKeepsReferencedItemWithStaleMark(t *testing.T) {
	ctx := context.Background()
	it := &fakeItems{ids: []string{"id1"}}
	r := &fakeRefs{refs: map[string]bool{"id1": true}, marks: map[string]int64{"id1": 1}}
	g := &GC{Items: it, Refs: r, Grace: 0, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}

	res, err := g.Pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if res.Referenced != 1 || res.Deleted != 0 || len(it.deleted) != 0 {
		t.Fatalf("res = %+v, deleted %v", res, it.deleted)
	}
	if _, ok := r.marks["id1"]; ok {
		t.Fatal("stale mark not cleared for a referenced item")
	}
}

// TestPassMarksNewlyUnreferenced: an item that becomes unreferenced is only
// marked on that pass (Grace=0 deletes on the following pass, never before a
// deploy could reference it).
func TestPassMarksNewlyUnreferenced(t *testing.T) {
	ctx := context.Background()
	it := &fakeItems{ids: []string{"id1"}}
	r := &fakeRefs{refs: map[string]bool{}, marks: map[string]int64{}}
	g := &GC{Items: it, Refs: r, Grace: 0, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}

	res, err := g.Pass(ctx)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Marked != 1 || len(it.deleted) != 0 {
		t.Fatalf("marking pass = %+v, deleted %v", res, it.deleted)
	}
	res, err = g.Pass(ctx)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if res.Deleted != 1 || len(it.deleted) != 1 {
		t.Fatalf("pass 2 = %+v, deleted %v", res, it.deleted)
	}
}
