package nodelog

import (
	"context"
	"testing"

	"cellhive/internal/bucket"
)

func newMgr(t *testing.T) *Manager {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(b, "node-1")
}

func TestOpenGetSeal(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)

	r, err := m.Open(ctx, "s1", 3, []string{"n2"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if r.Status != StatusOpen || r.Epoch != 3 || len(r.Followers) != 1 {
		t.Fatalf("record = %+v", r)
	}

	// Opening again is a no-op returning the existing record.
	again, err := m.Open(ctx, "s1", 3, []string{"n2"})
	if err != nil || again.Session != "s1" {
		t.Fatalf("reopen: %+v %v", again, err)
	}

	got, _, err := m.Get(ctx, "node-1", "s1")
	if err != nil || got.Status != StatusOpen {
		t.Fatalf("get: %+v %v", got, err)
	}

	if _, err := m.SetStatus(ctx, "node-1", "s1", StatusRecovering); err != nil {
		t.Fatalf("recovering: %v", err)
	}
	sealed, err := m.Seal(ctx, "node-1", "s1")
	if err != nil || sealed.Status != StatusSealed {
		t.Fatalf("seal: %+v %v", sealed, err)
	}

	list, err := m.ListNode(ctx, "node-1")
	if err != nil || len(list) != 1 || list[0].Status != StatusSealed {
		t.Fatalf("list: %+v %v", list, err)
	}
}

func TestNodesListsDistinctNodesSorted(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	a := New(b, "node-a")
	if _, err := a.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := a.Open(ctx, "s2", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	nb := New(b, "node-b")
	if _, err := nb.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	// A reader with a different NodeID still enumerates every node.
	got, err := New(b, "reader").Nodes(ctx)
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	if len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("nodes = %v; want [node-a node-b]", got)
	}
}
