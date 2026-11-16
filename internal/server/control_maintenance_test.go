package server

import (
	"testing"
	"time"

	"cellhive/internal/workflow"
)

func TestWorkflowMaintenanceCannotPruneNonOwnerLocalCopy(t *testing.T) {
	dir := t.TempDir()
	a, oa, ha := newControlForwardServer(t, dir, "owner")
	b, _, _ := newControlForwardServer(t, dir, "follower")
	t.Cleanup(func() { _ = a.Store.Close(); _ = b.Store.Close() })
	ctx := t.Context()
	for _, s := range []*Server{a, b} {
		if _, err := s.Workflows.Create(ctx, "acme", "WF", "instance", nil); err != nil {
			t.Fatal(err)
		}
		if err := s.Workflows.SetStatus(ctx, "acme", "WF", "instance", workflow.StatusComplete, ""); err != nil {
			t.Fatal(err)
		}
	}
	oa.Advertise = ha.URL
	sc := workflow.Scope("acme", "WF")
	if _, err := oa.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(time.Hour).UnixMilli()
	n, err := b.RunOwnedMaintenance(ctx, sc, func() (int, error) { return b.Workflows.PruneRetention(ctx, "acme", "WF", cutoff, cutoff) })
	if err != nil || n != 0 {
		t.Fatalf("non-owner pruned local state: n=%d err=%v", n, err)
	}
	if _, err := b.Workflows.Get(ctx, "acme", "WF", "instance"); err != nil {
		t.Fatalf("non-owner copy changed: %v", err)
	}
	n, err = a.RunOwnedMaintenance(ctx, sc, func() (int, error) { return a.Workflows.PruneRetention(ctx, "acme", "WF", cutoff, cutoff) })
	if err != nil || n != 1 {
		t.Fatalf("owner maintenance: n=%d err=%v", n, err)
	}
}
