package workflow

import (
	"context"
	"testing"
)

// TestStats covers the ADR-157 workflow stats: cheap disk accounting plus the
// opt-in exact status breakdown.
func TestStats(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.Create(ctx, "acme", "wf", "", nil); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	ids := []string{}
	inst, err := s.List(ctx, "acme", "wf", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, in := range inst {
		ids = append(ids, in.ID)
	}
	if len(ids) != 3 {
		t.Fatalf("instances = %d, want 3", len(ids))
	}
	if err := s.SetStatus(ctx, "acme", "wf", ids[0], StatusComplete, ""); err != nil {
		t.Fatalf("set status: %v", err)
	}

	st, err := s.Stats(ctx, "acme", "wf", StatsOptions{})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.PageSize <= 0 || st.PageCount <= 0 || st.TotalBytes <= 0 {
		t.Fatalf("disk stats empty: %+v", st.DiskStats)
	}
	if st.Exact || st.Instances != 0 {
		t.Fatalf("non-exact stats must not count: %+v", st)
	}
	if st.InstancesEstimate == 0 && st.Note == "" {
		t.Fatalf("no estimate and no note: %+v", st)
	}

	ex, err := s.Stats(ctx, "acme", "wf", StatsOptions{Exact: true})
	if err != nil {
		t.Fatalf("stats exact: %v", err)
	}
	if !ex.Exact || ex.Instances != 3 {
		t.Fatalf("exact = %+v, want 3 instances", ex)
	}
	if ex.ByStatus[string(StatusComplete)] != 1 || ex.ByStatus[string(StatusQueued)] != 2 {
		t.Fatalf("by_status = %v", ex.ByStatus)
	}
}
