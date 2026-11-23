package control

import (
	"errors"
	"testing"
)

func TestPendingRetirementSweeps(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"clean", "failed", "paged"} {
		if _, err := c.DB.ExecContext(ctx, `INSERT INTO resource_retirements(ns,data_scope,job_id,epoch_floor,updated_ms) VALUES('ns',?,'job',1,0)`, scope); err != nil {
			t.Fatal(err)
		}
	}
	update := func(scope, cursor string, cause error) {
		t.Helper()
		r, err := s.ResourceRetirement(ctx, scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateResourceRetirement(ctx, r, cursor, cause); err != nil {
			t.Fatal(err)
		}
	}
	update("failed", "", errors.New("bucket unavailable"))
	update("paged", "next-key", nil)
	for _, page := range []struct{ after, want string }{{"", "failed"}, {"failed", "paged"}} {
		rows, err := s.PendingRetirementSweeps(ctx, page.after, 1)
		if err != nil || len(rows) != 1 || rows[0].DataScope != page.want {
			t.Fatalf("pending after %q = %+v, %v", page.after, rows, err)
		}
	}
	update("failed", "", nil)
	update("paged", "", nil)
	rows, err := s.PendingRetirementSweeps(ctx, "", 20)
	if err != nil || len(rows) != 0 {
		t.Fatalf("finished sweep remained pending: %+v %v", rows, err)
	}
	rows, err = s.ResourceRetirements(ctx, "", 20)
	if err != nil || len(rows) != 3 {
		t.Fatalf("lost durable retirement history: %+v %v", rows, err)
	}
}
