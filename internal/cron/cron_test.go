package cron

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
)

func utc(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func TestParseAndMatches(t *testing.T) {
	sunday := utc(2026, time.January, 4, 0, 0) // Sunday
	monday := utc(2026, time.January, 5, 0, 0) // Monday
	first := utc(2026, time.February, 1, 0, 0) // 1st, Sunday
	cases := []struct {
		expr string
		at   time.Time
		want bool
	}{
		{"*/5 * * * *", utc(2026, time.January, 5, 10, 0), true},
		{"*/5 * * * *", utc(2026, time.January, 5, 10, 5), true},
		{"*/5 * * * *", utc(2026, time.January, 5, 10, 1), false},
		{"0 * * * *", utc(2026, time.January, 5, 10, 0), true},
		{"0 * * * *", utc(2026, time.January, 5, 10, 30), false},
		{"0 9-17 * * *", utc(2026, time.January, 5, 9, 0), true},
		{"0 9-17 * * *", utc(2026, time.January, 5, 18, 0), false},
		{"0 */6 * * *", utc(2026, time.January, 5, 12, 0), true},
		{"0 0,12 * * *", utc(2026, time.January, 5, 12, 0), true},
		{"0 0 * * 1", monday, true},
		{"0 0 * * 1", sunday, false},
		{"0 0 * * 7", sunday, true}, // 7 == Sunday
		{"0 0 * * 0", sunday, true},
		{"0 0 1 * *", first, true},
		// dom and dow both restricted -> OR (vixie): 1st OR Monday.
		{"0 0 1 * 1", first, true},
		{"0 0 1 * 1", monday, true},
		{"0 0 1 * 1", utc(2026, time.January, 6, 0, 0), false}, // Tue, not 1st
	}
	for _, c := range cases {
		sch, err := Parse(c.expr)
		if err != nil {
			t.Fatalf("parse %q: %v", c.expr, err)
		}
		if got := sch.Matches(c.at); got != c.want {
			t.Fatalf("Matches(%q, %s) = %v, want %v", c.expr, c.at, got, c.want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, expr := range []string{
		"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *",
		"0 0 0 * *", "0 0 * 13 *", "*/0 * * * *", "a * * * *", "0 0 * * * extra",
	} {
		if _, err := Parse(expr); err == nil {
			t.Fatalf("Parse(%q) should fail", expr)
		}
	}
}

func TestSchedulerMaterializesAndDedups(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	sc := cell.Scope{Namespace: "acme", Class: CronClass, ID: "web"}
	c, err := cs.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open cell: %v", err)
	}
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatalf("timer store: %v", err)
	}

	slot := utc(2026, time.January, 5, 10, 0)
	now := slot
	var registered []string
	s := &Scheduler{
		Now:  func() time.Time { return now },
		Open: func(context.Context, cell.Scope) (*timer.Store, error) { return st, nil },
		Register: func(scope string) {
			registered = append(registered, scope)
		},
		Targets: func(context.Context) ([]Target, error) {
			return []Target{{Namespace: "acme", Worker: "web", Cron: "0 * * * *"}}, nil
		},
	}

	n, err := s.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("pass = %d, %v; want 1", n, err)
	}
	if registered == nil || registered[0] != "acme/__cron__/web" {
		t.Fatalf("registered = %v", registered)
	}
	due, err := st.Due(ctx, slot.UnixMilli(), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("due = %+v, %v", due, err)
	}
	if due[0].Kind != timer.KindCron || due[0].Scope != "acme/__cron__/web" {
		t.Fatalf("timer = %+v", due[0])
	}

	// Same minute again -> upsert is idempotent (token dedup), count stays 1.
	if n, _ := s.Pass(ctx); n != 1 {
		t.Fatalf("second pass = %d, want 1 upsert", n)
	}
	if cnt, _ := st.Count(ctx); cnt != 1 {
		t.Fatalf("count = %d, want 1 (dedup by token)", cnt)
	}

	// Non-matching minute -> nothing.
	now = utc(2026, time.January, 5, 10, 1)
	if n, _ := s.Pass(ctx); n != 0 {
		t.Fatalf("non-matching pass = %d, want 0", n)
	}
}

func TestSchedulerSkipsBadExpression(t *testing.T) {
	ctx := context.Background()
	s := &Scheduler{
		Now:  func() time.Time { return utc(2026, time.January, 5, 10, 0) },
		Open: func(context.Context, cell.Scope) (*timer.Store, error) { return nil, nil },
		Targets: func(context.Context) ([]Target, error) {
			return []Target{{Namespace: "a", Worker: "w", Cron: "nope"}}, nil
		},
	}
	if n, err := s.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("bad expr pass = %d, %v; want 0, nil", n, err)
	}
}

type countingDispatcher struct{ got []timer.Timer }

func (d *countingDispatcher) Dispatch(_ context.Context, t timer.Timer) error {
	d.got = append(d.got, t)
	return nil
}

// TestSchedulerFeedsTimerRunner verifies the handoff: a materialized slot is
// registered and then dispatched by the timer runner (ADR-076 -> ADR-070).
func TestSchedulerFeedsTimerRunner(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	sc := cell.Scope{Namespace: "acme", Class: CronClass, ID: "web"}
	c, err := cs.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	reg := timer.NewRegistry()
	slot := utc(2026, time.January, 5, 10, 0)
	s := &Scheduler{
		Now:      func() time.Time { return slot },
		Open:     func(context.Context, cell.Scope) (*timer.Store, error) { return st, nil },
		Register: func(scope string) { reg.Add(scope) },
		Targets: func(context.Context) ([]Target, error) {
			return []Target{{Namespace: "acme", Worker: "web", Cron: "0 * * * *"}}, nil
		},
	}
	if n, err := s.Pass(ctx); err != nil || n != 1 {
		t.Fatalf("scheduler pass = %d, %v", n, err)
	}

	d := &countingDispatcher{}
	runner := &timer.Runner{
		Registry:   reg,
		Open:       func(context.Context, string) (*timer.Store, error) { return st, nil },
		Dispatcher: d,
		Now:        func() time.Time { return slot },
	}
	n, err := runner.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("runner pass = %d, %v; want 1", n, err)
	}
	if len(d.got) != 1 || d.got[0].Kind != timer.KindCron || d.got[0].Scope != "acme/__cron__/web" {
		t.Fatalf("dispatched = %+v", d.got)
	}
}
