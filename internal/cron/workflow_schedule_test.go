package cron

import (
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
	"context"
	"strconv"
	"testing"
	"time"
)

func TestWorkflowCoincidentSchedules(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__workflow__", ID: "wf"}
	c, err := cs.Open(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	now := utc(2026, time.January, 5, 10, 0)
	s := Scheduler{Now: func() time.Time { return now }, Targets: func(context.Context) ([]Target, error) { return nil, nil }, WorkflowTargets: func(context.Context) ([]WorkflowTarget, error) {
		return []WorkflowTarget{{"demo", "wf", "* * * * *"}, {"demo", "wf", "0 * * * *"}}, nil
	}, Open: func(context.Context, cell.Scope) (*timer.Store, error) { return st, nil }}
	n, err := s.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	due, err := st.Due(ctx, now.UnixMilli(), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("matched=%d pending=%d", n, len(due))
	if len(due) != 2 {
		t.Fatalf("coincident schedules collapsed: got %d timers", len(due))
	}
}

func TestWorkflowScheduleLegacyAndRestart(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__workflow__", ID: "wf"}
	c, err := cs.Open(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	now := utc(2026, time.January, 5, 10, 0)
	legacy := timer.New(now.UnixMilli(), timer.KindWorkflowSchedule, sc.String(), strconv.FormatInt(now.Unix()/60, 10))
	if err := st.Upsert(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkFired(ctx, legacy.Token, now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	targets := []WorkflowTarget{{"demo", "wf", "0 * * * *"}, {"demo", "wf", "* * * * *"}}
	for pass := 0; pass < 3; pass++ {
		targets[0], targets[1] = targets[1], targets[0]
		s := Scheduler{Now: func() time.Time { return now }, Targets: func(context.Context) ([]Target, error) { return nil, nil }, WorkflowTargets: func(context.Context) ([]WorkflowTarget, error) { return targets, nil }, Open: func(context.Context, cell.Scope) (*timer.Store, error) { return st, nil }}
		if _, err := s.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		due, err := st.Due(ctx, now.UnixMilli(), 10)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if pass == 0 {
			want = 1
		}
		if len(due) != want {
			t.Fatalf("pass %d: pending=%d want=%d", pass, len(due), want)
		}
		for _, tm := range due {
			if err := st.MarkFired(ctx, tm.Token, now.Add(time.Hour).UnixMilli()); err != nil {
				t.Fatal(err)
			}
		}
	}
}
