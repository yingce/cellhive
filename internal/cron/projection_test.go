package cron

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
)

func TestProjectionRetryAndNudgePreserveWrittenSlot(t *testing.T) {
	ctx := t.Context()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c, err := cs.Open(ctx, cell.Scope{Namespace: "demo", Class: CronClass, ID: "w"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	now := utc(2026, time.January, 5, 10, 0)
	version := "v1"
	checks, loads, writes, publications := 0, 0, 0, 0
	fail := true
	var projected time.Time
	s := Scheduler{
		Now:     func() time.Time { return now },
		Version: func(context.Context) (string, error) { checks++; return version, nil },
		Targets: func(context.Context) ([]Target, error) { loads++; return []Target{{"demo", "w", "0 * * * *"}}, nil },
		Open:    func(context.Context, cell.Scope) (*timer.Store, error) { writes++; return st, nil },
		ProjectNext: func(_ context.Context, next time.Time) error {
			publications++
			projected = next
			if fail {
				return context.DeadlineExceeded
			}
			return nil
		},
	}
	if n, err := s.Pass(ctx); n != 1 || err == nil {
		t.Fatalf("first pass: %d %v", n, err)
	}
	fail = false
	if n, err := s.Pass(ctx); n != 0 || err != nil {
		t.Fatalf("retry: %d %v", n, err)
	}
	if !projected.Equal(now.Add(time.Hour)) || publications != 2 || writes != 1 || loads != 1 {
		t.Fatalf("deadline=%v publications=%d writes=%d loads=%d", projected, publications, writes, loads)
	}
	version = "v2" // Recompiling a due schedule must not rewrite its fired slot.
	s.Nudge()
	if n, err := s.Pass(ctx); n != 0 || err != nil {
		t.Fatalf("nudged pass: %d %v", n, err)
	}
	if checks != 3 || loads != 2 || writes != 1 || publications != 3 {
		t.Fatalf("checks=%d loads=%d writes=%d publications=%d", checks, loads, writes, publications)
	}
	now = now.Add(time.Minute)
	if _, err := s.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if publications != 4 || loads != 2 || !projected.Equal(now.Truncate(time.Hour).Add(time.Hour)) {
		t.Fatalf("cached plan did not republish: %v calls=%d loads=%d", projected, publications, loads)
	}
}

func TestStartNudgeChecksVersionBeforeMinute(t *testing.T) {
	checked := make(chan struct{}, 4)
	s := Scheduler{Interval: time.Hour, Now: func() time.Time { return utc(2026, time.January, 5, 10, 0) }, Version: func(context.Context) (string, error) { checked <- struct{}{}; return "v1", nil }, Targets: func(context.Context) ([]Target, error) { return nil, nil }, Open: func(context.Context, cell.Scope) (*timer.Store, error) { return nil, nil }}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.Start(ctx)
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("initial pass missing")
	}
	s.Nudge()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("nudge did not force version check")
	}
}
