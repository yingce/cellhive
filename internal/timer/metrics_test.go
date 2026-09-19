package timer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestFireStatsAggregates(t *testing.T) {
	before := FireStats()
	observeFire("cron", "ok")
	observeFire("cron", "ok")
	observeFire("workflow_sleep", "failed")
	after := FireStats()
	if d := after["cron|ok"] - before["cron|ok"]; d != 2 {
		t.Fatalf("cron|ok delta = %d, want 2", d)
	}
	if d := after["workflow_sleep|failed"] - before["workflow_sleep|failed"]; d != 1 {
		t.Fatalf("workflow_sleep|failed delta = %d, want 1", d)
	}
}

type failOnceDispatcher struct{}

func (failOnceDispatcher) Dispatch(_ context.Context, tm Timer) error {
	if tm.Occurrence == "bad" {
		return errors.New("boom")
	}
	return nil
}

func TestDispatchDueObservesFireOutcomes(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	due := time.Now().Add(-time.Second).UnixMilli()
	for _, occ := range []string{"ok", "bad"} {
		if err := st.Upsert(ctx, New(due, KindCron, "app/__kv__/m", occ)); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	before := FireStats()
	if _, err := DispatchDue(ctx, st, failOnceDispatcher{}, time.Now().UnixMilli(), 10, time.Hour,
		slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	after := FireStats()
	if d := after["cron|ok"] - before["cron|ok"]; d != 1 {
		t.Fatalf("cron|ok delta = %d, want 1", d)
	}
	if d := after["cron|failed"] - before["cron|failed"]; d != 1 {
		t.Fatalf("cron|failed delta = %d, want 1", d)
	}
}
