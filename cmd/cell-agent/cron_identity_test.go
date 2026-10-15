package main

import (
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/cron"
	"cellhive/internal/timer"
)

func TestCronDispatchKeepsPersistedExpression(t *testing.T) {
	targets := []control.CronTarget{{Cron: "0 * * * *"}}
	tm := timer.New(1767607200000, timer.KindCron, "acme/__cron__/web", cron.Occurrence(time.UnixMilli(1767607200000), "*/5 * * * *"))
	if got := cronExprForSlot(targets, tm); got != "*/5 * * * *" {
		t.Fatalf("redeploy changed queued trigger: %q", got)
	}
	// Rows produced by the previous release remain deliverable.
	tm.Occurrence = "29460120"
	if got := cronExprForSlot(targets, tm); got != "0 * * * *" {
		t.Fatalf("legacy slot expression=%q", got)
	}
}
