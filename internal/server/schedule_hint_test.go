package server

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/scheduleindex"
)

func TestSharedHintOnlyNudgesCurrentOwner(t *testing.T) {
	dir := t.TempDir()
	a, _, _ := newControlForwardServer(t, dir, "a")
	b, om, _ := newControlForwardServer(t, dir, "b")
	b.Cfg.ScheduleMode = "shared"
	// Build the receiver with its explicit shared configuration.
	hs := httptest.NewServer(b.buildHandler())
	defer hs.Close()
	om.Advertise = hs.URL
	sc := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "mail"}
	o, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h := scheduleindex.Hint{Kind: "queue", Scope: sc.String()}
	var calls atomic.Int64
	b.QueueLocalNudge = func(ns, name string) {
		if ns != "acme" || name != "mail" {
			t.Error("wrong queue")
		}
		calls.Add(1)
	}
	if err := a.DeliverScheduleHint(t.Context(), h); err != nil || calls.Load() != 1 {
		t.Fatal("peer nudge", calls.Load(), err)
	}
	if err := b.acceptScheduleHint(t.Context(), scheduleRequest{h, o.Node, o.Session, o.Epoch + 1}); err == nil || calls.Load() != 1 {
		t.Fatal("accepted wrong epoch")
	}
}
