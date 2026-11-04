package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/cron"
	"cellhive/internal/dispatch"
	"cellhive/internal/timer"
)

func TestCronEnrichedFailureReachesTimerLog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, `{"error":"handler_failed","message":"secret-business-result"}`)
	}))
	defer srv.Close()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c, err := cs.Open(t.Context(), cell.Scope{Namespace: "acme", Class: "__cron__", ID: "web"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	st, err := timer.NewStore(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	tm := timer.New(1000, timer.KindCron, c.Scope.String(), "slot")
	if err := st.Upsert(t.Context(), tm); err != nil {
		t.Fatal(err)
	}
	e := &cronEnricher{next: dispatch.NewHTTP(srv.URL, ""), at: time.Now(), targets: map[string][]control.CronTarget{"acme\x00web": {{Worker: "resolved-web", BundleSHA: "sha", Version: 1}}}}
	var buf bytes.Buffer
	if n, err := timer.DispatchDue(t.Context(), st, e, 1000, 1, time.Hour, slog.New(slog.NewJSONHandler(&buf, nil))); n != 0 || err != nil {
		t.Fatalf("dispatch: %d %v", n, err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ns"] != "acme" || got["worker"] != "resolved-web" || got["scope"] != tm.Scope || got["error_code"] != "handler_failed" || got["http_status"] != float64(500) {
		t.Fatalf("lost enriched identity: %s", buf.String())
	}
	if strings.Contains(buf.String(), "secret-business-result") {
		t.Fatalf("leaked response: %s", buf.String())
	}
}

type recordingCronDispatcher struct {
	called bool
	timer  timer.Timer
}

func (d *recordingCronDispatcher) Dispatch(_ context.Context, t timer.Timer) error {
	d.called, d.timer = true, t
	return nil
}

func TestCronDispatchRetainsQueuedSlotAfterScheduleRemoval(t *testing.T) {
	slot := time.UnixMilli(1792748280000)
	queued := timer.New(slot.UnixMilli(), timer.KindCron, "vwork-24/__cron__/kv-cache-counter", cron.Occurrence(slot, "* * * * *"))
	proj := control.Projection{Apps: []control.ProjectionApp{{
		Namespace: "vwork-24",
		Workers: []control.ProjectionWorker{{
			Worker:  "kv-cache-counter",
			Version: control.Version{Number: 16, BundleSHA: "active-bundle"},
		}},
	}}}
	next := &recordingCronDispatcher{}
	e := &cronEnricher{next: next, targets: cronDispatchTargets(proj), at: time.Now()}
	if err := e.Dispatch(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	if !next.called || next.timer.Worker != "kv-cache-counter" || next.timer.BundleSHA != "active-bundle" || next.timer.Version != 16 || next.timer.Cron != "* * * * *" {
		t.Fatalf("queued slot lost its dispatch identity: %+v", next.timer)
	}
}

func TestCronDispatchRemovedScheduleReachesHTTP(t *testing.T) {
	slot := time.UnixMilli(1792748280000)
	queued := timer.New(slot.UnixMilli(), timer.KindCron, "vwork-24/__cron__/kv-cache-counter", cron.Occurrence(slot, "* * * * *"))
	proj := control.Projection{Apps: []control.ProjectionApp{{
		Namespace: "vwork-24",
		Workers: []control.ProjectionWorker{{Worker: "kv-cache-counter", Version: control.Version{
			Number: 16, BundleSHA: "active-bundle",
		}}},
	}}}
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Namespace string `json:"namespace"`
			Worker    string `json:"worker"`
			BundleSHA string `json:"bundle_sha"`
			Cron      string `json:"cron"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Namespace != "vwork-24" || body.Worker != "kv-cache-counter" || body.BundleSHA != "active-bundle" || body.Cron != "* * * * *" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	e := &cronEnricher{next: dispatch.NewHTTP(srv.URL, ""), targets: cronDispatchTargets(proj), at: time.Now()}
	if err := e.Dispatch(context.Background(), queued); err != nil || !called {
		t.Fatalf("HTTP dispatch failed: err=%v called=%v", err, called)
	}
}

func TestCronDispatchMissingWorkerFailsBeforeHTTP(t *testing.T) {
	queued := timer.New(1, timer.KindCron, "ns/__cron__/deleted", "1")
	next := &recordingCronDispatcher{}
	e := &cronEnricher{next: next, targets: map[string][]control.CronTarget{}, at: time.Now()}
	err := e.Dispatch(context.Background(), queued)
	if err == nil || !strings.Contains(err.Error(), "active worker unavailable") || next.called {
		t.Fatalf("missing worker: err=%v dispatched=%v", err, next.called)
	}
}

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
