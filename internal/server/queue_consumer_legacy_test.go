package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/queue"
)

func TestQueueConsumerMixedFleet(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	a, om, _ := newControlForwardServer(t, root, "old-owner")
	b, _, _ := newControlForwardServer(t, root, "client")
	h := a.Handler()
	var requests atomic.Int32
	var changeRevision atomic.Bool
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var q controlReadQuery
		if err := json.NewDecoder(req.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		requests.Add(1)
		// Previous releases return the full projection for all selectors.
		if q.Op == "projection" {
			if q.Kind == "" && changeRevision.Load() {
				if _, err := a.Control.CreateApp(ctx, "concurrent", "test"); err != nil {
					t.Error(err)
				}
			}
			q = controlReadQuery{Op: "projection"}
		}
		data, _ := json.Marshal(q)
		req.Body = io.NopCloser(bytes.NewReader(data))
		h.ServeHTTP(w, req)
	}))
	defer legacy.Close()
	om.Advertise = legacy.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateApp(ctx, "ns", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateResource(ctx, "ns", "queue", "jobs", queue.Scope("ns", "id").String(), "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.Deploy(ctx, "ns", "worker", control.DeploySpec{BundleSHA: "sha", Consumers: []control.Consumer{{Queue: "jobs", MaxBatchSize: 7}}}, "test"); err != nil {
		t.Fatal(err)
	}
	reader := b.BackgroundControl()
	ref, ok, err := reader.QueueConsumer(ctx, "ns", "id")
	if err != nil || !ok || ref.Name != "id" || ref.BindingName != "jobs" || ref.MaxBatchSize != 7 || requests.Load() != 3 {
		t.Fatalf("ref=%+v found=%v calls=%d err=%v", ref, ok, requests.Load(), err)
	}
	changeRevision.Store(true)
	if _, ok, err := reader.QueueConsumer(ctx, "ns", "id"); err == nil || ok {
		t.Fatalf("accepted split revision: ok=%t err=%v", ok, err)
	}
}
