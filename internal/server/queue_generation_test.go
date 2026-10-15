package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"cellhive/internal/control"
	"cellhive/internal/queue"
)

func TestDelayedTransferRejectsDeletedSourceIncarnationAtReceiver(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	for _, name := range []string{"Q", "D"} {
		if _, err := s.Control.CreateResource(ctx, "acme", "queue", name, queue.Scope("acme", name).String(), "ops"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Queue.Send(ctx, "acme", "Q", []byte("old"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	old, err := s.Queue.Claim(ctx, "acme", "Q", 1, 1000)
	if err != nil || len(old) != 1 {
		t.Fatal(err)
	}
	// The sender registered the destination, but its HTTP body is delayed until
	// after deletion/recreation. The receiver must not trust sender preflight.
	if err := s.recordQueueDestination(ctx, "acme", "D", old[0]); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(old[0])
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=Q", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	rejected := do(t, s.Handler(), http.MethodPost, "/v1/internal/queue/transfer?ns=acme&queue=D", "tok", raw)
	if rejected.Code < 400 {
		t.Fatalf("late transfer accepted after delete: %d %s", rejected.Code, rejected.Body.String())
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "Q", queue.Scope("acme", "Q").String(), "ops"); err != nil {
		t.Fatal(err)
	}
	rejected = do(t, s.Handler(), http.MethodPost, "/v1/internal/queue/transfer?ns=acme&queue=D", "tok", raw)
	if rejected.Code < 400 {
		t.Fatalf("predecessor accepted after recreation: %d %s", rejected.Code, rejected.Body.String())
	}
	if _, err := s.Queue.Send(ctx, "acme", "Q", []byte("new"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	current, err := s.Queue.Claim(ctx, "acme", "Q", 1, 1000)
	if err != nil || len(current) != 1 {
		t.Fatal(err)
	}
	if current[0].SourceGeneration == old[0].SourceGeneration {
		t.Fatal("new producer did not capture incarnation")
	}
	raw, _ = json.Marshal(current[0])
	accepted := do(t, s.Handler(), http.MethodPost, "/v1/internal/queue/transfer?ns=acme&queue=D", "tok", raw)
	if accepted.Code != 200 {
		t.Fatalf("new incarnation blocked: %d %s", accepted.Code, accepted.Body.String())
	}
	messages, _, err := s.Queue.PeekPage(ctx, "acme", "D", "", 100)
	if err != nil || len(messages) != 1 || string(messages[0].Body) != "new" {
		t.Fatalf("old transfer resurrected: %+v %v", messages, err)
	}
}

func TestQueueDeleteDiscoversHistoricalAndTransferOnlyDestinations(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Q", "D1", "D2", "D3"} {
		if _, err := s.Control.CreateResource(ctx, "acme", "queue", name, queue.Scope("acme", name).String(), "ops"); err != nil {
			t.Fatal(err)
		}
	}
	for _, destination := range []string{"D1", "D2", "D3"} {
		if destination != "D3" {
			if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: destination, Consumers: []control.Consumer{{Queue: "Q", DeadLetterQueue: destination}}}, "ops"); err != nil {
				t.Fatal(err)
			}
		}
		id, err := s.Queue.Send(ctx, "acme", "Q", []byte(destination), "text", 0, "")
		if err != nil {
			t.Fatal(err)
		}
		messages, _, err := s.Queue.PeekPage(ctx, "acme", "Q", "", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range messages {
			if m.ID == id {
				if err := s.EnqueueToQueueOwner(ctx, "acme", destination, m); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// The current version no longer declares any consumer. Historical versions
	// and the durable transfer-only edge must still identify every destination.
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "none"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(ctx, "acme", "D1", []byte("independent"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=Q&force=1", "")
	job, err := s.Control.ResourceCleanup(ctx, "acme", "queue", "Q")
	if err != nil || len(job.DeadLetters) != 3 {
		t.Fatalf("destinations lost: %+v %v", job, err)
	}
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	job, _ = s.Control.ResourceCleanup(ctx, "acme", "queue", "Q")
	if job.State != "complete" {
		t.Fatalf("cleanup failed: %+v", job)
	}
	for _, name := range []string{"D1", "D2", "D3"} {
		messages, _, err := s.Queue.PeekPage(ctx, "acme", name, "", 100)
		want := 0
		if name == "D1" {
			want = 1
		}
		if err != nil || len(messages) != want {
			t.Fatalf("%s retains source payload: %+v %v", name, messages, err)
		}
		if _, ok, err := s.Control.Binding(ctx, "acme", "queue", name); err != nil || !ok {
			t.Fatalf("independent resource %s removed", name)
		}
	}
}
