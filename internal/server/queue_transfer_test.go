package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/owner"
	queuepkg "cellhive/internal/queue"
)

func TestRemoteQueueTransferPreservesMetadata(t *testing.T) {
	destination, _ := newTestServer(t)
	destination.Queue = queuepkg.New(destination.Store)
	remote := httptest.NewServer(destination.Handler())
	defer remote.Close()
	destination.Owner.Advertise = remote.URL
	if _, err := destination.Owner.Claim(t.Context(), queuepkg.Scope("acme", "jobs"), time.Now()); err != nil {
		t.Fatal(err)
	}
	source, _ := newTestServer(t)
	source.Cfg.NodeID = "source"
	source.Owner = &owner.Manager{B: destination.Bucket, NodeID: "source", Session: "source", OwnerTTL: time.Minute}
	m := queuepkg.Message{ID: "source-message", Body: []byte("body"), ContentType: "text", IdempotencyKey: "business", Metadata: json.RawMessage(`{"trace":"t"}`)}
	if err := source.EnqueueToQueueOwner(t.Context(), "acme", "jobs", m); err != nil {
		t.Fatal(err)
	}
	msgs, err := destination.Queue.Claim(t.Context(), "acme", "jobs", 1, 1000)
	if err != nil || len(msgs) != 1 || string(msgs[0].Metadata) != `{"trace":"t"}` || msgs[0].IdempotencyKey != "business" {
		t.Fatalf("remote context lost: %+v %v", msgs, err)
	}
	if err := destination.Queue.Ack(t.Context(), "acme", "jobs", msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := source.EnqueueToQueueOwner(t.Context(), "acme", "jobs", m); err != nil {
		t.Fatal(err)
	}
	if depth, _ := destination.Queue.Depth(t.Context(), "acme", "jobs"); depth != 0 {
		t.Fatalf("remote receipt lost after ack: %d", depth)
	}
	rr := do(t, destination.Handler(), http.MethodPost, "/v1/internal/queue/transfer?ns=acme&queue=jobs", "", nil)
	if rr.Code != 401 {
		t.Fatalf("unprivileged transfer accepted: %d", rr.Code)
	}
}
