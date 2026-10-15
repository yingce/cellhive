package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"cellhive/internal/control"
	"cellhive/internal/queue"
)

func TestDLQPreviewIsReadOnlyAndAckIsIdempotent(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "sha", Consumers: []control.Consumer{{Queue: "jobs", DeadLetterQueue: "dlq"}}}, "ops"); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, delay := range []int{0, 0, 3600} {
		id, err := s.Queue.Send(ctx, "acme", "dlq", []byte("body"), "text", delay, "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := s.Queue.Claim(ctx, "acme", "dlq", 1, 60000); err != nil {
		t.Fatal(err)
	}
	before, _, err := s.Queue.PeekPage(ctx, "acme", "dlq", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.Cell(ctx, queue.Scope("acme", "dlq"))
	if err != nil {
		t.Fatal(err)
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	cursor := ""
	seen := map[string]bool{}
	for {
		rr := mustAdmin(t, h, http.MethodGet, "/v1/queue/dead-letters?namespace=acme&queue=jobs&limit=1&cursor="+url.QueryEscape(cursor), "")
		var out struct {
			Messages []queue.Message `json:"messages"`
			Next     string          `json:"next_cursor"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			if seen[m.ID] {
				t.Fatal("duplicate page entry")
			}
			seen[m.ID] = true
		}
		cursor = out.Next
		if cursor == "" {
			break
		}
	}
	if len(seen) != 3 {
		t.Fatalf("pagination lost messages: %v", seen)
	}
	after, _, err := s.Queue.PeekPage(ctx, "acme", "dlq", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatalf("preview changed attempts/lease/visibility: %s -> %s", a, b)
	}
	current, err := c.TxID(ctx)
	if err != nil || current != txid {
		t.Fatalf("preview wrote SQLite: %d -> %d (%v)", txid, current, err)
	}
	body, _ := json.Marshal(map[string]any{"message_ids": []string{ids[0], "already-missing"}})
	for i := 0; i < 2; i++ {
		mustAdmin(t, h, http.MethodPost, "/v1/queue/dead-letters/ack?namespace=acme&queue=jobs", string(body))
	}
	remaining, _, err := s.Queue.PeekPage(ctx, "acme", "dlq", "", 100)
	if err != nil || len(remaining) != 2 {
		t.Fatalf("selected ack: %+v %v", remaining, err)
	}
	for _, m := range remaining {
		if m.ID == ids[0] {
			t.Fatal("acked message remains")
		}
	}
}
