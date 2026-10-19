package logbuf

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTailEventKeepsPrivateTraceparentOnInput(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var event TailEvent
	if err := json.Unmarshal([]byte(`{"outcome":"ok","requestId":"req-1","traceparent":"`+traceparent+`"}`), &event); err != nil {
		t.Fatal(err)
	}
	if event.Traceparent != traceparent || event.RequestID != "req-1" {
		t.Fatalf("decoded event = %+v", event)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "traceparent") || !strings.Contains(string(raw), `"requestId":"req-1"`) {
		t.Fatalf("public event = %s", raw)
	}
}

func TestBufferBoundedAndSince(t *testing.T) {
	b := New(3, 2)
	for i := 0; i < 5; i++ {
		b.Add(Entry{Namespace: "acme", Worker: "web", Level: "log", Message: "m"})
	}
	got := b.Since("acme", "web", 0, 100)
	if len(got) != 3 {
		t.Fatalf("bounded entries = %d, want 3", len(got))
	}
	if got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("seqs = %d..%d, want 3..5", got[0].Seq, got[2].Seq)
	}
	if since := b.Since("acme", "web", 4, 100); len(since) != 1 || since[0].Seq != 5 {
		t.Fatalf("since(4) = %+v", since)
	}
	// LRU cap on distinct workers.
	b.Add(Entry{Namespace: "acme", Worker: "api", Message: "x"})
	b.Add(Entry{Namespace: "acme", Worker: "other", Message: "y"})
	if len(b.Since("acme", "web", 0, 100)) != 0 {
		t.Fatal("oldest worker not evicted")
	}
}

func TestTailEventsBoundedByBytes(t *testing.T) {
	b := New(100, 2)
	for i := 0; i < 10; i++ {
		b.AddEvent(TailEvent{Namespace: "acme", Worker: "web", Outcome: "exception", Exceptions: []TailException{{Message: strings.Repeat("x", 200_000)}}})
	}
	got := b.EventsSince("acme", "web", 0, 100)
	if len(got) == 0 || len(got) > 5 {
		t.Fatalf("byte bounded events = %d", len(got))
	}
	if got[len(got)-1].Seq != 10 {
		t.Fatalf("newest seq = %d, want 10", got[len(got)-1].Seq)
	}
}
