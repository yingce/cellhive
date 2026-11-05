package logbuf

import (
	"encoding/json"
	"strings"
	"sync"
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

func TestEntryByteBudgetTruncatesAndEvicts(t *testing.T) {
	const budget = 4 << 20
	b := NewWithBudget(10_000, 2, budget)
	message := strings.Repeat("x", 1<<20)
	for i := 0; i < 1000; i++ {
		if res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Message: message}); !res.Stored {
			t.Fatalf("entry %d not stored", i)
		}
	}
	stats := b.Stats()
	if stats.EntryBytes > budget {
		t.Fatalf("entry bytes = %d, want <= %d", stats.EntryBytes, budget)
	}
	if stats.Entries > 10_000 {
		t.Fatalf("entries = %d, want <= maxPerKey", stats.Entries)
	}
	if stats.Truncated != 1000 {
		t.Fatalf("truncated = %d, want 1000", stats.Truncated)
	}
	if stats.Evicted == 0 {
		t.Fatal("expected byte budget evictions")
	}
	got := b.Since("acme", "web", 0, 10_000)
	if len(got) == 0 || got[len(got)-1].Seq != 1000 {
		t.Fatalf("retained entries = %d, newest seq = %d", len(got), got[len(got)-1].Seq)
	}
	for _, e := range got {
		if len(e.Message) > MaxEntryMessageBytes {
			t.Fatalf("message bytes = %d, want <= %d", len(e.Message), MaxEntryMessageBytes)
		}
	}
}

func TestEntryOversizedDroppedWithCounterAndLaterLinesKept(t *testing.T) {
	b := NewWithBudget(10, 2, 1024)
	if res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Message: "ok"}); !res.Stored {
		t.Fatal("small entry not stored")
	}
	res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Message: strings.Repeat("x", 4096)})
	if res.Stored || res.Entry.Seq != 0 {
		t.Fatalf("oversized entry = %+v, want dropped", res)
	}
	if res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Message: "after"}); !res.Stored {
		t.Fatal("subsequent entry not stored")
	}
	stats := b.Stats()
	if stats.Dropped != 1 || stats.Added != 2 {
		t.Fatalf("stats = %+v, want dropped=1 added=2", stats)
	}
	got := b.Since("acme", "web", 0, 10)
	if len(got) != 2 || got[0].Message != "ok" || got[1].Message != "after" {
		t.Fatalf("retained = %+v", got)
	}
}

func TestEntryMessageTruncated(t *testing.T) {
	b := New(10, 2)
	res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Message: strings.Repeat("x", 1<<20)})
	if !res.Stored || !res.Truncated {
		t.Fatalf("result = %+v, want stored+truncated", res)
	}
	if len(res.Entry.Message) > MaxEntryMessageBytes {
		t.Fatalf("message bytes = %d, want <= %d", len(res.Entry.Message), MaxEntryMessageBytes)
	}
	if b.Stats().Truncated != 1 {
		t.Fatalf("truncated counter = %d", b.Stats().Truncated)
	}
}

func TestEntryMessageArgsCapped(t *testing.T) {
	b := New(10, 2)
	arg, err := json.Marshal(strings.Repeat("a", 4096))
	if err != nil {
		t.Fatal(err)
	}
	args := make([]json.RawMessage, 100)
	for i := range args {
		args[i] = arg
	}
	res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", MessageArgs: args})
	if !res.Stored || !res.Truncated {
		t.Fatalf("result = %+v, want stored+truncated", res)
	}
	total := 0
	for _, raw := range res.Entry.MessageArgs {
		if !json.Valid(raw) {
			t.Fatalf("invalid raw arg %q", raw)
		}
		total += len(raw)
	}
	if total > MaxEntryMessageArgsBytes {
		t.Fatalf("message args bytes = %d, want <= %d", total, MaxEntryMessageArgsBytes)
	}
	if len(res.Entry.MessageArgs) >= len(args) {
		t.Fatalf("message args not truncated: %d", len(res.Entry.MessageArgs))
	}
}

func TestEntryExceptionsCapped(t *testing.T) {
	b := New(10, 2)
	exceptions := make([]TailException, 100)
	for i := range exceptions {
		exceptions[i] = TailException{
			Name:    "Error",
			Message: strings.Repeat("m", 100_000),
			Stack:   strings.Repeat("s", 100_000),
		}
	}
	res := b.AddEntry(Entry{Namespace: "acme", Worker: "web", Exceptions: exceptions})
	if !res.Stored || !res.Truncated {
		t.Fatalf("result = %+v, want stored+truncated", res)
	}
	if len(res.Entry.Exceptions) > MaxEntryExceptions {
		t.Fatalf("exceptions = %d, want <= %d", len(res.Entry.Exceptions), MaxEntryExceptions)
	}
	for _, exception := range res.Entry.Exceptions {
		if len(exception.Message) > MaxExceptionMessageBytes {
			t.Fatalf("exception message bytes = %d", len(exception.Message))
		}
		if len(exception.Stack) > MaxExceptionStackBytes {
			t.Fatalf("exception stack bytes = %d", len(exception.Stack))
		}
	}
}

func TestAddEventOversizedDroppedSignalsCaller(t *testing.T) {
	b := NewWithBudget(10, 2, 1024)
	raw := json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`)
	got, stored := b.AddEventResult(TailEvent{Namespace: "acme", Worker: "web", Outcome: "ok", Event: raw})
	if stored || got.Seq != 0 {
		t.Fatalf("oversized event = %+v stored=%v, want dropped", got, stored)
	}
	if b.Stats().Dropped != 1 {
		t.Fatalf("dropped counter = %d", b.Stats().Dropped)
	}
	small, stored := b.AddEventResult(TailEvent{Namespace: "acme", Worker: "web", Outcome: "ok"})
	if !stored || small.Seq == 0 {
		t.Fatalf("small event = %+v stored=%v", small, stored)
	}
	if len(b.EventsSince("acme", "web", 0, 10)) != 1 {
		t.Fatal("small event not retained")
	}
}

func TestBufferConcurrentIngest(t *testing.T) {
	b := NewWithBudget(100, 8, 256<<10)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := "w" + string(rune('0'+w%3))
			for i := 0; i < 200; i++ {
				b.AddEntry(Entry{Namespace: "acme", Worker: worker, Message: strings.Repeat("x", 1024)})
				b.AddEvent(TailEvent{Namespace: "acme", Worker: worker, Outcome: "ok", Logs: []TailLog{{Message: []json.RawMessage{json.RawMessage(`"x"`)}}}})
				b.Since("acme", worker, 0, 10)
				b.EventsSince("acme", worker, 0, 10)
				b.Stats()
			}
		}(w)
	}
	wg.Wait()
	if b.Stats().Added == 0 {
		t.Fatal("no accepted entries")
	}
}
