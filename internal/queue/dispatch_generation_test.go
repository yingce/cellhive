package queue

import (
	"context"
	"testing"
)

type generationDispatcher struct{ calls int }

func (d *generationDispatcher) Dispatch(context.Context, Ref, []Message) error { d.calls++; return nil }
func TestClaimedPredecessorBatchCannotDispatchAfterRecreation(t *testing.T) {
	s := newStore(t)
	generation := "old"
	s.ResourceGeneration = func(context.Context, string, string) (string, error) { return generation, nil }
	if _, err := s.Send(t.Context(), "acme", "q", []byte("old"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	messages, err := s.Claim(t.Context(), "acme", "q", 1, 1000)
	if err != nil || len(messages) != 1 {
		t.Fatal(err)
	}
	generation = "new"
	d := &generationDispatcher{}
	r := &Runner{Store: s, Dispatch: d}
	if n := r.dispatchBatch(t.Context(), Ref{Namespace: "acme", Name: "q"}, messages); n != 0 || d.calls != 0 {
		t.Fatalf("old payload dispatched after delete/recreate: n=%d calls=%d", n, d.calls)
	}
}
