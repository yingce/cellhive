package workerdpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeLauncher struct {
	started []Slot
	handles map[string]*Handle
}

func (f *fakeLauncher) Start(_ context.Context, s Slot) (*Handle, error) {
	if f.handles == nil {
		f.handles = map[string]*Handle{}
	}
	done := make(chan error, 1)
	h := &Handle{PID: int(s.Generation), Done: done, Kill: func() error { done <- errors.New("killed"); return nil }}
	f.started = append(f.started, s)
	f.handles[s.ID] = h
	return h, nil
}

func TestSupervisorRemovesOnlyMatchingGeneration(t *testing.T) {
	f := &fakeLauncher{}
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 2}}, Launcher: f, Events: make(chan ProcessEvent, 2)}
	first, err := s.Start(context.Background(), FunctionKey{Namespace: "n", Worker: "w", Version: 1}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Kill(first.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Events:
	case <-time.After(time.Second):
		t.Fatal("missing process event")
	}
	second, err := s.Start(context.Background(), first.Key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation == first.Generation {
		t.Fatal("generation did not advance")
	}
	if err := s.Kill(second.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-s.Events:
		if ev.Slot.Generation != second.Generation {
			t.Fatalf("event generation=%d", ev.Slot.Generation)
		}
	case <-time.After(time.Second):
		t.Fatal("missing rebuilt event")
	}
}
