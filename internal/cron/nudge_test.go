package cron

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

func TestNudgeDoesNotWaitForDiscovery(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	loads := 0
	s := &Scheduler{Targets: func(context.Context) ([]Target, error) {
		loads++
		if loads == 1 {
			close(entered)
			<-release
		}
		return nil, nil
	}, Open: func(context.Context, cell.Scope) (*timer.Store, error) { return nil, nil }}
	go func() { s.Pass(context.Background()); close(done) }()
	<-entered
	n := make(chan struct{})
	go func() { s.Nudge(); close(n) }()
	select {
	case <-n:
	case <-time.After(time.Second):
		t.Error("Nudge blocked behind discovery")
	}
	close(release)
	<-done
	<-n
	if _, err := s.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if loads != 2 {
		t.Fatalf("notification lost during discovery: loads=%d", loads)
	}
}
