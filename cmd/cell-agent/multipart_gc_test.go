package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMultipartGCTickBudgetAndRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time)
	calls := make(chan string, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		n := 0
		runMultipartGC(ctx, nil, ticks, func(c context.Context, ttl time.Duration, cursor string, limit int) (int, string, error) {
			if ttl != 24*time.Hour || limit != 128 {
				t.Errorf("unbounded/wrong GC batch: ttl=%s limit=%d", ttl, limit)
			}
			deadline, ok := c.Deadline()
			if !ok || time.Until(deadline) > 30*time.Second {
				t.Error("missing GC time budget")
			}
			calls <- cursor
			n++
			if n == 1 {
				return 1, "next", nil
			}
			if n == 2 {
				return 0, "confirmed-progress", errors.New("temporary failure after progress")
			}
			if n == 3 {
				return 0, cursor, errors.New("temporary failure without progress")
			}
			return 1, "", nil
		})
	}()
	for _, want := range []string{"", "next", "confirmed-progress", "confirmed-progress", ""} {
		select {
		case ticks <- time.Now():
		case <-done:
			t.Fatal("GC loop exited before cancellation")
		case <-time.After(5 * time.Second):
			t.Fatal("GC did not accept tick")
		}
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("cursor=%q want=%q", got, want)
			}
		case <-done:
			t.Fatal("GC loop exited before collection")
		case <-time.After(5 * time.Second):
			t.Fatal("GC collection stalled")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not stop on cancellation")
	}
}
