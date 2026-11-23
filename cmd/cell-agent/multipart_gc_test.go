package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestMultipartGCTickBudgetAndRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		type call struct {
			at     time.Duration
			cursor string
		}
		calls := make(chan call, 1)
		n := 0
		go runMultipartGC(ctx, nil, func(c context.Context, ttl time.Duration, cursor string, limit int) (int, string, error) {
			if ttl != 24*time.Hour || limit != 128 {
				t.Errorf("unbounded/wrong GC batch: ttl=%s limit=%d", ttl, limit)
			}
			deadline, ok := c.Deadline()
			if !ok || time.Until(deadline) > 30*time.Second {
				t.Error("missing GC time budget")
			}
			calls <- call{time.Since(start), cursor}
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
			if n == 5 {
				return 0, "", errors.New("first page unavailable")
			}
			return 1, "", nil
		})
		for _, want := range []call{
			{time.Minute, ""}, {2 * time.Minute, "next"},
			{3 * time.Minute, "confirmed-progress"}, {4 * time.Minute, "confirmed-progress"},
			{64 * time.Minute, ""}, {65 * time.Minute, ""}, {125 * time.Minute, ""},
		} {
			select {
			case got := <-calls:
				if got != want {
					t.Fatalf("collection = %+v, want %+v", got, want)
				}
			case <-time.After(61 * time.Minute):
				t.Fatal("collection stalled")
			}
		}
	})
}
