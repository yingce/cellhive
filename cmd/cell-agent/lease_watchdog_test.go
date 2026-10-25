package main

import (
	"cellhive/internal/bucket"
	"cellhive/internal/lease"
	"context"
	"testing"
	"time"
)

func TestLeaseWatchdogHaltsExpiredSession(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(b, "a", "s", "", "", 100*time.Millisecond)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	halted := make(chan struct{})
	go watchNodeLease(ctx, lm, func() { close(halted) })
	select {
	case <-halted:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not halt")
	}
	if !lm.Fenced() || lm.Healthy(time.Now()) {
		t.Fatal("halted session remained authoritative")
	}
}
