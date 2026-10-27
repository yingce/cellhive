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

func TestLeaseRenewalSurvivesServiceShutdown(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(b, "a", "draining", "", "", 300*time.Millisecond)
	leaseCtx, stopLease := context.WithCancel(t.Context())
	defer stopLease()
	publish := func() error { return lm.Publish(leaseCtx, lease.Load{}) }
	if err := publish(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { runNodeLeaseRenewal(leaseCtx, lm.TTL, publish); close(done) }()
	watchCtx, stopWatch := context.WithCancel(t.Context())
	defer stopWatch()
	halted := make(chan struct{}, 1)
	go watchNodeLease(watchCtx, lm, func() { halted <- struct{}{} })
	serviceCtx, stopService := context.WithCancel(t.Context())
	stopService()
	<-serviceCtx.Done()
	// Simulated flush/handoff lasts longer than two full lease periods.
	time.Sleep(750 * time.Millisecond)
	if !lm.Healthy(time.Now()) {
		t.Fatal("lease expired during graceful drain")
	}
	select {
	case <-halted:
		t.Fatal("watchdog interrupted healthy drain")
	default:
	}
	stopWatch()
	stopLease()
	<-done
}
