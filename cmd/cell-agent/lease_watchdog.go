package main

import (
	"cellhive/internal/lease"
	"context"
	"time"
)

// This must run independently of renewal and skip graceful handoff: a fenced
// process no longer has authority to snapshot or release its former cells.
func watchNodeLease(ctx context.Context, lm *lease.Manager, halt func()) {
	interval := min(lm.TTL/10, 100*time.Millisecond)
	if interval <= 0 {
		interval = time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if lm.Fenced() {
			halt()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// The caller owns a context separate from the HTTP signal context and cancels
// it only after graceful handoff. True fencing is still handled by the watchdog.
func runNodeLeaseRenewal(ctx context.Context, ttl time.Duration, publish func() error) {
	interval := ttl / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = publish()
		}
	}
}
