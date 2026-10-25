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
