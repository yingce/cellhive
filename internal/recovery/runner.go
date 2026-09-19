package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/lease"
	"cellhive/internal/nodelog"
)

// Runner orchestrates automatic recovery of dead nodes (docs/cell-protocol.md §6,
// ADR-057/ADR-066). It is invoked by the fleet leader (the waker) on a cold
// interval: for every node that has node-log records and is not alive (lease
// absent or expired), it runs RecoverNode so already-acknowledged-but-not-yet-
// uploaded segments are collected and sealed. It is idempotent and fail-safe:
// it never recovers a node whose lease is live, and on an uncertain lease read
// it skips rather than risk fencing a healthy node.
type Runner struct {
	Nodelog  *nodelog.Manager
	Lease    *lease.Manager
	Recovery *Manager
	SelfNode string
	Log      *slog.Logger
	Now      func() time.Time
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

// Pass runs one orchestration pass. It returns the number of dead nodes
// recovered, the segments collected and the sessions sealed.
func (r *Runner) Pass(ctx context.Context) (nodes, segments, sessions int, err error) {
	if r.Nodelog == nil || r.Recovery == nil {
		return 0, 0, 0, fmt.Errorf("recovery: nodelog and recovery manager are required")
	}
	if r.Lease == nil {
		return 0, 0, 0, fmt.Errorf("recovery: lease manager is required")
	}
	nodeList, err := r.Nodelog.Nodes(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	now := r.now()
	for _, node := range nodeList {
		if node == r.SelfNode {
			continue
		}
		l, lerr := r.Lease.Get(ctx, node)
		switch {
		case lerr == nil && l.Live(now):
			continue // alive: never recover a live node
		case lerr != nil && !errors.Is(lerr, bucket.ErrNotFound):
			// Uncertain liveness: fail safe by skipping (miss a recovery rather
			// than fence a healthy node).
			r.log().Warn("recovery: lease read failed; skipping node", "node", node, "err", lerr)
			continue
		}
		seg, sess, rerr := r.Recovery.RecoverNode(ctx, r.Nodelog, node)
		if rerr != nil {
			r.log().Warn("recovery: node recovery failed", "node", node, "err", rerr)
			continue
		}
		if sess > 0 {
			nodes++
			segments += seg
			sessions += sess
			r.log().Info("recovery: recovered dead node", "node", node, "segments", seg, "sessions", sess)
		}
	}
	return nodes, segments, sessions, nil
}
