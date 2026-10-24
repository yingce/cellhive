package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/lease"
	"cellhive/internal/nodelog"
)

// Runner recovers historical sessions, including predecessors of live nodes.
// Its independent loop shares the waker election without blocking timer work.
// Current live sessions and uncertain lease reads are skipped (ADR-285).
type retryState struct {
	after time.Time
	delay time.Duration
}

type Runner struct {
	mu             sync.Mutex
	retries        map[string]retryState
	AttemptTimeout time.Duration
	// IsLeader is optional for embedded/test use. Production shares the waker election.
	IsLeader func(context.Context) (bool, error)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retries == nil {
		r.retries = map[string]retryState{}
	}
	if r.IsLeader != nil {
		ok, err := r.IsLeader(ctx)
		if err != nil || !ok {
			return 0, 0, 0, err
		}
	}
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
		l, lerr := r.Lease.Get(ctx, node)
		if lerr != nil && !errors.Is(lerr, bucket.ErrNotFound) {
			r.log().Warn("recovery: lease read failed; skipping node", "node", node, "err", lerr)
			continue
		}
		recs, rerr := r.Nodelog.ListNode(ctx, node)
		if rerr != nil {
			r.log().Warn("recovery: list sessions failed", "node", node, "err", rerr)
			continue
		}
		recovered := false
		for _, rec := range recs {
			if rec.Status == nodelog.StatusSealed || (lerr == nil && l.Live(now) && l.Session == rec.Session) {
				continue
			}
			key := node + "/" + rec.Session
			retry := r.retries[key]
			if retry.after.After(now) {
				continue
			}
			timeout := r.AttemptTimeout
			if timeout <= 0 {
				timeout = 2 * time.Minute
			}
			attempt, cancel := context.WithTimeout(ctx, timeout)
			n, rerr := r.Recovery.Recover(attempt, r.Nodelog, node, rec.Session)
			cancel()
			if rerr != nil {
				if retry.delay == 0 {
					retry.delay = time.Second
				} else {
					retry.delay *= 2
				}
				if retry.delay > time.Minute {
					retry.delay = time.Minute
				}
				retry.after = r.now().Add(retry.delay)
				r.retries[key] = retry
				r.log().Warn("recovery: session recovery failed", "node", node, "session", rec.Session, "retry_in", retry.delay, "recovered_seq", rec.Progress, "err", rerr)
				continue
			}
			delete(r.retries, key)
			recovered = true
			segments += n
			sessions++
			r.log().Info("recovery: session sealed", "node", node, "session", rec.Session, "segments", n)
		}
		if recovered {
			nodes++
		}
	}
	return nodes, segments, sessions, nil
}

// Run is independent of timer dispatch. One unavailable follower must not hold
// the fleet waker's pass or cron materialization behind a recovery timeout.
func (r *Runner) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, _, _, err := r.Pass(ctx); err != nil {
				r.log().Warn("recovery pass failed", "err", err)
			}
		}
	}
}
