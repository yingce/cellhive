package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

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

type discoveredNode struct {
	records   []nodelog.Record
	refreshed time.Time
	session   string
	live      bool
}

type Runner struct {
	// DiscoveryInterval bounds candidate staleness, never authority freshness.
	DiscoveryInterval time.Duration
	discovered        map[string]discoveredNode
	nodeList          []string
	discoveredAt      time.Time

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
			r.discoveredAt = time.Time{}
			r.discovered = nil
			return 0, 0, 0, err
		}
	}
	if r.Nodelog == nil || r.Recovery == nil {
		return 0, 0, 0, fmt.Errorf("recovery: nodelog and recovery manager are required")
	}
	if r.Lease == nil {
		return 0, 0, 0, fmt.Errorf("recovery: lease manager is required")
	}
	now := r.now()
	interval := r.DiscoveryInterval
	if interval <= 0 {
		interval = time.Minute
	}
	if r.discovered == nil {
		r.discovered = make(map[string]discoveredNode)
	}
	var listing *nodelog.Listing
	if r.discoveredAt.IsZero() || now.Sub(r.discoveredAt) >= interval {
		var discoverErr error
		listing, discoverErr = r.Nodelog.DiscoverPending(ctx)
		if discoverErr != nil {
			return 0, 0, 0, discoverErr
		}
		nodeList := listing.Nodes()
		r.nodeList = nodeList
		r.discoveredAt = now
		// Bound memory to the last successful discovery snapshot.
		next := make(map[string]discoveredNode, len(nodeList))
		for _, node := range nodeList {
			next[node] = r.discovered[node]
		}
		r.discovered = next
	}
	for _, node := range r.nodeList {
		l, found, lerr := r.Lease.NodeLeaseForDiscovery(ctx, node)
		if lerr != nil {
			r.log().Warn("recovery: lease read failed; skipping node", "node", node, "err", lerr)
			continue
		}
		state := r.discovered[node]
		live := found && l.Live(now)
		leaseChanged := !state.refreshed.IsZero() && (state.session != l.Session || state.live != live)
		if state.refreshed.IsZero() || now.Sub(state.refreshed) >= interval || leaseChanged {
			started := r.now()
			read := r.Nodelog.ListPendingNode
			// A lease transition can postdate the global listing. Rediscover that node
			// after the lease read so newly published sessions are visible.
			if listing != nil && !leaseChanged {
				read = listing.Records
			}
			recs, readErr := read(ctx, node)
			if readErr != nil {
				r.log().Warn("recovery: list sessions failed", "node", node, "err", readErr)
				continue
			}
			state = discoveredNode{records: recs, refreshed: started, session: l.Session, live: live}
			r.discovered[node] = state
		}
		recs := state.records
		recovered := false
		for i, rec := range recs {
			if rec.Status == nodelog.StatusSealed || (live && l.Session == rec.Session) {
				continue
			}
			key := node + "/" + rec.Session
			retry := r.retries[key]
			if retry.after.After(now) {
				continue
			}
			// Discovery may have reused a live lease for a different session.
			// Never let that stale identity authorize sealing a newly published
			// session. Recheck authority after loading the candidate record.
			current, exists, leaseErr := r.Lease.NodeLeaseFresh(ctx, node)
			if leaseErr != nil {
				r.log().Warn("recovery: candidate lease read failed", "node", node, "err", leaseErr)
				continue
			}
			if exists && current.Live(r.now()) && current.Session == rec.Session {
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
			recs[i].Status = nodelog.StatusSealed
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
