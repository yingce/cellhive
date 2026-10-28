package server

import (
	"context"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/scopedtoken"
)

// The refresh set is retained only for pinned authorization entries. KV data
// requests must not create a time-based "hot" window or background COS reads:
// owner leases, control revisions, and the normal fail-closed request checks
// are the source of truth.
const (
	hotKVAccessLimit       = 64
	hotKVAccessConcurrency = 8
	pinnedAuthRetry        = 300 * time.Millisecond
)

type hotKVAccess struct {
	pinned     *scopedtoken.Claims
	scope      cell.Scope
	ns, name   string
	lastAccess time.Time
	next       time.Time
	active     bool
	refreshing bool
}

func (s *Server) runKVReadWarmLoop(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	sem := make(chan struct{}, hotKVAccessConcurrency)
	var workers sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			return
		case <-tick.C:
		}
		now := time.Now()
		s.hotKVAccessMu.Lock()
		for key, e := range s.hotKVAccesses {
			// Non-pinned entries were created by the removed KV warm path (or by
			// an old process) and must not be refreshed anymore.
			if e.pinned == nil {
				delete(s.hotKVAccesses, key)
				continue
			}
			idle := time.Minute
			if now.Sub(e.lastAccess) > idle {
				delete(s.hotKVAccesses, key)
				continue
			}
			if !e.active || e.refreshing || now.Before(e.next) {
				continue
			}
			select {
			case sem <- struct{}{}:
				e.refreshing = true
				workers.Add(1)
				go func(key string, e *hotKVAccess) {
					defer workers.Done()
					defer func() { <-sem }()
					work, cancel := context.WithTimeout(ctx, time.Second)
					err := s.refreshHotKVAccess(work, e)
					cancel()
					s.hotKVRefreshes.Add(1)
					if err != nil {
						s.hotKVRefreshFailures.Add(1)
					}
					s.hotKVAccessMu.Lock()
					if s.hotKVAccesses[key] == e {
						e.refreshing = false
						if err == nil {
							e.next = time.Now().Add(s.pinnedRefreshInterval())
						} else {
							e.next = time.Now().Add(pinnedAuthRetry)
						}
					}
					s.hotKVAccessMu.Unlock()
				}(key, e)
			default:
				// Over-capacity scopes continue through the ordinary read-through
				// cache; do not create an unbounded background request queue.
			}
		}
		s.hotKVAccessMu.Unlock()
	}
}

func (s *Server) refreshHotKVAccess(ctx context.Context, e *hotKVAccess) error {
	var ok bool
	var ref string
	var err error
	if e.pinned != nil {
		ok, err = s.pinnedBindingAllowed(ctx, *e.pinned, true)
		ref = e.pinned.Resource
	} else {
		ok, ref, err = s.refreshBindingInfo(ctx, e.ns, "kv", e.name)
	}
	if err != nil || !ok {
		return err
	}
	// A binding that changed scope is not allowed to prewarm the old cell.
	if ref != e.scope.ID && ref != e.scope.String() {
		return nil
	}
	if s.resourceReplica() == nil || s.Owner == nil {
		return nil
	}
	// Refresh the resource fence independently of the lease-bounded route.
	// A cold route lookup must not delay the fence refresh.
	fence := make(chan error, 1)
	go func() { fence <- s.resourceReplica().RefreshResourceSnapshot(ctx, e.scope) }()
	_, _, ownerErr := s.Owner.ResolveForwardRoute(ctx, e.scope)
	fenceErr := <-fence
	if ownerErr != nil {
		return ownerErr
	}
	return fenceErr
}
