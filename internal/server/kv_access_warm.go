package server

import (
	"context"
	"sync"
	"time"

	"cellhive/internal/cell"
)

// A bounded active set moves the control/fence refresh and lease-bounded owner lookup off
// requests that repeatedly access the same resident KV cell. Cache validity and
// all fail-closed checks still live in their original request paths.
const (
	hotKVAccessLimit       = 64
	hotKVAccessIdle        = 2 * time.Second
	hotKVAccessRefresh     = 500 * time.Millisecond
	hotKVAccessFirst       = 500 * time.Millisecond
	hotKVAccessRetry       = 300 * time.Millisecond
	hotKVAccessConcurrency = 8
)

type hotKVAccess struct {
	scope      cell.Scope
	ns, name   string
	lastAccess time.Time
	next       time.Time
	active     bool
	refreshing bool
}

func (s *Server) noteHotKVAccess(sc cell.Scope, ns, name string) {
	if s.Control == nil || s.Owner == nil || s.resourceReplica() == nil || s.Cfg.BindingCacheTTL <= 0 {
		return
	}
	now := time.Now()
	key := sc.String()
	s.hotKVAccessMu.Lock()
	defer s.hotKVAccessMu.Unlock()
	if e := s.hotKVAccesses[key]; e != nil {
		if now.Sub(e.lastAccess) > hotKVAccessIdle {
			e.active = false
			e.lastAccess = now
			return
		}
		e.lastAccess = now
		if !e.active {
			e.active = true // the second recent access makes this scope hot
			e.next = now.Add(hotKVAccessFirst)
		}
		return
	}
	if s.hotKVAccesses == nil {
		s.hotKVAccesses = make(map[string]*hotKVAccess)
	}
	if len(s.hotKVAccesses) >= hotKVAccessLimit {
		var oldest string
		var at time.Time
		for k, e := range s.hotKVAccesses {
			if oldest == "" || e.lastAccess.Before(at) {
				oldest, at = k, e.lastAccess
			}
		}
		delete(s.hotKVAccesses, oldest)
	}
	s.hotKVAccesses[key] = &hotKVAccess{scope: sc, ns: ns, name: name, lastAccess: now}
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
			if now.Sub(e.lastAccess) > hotKVAccessIdle {
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
							e.next = time.Now().Add(hotKVAccessRefresh)
						} else {
							e.next = time.Now().Add(hotKVAccessRetry)
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
	ok, ref, err := s.refreshBindingInfo(ctx, e.ns, "kv", e.name)
	if err != nil || !ok {
		return err
	}
	// A binding that changed scope is not allowed to prewarm the old cell.
	if ref != e.scope.ID && ref != e.scope.String() {
		return nil
	}
	// Refresh the resource fence independently of the lease-bounded route.
	// A cold route lookup must not delay the fence refresh.
	fence := make(chan error, 1)
	go func() { fence <- s.resourceReplica().RefreshResourceSnapshot(ctx, e.scope) }()
	_, _, ownerErr := s.Owner.ResolveRoute(ctx, e.scope)
	fenceErr := <-fence
	if ownerErr != nil {
		return ownerErr
	}
	return fenceErr
}
