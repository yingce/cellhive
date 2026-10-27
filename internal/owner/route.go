package owner

import (
	"context"
	"errors"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

// Bound retained hints even when callers visit many one-off cells. Eviction
// only causes a fresh point read; it never changes ownership.
const maxOwnerRoutes = 4096

// ownerRoute is a routing hint, never a replacement for the receiver's epoch
// and liveness fences. Deadlines are fixed for foreign owners; only a local
// route observed continuously within its safe lease window may be extended.
type ownerRoute struct {
	owner      cell.Owner
	etag       string
	until      time.Time
	wallExpiry int64
	local      bool
}
type routeFlight struct {
	done  chan struct{}
	rev   uint64
	owner cell.Owner
	etag  string
	err   error
	retry bool
}

func (m *Manager) localLease(o cell.Owner) *lease.Manager {
	lm, ok := m.LeaseView.(*lease.Manager)
	if ok && o.Liveness == "node-lease" && o.Node == m.NodeID && o.Session == m.Session && lm.NodeID == m.NodeID && lm.Session == m.Session {
		return lm
	}
	return nil
}

// ResolveRoute resolves a cell's owner with lease-bounded caching and one
// in-flight lookup per scope. It does not claim ownership or replay requests.
// Legacy/DO records retain the short cache; control-plane callers needing an
// authoritative read must continue to use Resolve (or ResolveCached ttl=0).
func (m *Manager) ResolveRoute(ctx context.Context, s cell.Scope) (cell.Owner, string, error) {
	key := s.String()
	for {
		if err := ctx.Err(); err != nil {
			return cell.Owner{}, "", err
		}
		m.mu.Lock()
		now := time.Now()
		if c, ok := m.routes[key]; ok {
			valid := now.Before(c.until) && now.UnixMilli() < c.wallExpiry
			if valid && c.local {
				lm := m.localLease(c.owner)
				valid = lm != nil && lm.Healthy(now)
				if valid {
					c.until = lm.HealthyUntil()
					c.wallExpiry = c.until.UnixMilli()
					m.routes[key] = c
				}
			}
			if valid {
				m.mu.Unlock()
				return c.owner, c.etag, nil
			}
			delete(m.routes, key)
		}
		if f := m.routeFlights[key]; f != nil {
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return cell.Owner{}, "", ctx.Err()
			case <-f.done:
			}
			m.mu.Lock()
			current := m.cacheRev[key] == f.rev
			m.mu.Unlock()
			if !current || f.retry {
				continue
			}
			// A cancelled leader must not cancel unrelated waiting requests.
			if errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded) {
				continue
			}
			return f.owner, f.etag, f.err
		}
		f := &routeFlight{done: make(chan struct{}), rev: m.cacheRev[key]}
		if m.routeFlights == nil {
			m.routeFlights = make(map[string]*routeFlight)
		}
		m.routeFlights[key] = f
		m.mu.Unlock()

		// Sample the local lease before the bucket read. A delayed response must
		// not be made reusable by a lease that was revived while that read waited.
		var localUntil time.Time
		if lm, ok := m.LeaseView.(*lease.Manager); ok {
			localUntil = lm.HealthyUntil()
		}
		issued := time.Now()
		o, e, err := m.Resolve(ctx, s)
		c := ownerRoute{owner: o, etag: e}
		if err == nil {
			switch {
			case m.localLease(o) != nil:
				c.local = true
				c.until = localUntil
				c.wallExpiry = localUntil.UnixMilli()
			case o.Liveness == "node-lease" && m.LeaseView != nil:
				l, ok, lerr := m.LeaseView.NodeLease(ctx, o.Node)
				if lerr == nil && (!ok || l.Node != o.Node || l.Session != o.Session || !l.Live(time.Now())) {
					if fresh, available := m.LeaseView.(FreshLeaseView); available {
						l, ok, lerr = fresh.NodeLeaseFresh(ctx, o.Node)
					}
				}
				if lerr != nil {
					err = lerr
				} else if ok && l.Node == o.Node && l.Session == o.Session {
					c.wallExpiry = l.Expiry
					// Pair response wall time with the earlier monotonic instant, so a
					// delayed read can only shorten reuse, never extend the lease.
					c.until = issued.Add(time.Duration(l.Expiry-time.Now().UnixMilli()) * time.Millisecond)
				}
			default:
				c.until = issued.Add(time.Second)
				c.wallExpiry = o.Expiry
			}
		}
		// A delayed local owner read may straddle expiry and takeover. A new
		// healthy publication cannot validate that pre-gap response, even for
		// the current request (not merely its subsequent cache reuse).
		retryLocal := false
		if err == nil && c.local && !time.Now().Before(localUntil) {
			retryLocal = m.localLease(o).Healthy(time.Now())
		}
		m.mu.Lock()
		current := m.cacheRev[key] == f.rev
		if current && err == nil && time.Now().Before(c.until) && time.Now().UnixMilli() < c.wallExpiry {
			if m.routes == nil {
				m.routes = make(map[string]ownerRoute)
			}
			if len(m.routes) >= maxOwnerRoutes {
				for victim := range m.routes {
					delete(m.routes, victim)
					break
				}
			}
			m.routes[key] = c
		}
		f.owner, f.etag, f.err, f.retry = o, e, err, retryLocal
		delete(m.routeFlights, key)
		close(f.done)
		m.mu.Unlock()
		if !current || retryLocal {
			continue
		}
		return o, e, err
	}
}

// LiveRoute reuses the lease evidence attached to this exact routing hint.
// Takeover and administrative checks continue to use Live/liveFresh instead.
func (m *Manager) LiveRoute(ctx context.Context, s cell.Scope, o cell.Owner, now time.Time) (bool, error) {
	m.mu.Lock()
	c, ok := m.routes[s.String()]
	valid := ok && c.owner == o && now.Before(c.until) && now.UnixMilli() < c.wallExpiry
	m.mu.Unlock()
	if valid && !c.local && o.Liveness == "node-lease" && m.LeaseView != nil {
		return true, nil
	}
	return m.Live(ctx, o, now)
}

// LocalReadGuard proves a read is covered by an authoritative local route and
// the same healthy process lease. Call the returned check AFTER the read too:
// a release or lease boundary during SQLite access invalidates its result.
// No manager lock is held across caller I/O.
func (m *Manager) LocalReadGuard(sc cell.Scope) (func() bool, bool) {
	key := sc.String()
	m.mu.Lock()
	c, ok := m.routes[key]
	rev := m.cacheRev[key]
	lm := m.localLease(c.owner)
	now := time.Now()
	valid := ok && c.local && lm != nil && m.releasing[key] == 0 && now.Before(c.until) && now.UnixMilli() < c.wallExpiry && lm.Healthy(now)
	m.mu.Unlock()
	if !valid {
		return nil, false
	}
	return func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		current, exists := m.routes[key]
		now := time.Now()
		return exists && current.owner == c.owner && m.cacheRev[key] == rev && m.releasing[key] == 0 && m.localLease(c.owner) == lm && now.Before(c.until) && now.UnixMilli() < c.wallExpiry && lm.Healthy(now)
	}, true
}

// ReadFence detects local ownership transitions while a caller obtains fresh
// evidence and reads SQLite. It is not, by itself, proof of ownership.
func (m *Manager) ReadFence(sc cell.Scope) (func() bool, bool) {
	key := sc.String()
	m.mu.Lock()
	rev := m.cacheRev[key]
	busy := m.releasing[key] > 0
	m.mu.Unlock()
	if busy {
		return nil, false
	}
	return func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.releasing[key] == 0 && m.cacheRev[key] == rev }, true
}
