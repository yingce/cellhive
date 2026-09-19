// Package admission implements per-namespace write-rate admission (ADR-035):
// a token bucket per namespace so one tenant cannot starve others of the
// replication/durability path. It also tracks shed counts for diagnostics.
package admission

import (
	"sync"
	"sync/atomic"
	"time"
)

// Limiter is a per-namespace token bucket. A zero/negative rate disables it.
type Limiter struct {
	rate  float64
	burst float64
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
	shed    atomic.Uint64
	allowed atomic.Uint64
}

type bucket struct {
	tokens float64
	at     time.Time
}

// New creates a limiter. rps <= 0 returns a disabled limiter that allows all.
func New(rps, burst float64) *Limiter {
	if burst <= 0 {
		burst = rps
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{rate: rps, burst: burst, now: time.Now, buckets: map[string]*bucket{}}
}

// Enabled reports whether the limiter enforces a rate.
func (l *Limiter) Enabled() bool { return l != nil && l.rate > 0 }

// Allow consumes one token for ns, returning false when the bucket is empty.
func (l *Limiter) Allow(ns string) bool {
	if !l.Enabled() {
		return true
	}
	now := l.now()
	l.mu.Lock()
	b := l.buckets[ns]
	if b == nil {
		b = &bucket{tokens: l.burst, at: now}
		l.buckets[ns] = b
		// Opportunistic cap so a flood of namespaces cannot grow unbounded.
		if len(l.buckets) > 100000 {
			for k := range l.buckets {
				if k != ns {
					delete(l.buckets, k)
					break
				}
			}
		}
	}
	elapsed := now.Sub(b.at).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.at = now
	}
	if b.tokens < 1 {
		l.mu.Unlock()
		l.shed.Add(1)
		return false
	}
	b.tokens--
	l.mu.Unlock()
	l.allowed.Add(1)
	return true
}

// Shed returns the number of rejected writes.
func (l *Limiter) Shed() uint64 {
	if l == nil {
		return 0
	}
	return l.shed.Load()
}

// Allowed returns the number of admitted writes.
func (l *Limiter) Allowed() uint64 {
	if l == nil {
		return 0
	}
	return l.allowed.Load()
}
