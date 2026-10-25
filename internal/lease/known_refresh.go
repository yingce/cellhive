package lease

import (
	"context"
	"errors"
	"sync"
	"time"

	"cellhive/internal/bucket"
)

// RunKnownRefresh maintains already discovered nodes without waiting for a
// bucket List. Call once for the agent lifetime; cancellation joins all reads.
// It never extends a lease locally or turns a failed GET into a live proof.
func (m *Manager) RunKnownRefresh(ctx context.Context) {
	m.mu.Lock()
	if m.knownRunning {
		m.mu.Unlock()
		return
	}
	m.knownRunning = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.knownRunning = false; m.mu.Unlock() }()
	interval := m.knownRefreshInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		m.refreshKnown(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) knownTTL() time.Duration {
	if m.TTL > 0 {
		return m.TTL
	}
	return 10 * time.Second
}
func (m *Manager) knownRefreshInterval() time.Duration {
	d := m.knownTTL() / 5
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	if d < 10*time.Millisecond {
		d = 10 * time.Millisecond
	}
	return d
}

func (m *Manager) refreshKnown(ctx context.Context) {
	// Recently expired peers get a bounded recovery window; historical dead
	// records do not add permanent point-read traffic.
	floor := time.Now().Add(-m.knownTTL()).UnixMilli()
	m.mu.Lock()
	nodes := make([]string, 0, len(m.cached))
	for _, l := range m.cached {
		if l.Node != "" && l.Node != m.NodeID && l.Expiry > floor {
			nodes = append(nodes, l.Node)
		}
	}
	m.mu.Unlock()
	if len(nodes) == 0 {
		return
	}
	jobs := make(chan string, len(nodes))
	for _, node := range nodes {
		jobs <- node
	}
	close(jobs)
	workers := len(nodes)
	if workers > 8 {
		workers = 8
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range jobs {
				if ctx.Err() != nil {
					return
				}
				timeout := m.knownRefreshInterval()
				if timeout > time.Second {
					timeout = time.Second
				}
				readCtx, cancel := context.WithTimeout(ctx, timeout)
				started := time.Now()
				l, err := m.Get(readCtx, node)
				// Ignore a late response after timeout/shutdown even if a bucket backend
				// failed to propagate cancellation itself.
				if readCtx.Err() == nil {
					m.applyKnownRead(node, l, err, started)
				}
				cancel()
			}
		}()
	}
	wg.Wait()
}

func (m *Manager) applyKnownRead(node string, l NodeLease, err error, started time.Time) {
	missing := errors.Is(err, bucket.ErrNotFound)
	if err != nil && !missing {
		return
	}
	if !missing && l.Node != node {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.observed[node].After(started) {
		return
	}
	for i, old := range m.cached {
		if old.Node != node {
			continue
		}
		// Readers retain immutable slices after releasing mu.
		next := append([]NodeLease{}, m.cached...)
		if missing {
			next = append(next[:i], next[i+1:]...)
		} else {
			next[i] = l
		}
		m.cached = next
		if m.observed == nil {
			m.observed = make(map[string]time.Time)
		}
		m.observed[node] = started
		return
	}
}

// mergeSampleLocked prevents a slow full scan from overwriting newer point
// observations, including removals. Only one discovery scan runs at a time.
func (m *Manager) mergeSampleLocked(sample []NodeLease, started time.Time) {
	if m.observed == nil {
		m.observed = make(map[string]time.Time)
	}
	current := make(map[string]NodeLease, len(m.cached))
	for _, l := range m.cached {
		current[l.Node] = l
	}
	next := make([]NodeLease, 0, len(sample))
	seen := make(map[string]bool, len(sample))
	for _, l := range sample {
		if seen[l.Node] {
			continue
		}
		seen[l.Node] = true
		if m.observed[l.Node].After(started) {
			if fresh, ok := current[l.Node]; ok {
				next = append(next, fresh)
			}
		} else {
			next = append(next, l)
			m.observed[l.Node] = started
		}
	}
	for _, l := range m.cached {
		if !seen[l.Node] && m.observed[l.Node].After(started) {
			next = append(next, l)
		}
	}
	// A tombstone need only outlive scans that began before it. Do not retain
	// bookkeeping for every node ever discovered.
	for node, at := range m.observed {
		if !seen[node] && !at.After(started) {
			delete(m.observed, node)
		}
	}
	m.cached = next
}
