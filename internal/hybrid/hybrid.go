// Package hybrid drives the per-pod static/dynamic decision: it turns ingress
// per-host counters into a hot set, renders that set into a static generation,
// and refreshes the plan when versions change. Everything is local to one
// runtime instance; the dynamic loader remains the fallback.
package hybrid

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"cellhive/internal/hotset"
	"cellhive/internal/plangen"
	"cellhive/internal/staticgen"
)

// Builder renders a set of hosts into a static plan.
type Builder interface {
	Build(ctx context.Context, hosts []string) (*staticgen.Plan, []plangen.Skipped, error)
}

// Manager applies static generations and publishes the routing table.
type Manager interface {
	Apply(ctx context.Context, plan *staticgen.Plan) error
	DropHosts(hosts []string) error
}

// Config controls the coordinator cadence.
type Config struct {
	Policy          hotset.Policy
	Window          time.Duration // scoring window (default 5m)
	RefreshInterval time.Duration // plan refresh for version changes (default 30s)
	Logger          *slog.Logger
}

// Coordinator is a single-goroutine driver (call Tick/Refresh serially).
type Coordinator struct {
	cfg Config
	hot Builder
	mgr Manager

	lastSnapshot map[string]uint64
	buckets      map[string][]bucket
	static       []string
	lastPlan     []byte
	lastRoutes   map[string]string // host -> service (version-encoded)
}

type bucket struct {
	at    time.Time
	count uint64
}

// New builds a coordinator.
func New(cfg Config, hot Builder, mgr Manager) *Coordinator {
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Minute
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Coordinator{
		cfg: cfg, hot: hot, mgr: mgr,
		lastSnapshot: map[string]uint64{},
		buckets:      map[string][]bucket{},
	}
}

// Tick consumes a cumulative per-host snapshot and may roll the static set.
// It returns the decision and whether a new generation was applied.
func (c *Coordinator) Tick(ctx context.Context, snapshot map[string]uint64, now time.Time) (hotset.Decision, bool) {
	stats := c.accumulate(snapshot, now)
	decision := hotset.Select(c.cfg.Policy, stats, c.static, now)
	if !decision.Render {
		return decision, false
	}
	c.static = append([]string(nil), decision.Static...)
	applied, err := c.render(ctx)
	if err != nil {
		c.cfg.Logger.Warn("hybrid: render failed", "err", err)
		return decision, false
	}
	return decision, applied
}

// Refresh re-renders the current static set (deploy/version changes) and
// applies only when the plan changed.
func (c *Coordinator) Refresh(ctx context.Context) (bool, error) {
	return c.render(ctx)
}

func (c *Coordinator) accumulate(snapshot map[string]uint64, now time.Time) map[string]hotset.Stats {
	for host, total := range snapshot {
		prev := c.lastSnapshot[host]
		if total < prev {
			prev = 0
		}
		delta := total - prev
		if delta > 0 {
			c.buckets[host] = append(c.buckets[host], bucket{at: now, count: delta})
		}
		c.lastSnapshot[host] = total
	}
	cutoff := now.Add(-c.cfg.Window)
	stats := make(map[string]hotset.Stats, len(c.buckets))
	for host, buckets := range c.buckets {
		keep := buckets[:0]
		var sum int64
		for _, b := range buckets {
			if b.at.Before(cutoff) {
				continue
			}
			keep = append(keep, b)
			sum += int64(b.count)
		}
		if len(keep) == 0 {
			delete(c.buckets, host)
			continue
		}
		c.buckets[host] = keep
		stats[host] = hotset.Stats{Requests: sum}
	}
	// Hosts that produced no new delta but are still inside the window retain
	// their accumulated score via c.buckets above.
	return stats
}

func (c *Coordinator) render(ctx context.Context) (bool, error) {
	plan, _, err := c.hot.Build(ctx, c.static)
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return false, err
	}
	if bytes.Equal(data, c.lastPlan) {
		return false, nil
	}
	// Deploy/rollback: a host whose service identity (version) changed must
	// leave the static table immediately; the new generation is rendered right
	// after, but the request path never keeps serving the old immutable version
	// for the whole render window.
	if len(c.lastRoutes) > 0 {
		var changed []string
		for host, service := range plan.Routes {
			if prev, ok := c.lastRoutes[host]; ok && prev != service {
				changed = append(changed, host)
			}
		}
		if len(changed) > 0 {
			if err := c.mgr.DropHosts(changed); err != nil {
				c.cfg.Logger.Warn("hybrid: drop changed hosts failed", "err", err)
			}
		}
	}
	if err := c.mgr.Apply(ctx, plan); err != nil {
		return false, err
	}
	c.lastPlan = data
	c.lastRoutes = plan.Routes
	return true, nil
}
