// Package autoscaler turns node lease signals into a capacity recommendation
// (ADR-035 / roadmap P4). It reads each node's self-published Load (owned cells,
// pressure, shed) and recommends a desired node count. Actual provisioning is
// environment-specific and intentionally out of scope: this is the signal a
// scheduler/orchestrator acts on.
package autoscaler

import (
	"context"
	"log/slog"
	"time"

	"cellhive/internal/lease"
)

// Plan is a capacity recommendation.
type Plan struct {
	Nodes          int    `json:"nodes"`
	OwnedCells     int    `json:"owned_cells"`
	PressuredNodes int    `json:"pressured_nodes"`
	ShedCells      int    `json:"shed_cells"`
	DesiredNodes   int    `json:"desired_nodes"`
	Action         string `json:"action"` // scale-up | scale-down | hold
	Reason         string `json:"reason"`
	// CooldownRemainingMs is set when an action change was suppressed by the
	// cooldown window (ADR-151).
	CooldownRemainingMs int64 `json:"cooldown_remaining_ms,omitempty"`
}

// Advisor computes a plan from node leases.
type Advisor struct {
	Min                int
	Max                int
	TargetCellsPerNode int
	Sample             func(ctx context.Context) ([]lease.NodeLease, error)
	Now                func() time.Time
	// Cooldown suppresses an action *change* for this long after the previous
	// change, so an orchestrator does not flap on noisy signals (ADR-151).
	// 0 disables it (previous behavior).
	Cooldown time.Duration

	lastAction   string
	lastActionAt time.Time
}

func (a *Advisor) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// Recommend returns the desired capacity. Nodes whose lease is expired are not
// counted. Pressure/shed forces at least one more node (bounded by Max).
func (a *Advisor) Recommend(ctx context.Context) (Plan, error) {
	target := a.TargetCellsPerNode
	if target <= 0 {
		target = 100
	}
	leases, err := a.Sample(ctx)
	if err != nil {
		return Plan{}, err
	}
	now := a.now()
	var p Plan
	for _, l := range leases {
		if l.Expiry > 0 && l.Expiry < now.UnixMilli() {
			continue // expired
		}
		p.Nodes++
		p.OwnedCells += l.Load.OwnedCells
		p.ShedCells += l.Load.ShedCells
		if l.Load.Pressured {
			p.PressuredNodes++
		}
	}
	desired := (p.OwnedCells + target - 1) / target // ceil
	if p.PressuredNodes > 0 || p.ShedCells > 0 {
		if desired < p.Nodes+1 {
			desired = p.Nodes + 1
		}
	}
	if desired < a.Min {
		desired = a.Min
	}
	if a.Max > 0 && desired > a.Max {
		desired = a.Max
	}
	p.DesiredNodes = desired
	switch {
	case p.Nodes == 0:
		p.Action, p.Reason = "scale-up", "no live nodes"
	case desired > p.Nodes:
		p.Action, p.Reason = "scale-up", "cells-per-node above target or pressure"
	case desired < p.Nodes:
		p.Action, p.Reason = "scale-down", "cells-per-node below target"
	default:
		p.Action, p.Reason = "hold", "within target"
	}
	// Cooldown: keep the previous action until the window elapses, so a noisy
	// signal cannot flip scale-up/scale-down every interval (ADR-151).
	if a.Cooldown > 0 && a.lastAction != "" && a.lastAction != p.Action {
		if elapsed := now.Sub(a.lastActionAt); elapsed < a.Cooldown {
			p.CooldownRemainingMs = (a.Cooldown - elapsed).Milliseconds()
			p.Action, p.Reason = "hold", "cooldown"
			return p, nil
		}
	}
	if p.Action != "hold" {
		a.lastAction, a.lastActionAt = p.Action, now
	}
	return p, nil
}

// Actuator applies a plan to the environment (add/remove nodes). The platform
// ships a no-op default; a real deployment plugs in its orchestrator here, so
// the autoscaler signal actually drives scaling without the core depending on
// any cloud SDK.
type Actuator interface {
	Apply(ctx context.Context, p Plan) error
}

// LogActuator only logs the recommendation (safe default).
type LogActuator struct{ Log *slog.Logger }

func (a LogActuator) Apply(_ context.Context, p Plan) error {
	log := a.Log
	if log == nil {
		log = slog.Default()
	}
	log.Info("autoscaler", "action", p.Action, "nodes", p.Nodes, "desired", p.DesiredNodes, "owned", p.OwnedCells, "reason", p.Reason)
	return nil
}

// Run periodically recommends and applies a plan until ctx is cancelled.
func (a *Advisor) Run(ctx context.Context, interval time.Duration, act Actuator, log *slog.Logger) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if act == nil {
		act = LogActuator{Log: log}
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p, err := a.Recommend(ctx)
				if err != nil {
					if log != nil {
						log.Warn("autoscaler recommend failed", "err", err)
					}
					continue
				}
				if p.Action != "hold" {
					if err := act.Apply(ctx, p); err != nil && log != nil {
						log.Warn("autoscaler apply failed", "err", err)
					}
				}
			}
		}
	}()
}
