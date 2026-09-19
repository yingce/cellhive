// Package rebalance plans weight-proportional ownership moves across a fleet
// (ADR-101). The planner is a pure function so it is easy to test; the caller
// (cell-agent) releases the chosen idle cells, and a peer claims them on demand.
package rebalance

import "sort"

// Load is one node's placement load.
type Load struct {
	Node   string
	Weight int // placement weight (default: CPU count)
	Owned  int // owned cells
}

// Candidate is a cell this node owns that could be released.
type Candidate struct {
	Scope string
	Epoch uint64
	Idle  bool // no open handle: safe to move without disturbing a running cell
}

// Plan returns the idle owned cells this node should release so ownership moves
// toward a weight-proportional target. It returns nothing unless this node is
// above its target and some peer is below its own target with 2% headroom. It
// moves at most maxMove cells, in deterministic (scope-sorted) order.
func Plan(self Load, peers []Load, owned []Candidate, maxMove int) []Candidate {
	if maxMove <= 0 || self.Weight <= 0 {
		return nil
	}
	totalWeight := self.Weight
	totalOwned := self.Owned
	for _, p := range peers {
		if p.Weight > 0 {
			totalWeight += p.Weight
			totalOwned += p.Owned
		}
	}
	if totalWeight <= 0 {
		return nil
	}
	targetSelf := float64(totalOwned) * float64(self.Weight) / float64(totalWeight)
	over := float64(self.Owned) - targetSelf
	if over <= 0 {
		return nil
	}
	// The peer furthest below its target, keeping 2% headroom on the receiver.
	bestRoom := 0.0
	for _, p := range peers {
		if p.Weight <= 0 {
			continue
		}
		target := float64(totalOwned) * float64(p.Weight) / float64(totalWeight)
		if room := target*0.98 - float64(p.Owned); room > bestRoom {
			bestRoom = room
		}
	}
	if bestRoom <= 0 {
		return nil
	}
	move := int(over)
	if r := int(bestRoom); r < move {
		move = r
	}
	if move > maxMove {
		move = maxMove
	}
	if move <= 0 {
		return nil
	}
	idle := make([]Candidate, 0, len(owned))
	for _, c := range owned {
		if c.Idle {
			idle = append(idle, c)
		}
	}
	sort.Slice(idle, func(i, j int) bool { return idle[i].Scope < idle[j].Scope })
	if move > len(idle) {
		move = len(idle)
	}
	if move == 0 {
		return nil
	}
	return idle[:move]
}
