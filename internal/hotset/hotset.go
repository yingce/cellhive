// Package hotset selects which tenant hosts (worker routes) should be rendered
// as static workerd services on one runtime instance. Selection is local to the
// pod: each instance keeps its own counters and converges independently, while
// the dynamic loader remains the correctness fallback for every host.
package hotset

import (
	"sort"
	"time"
)

// Policy is the hysteresis and cadence configuration for hot-set selection.
type Policy struct {
	// Max is the number of static entries kept (0 disables static serving).
	Max int
	// Window is the scoring window (default 5m).
	Window time.Duration
	// EvalInterval is how often the manager re-evaluates (default 60s).
	EvalInterval time.Duration
	// MinRenderInterval is the cooldown between actual renders (default 5m).
	MinRenderInterval time.Duration
	// MinQPS is the per-edge minimum request rate to enter the static set.
	MinQPS float64
	// SwapRatio bounds how many entries may change per render (default 0.1).
	SwapRatio float64
	// ReplaceFactor requires a challenger to beat an incumbent by this factor
	// (default 2.0) before it can displace it.
	ReplaceFactor float64
	// DeployStability is how long after a version change a route stays dynamic
	// before it is eligible for static rendering (default 5m).
	DeployStability time.Duration
}

// Defaults fills unset policy fields.
func (p Policy) Defaults() Policy {
	if p.Window <= 0 {
		p.Window = 5 * time.Minute
	}
	if p.EvalInterval <= 0 {
		p.EvalInterval = time.Minute
	}
	if p.MinRenderInterval <= 0 {
		p.MinRenderInterval = 5 * time.Minute
	}
	if p.SwapRatio <= 0 || p.SwapRatio > 1 {
		p.SwapRatio = 0.1
	}
	if p.ReplaceFactor < 1 {
		p.ReplaceFactor = 2
	}
	if p.DeployStability <= 0 {
		p.DeployStability = 5 * time.Minute
	}
	if p.MinQPS < 0 {
		p.MinQPS = 0
	}
	return p
}

// Stats is the per-key observation supplied by the ingress/loader counters.
type Stats struct {
	// Requests is the number of requests in the scoring window.
	Requests int64
	// LastDeploy is when the active version last changed (zero = unknown).
	LastDeploy time.Time
}

// Decision is the outcome of one evaluation.
type Decision struct {
	// Static is the desired static host set, sorted.
	Static []string
	// Add/Remove are the diff against the previously rendered set.
	Add    []string
	Remove []string
	// Render is true when the change is material enough to restart workerd.
	Render bool
	// Reason is a short human-readable explanation for logs/metrics.
	Reason string
}

// Select applies load, floor and hysteresis rules and returns the desired set.
// previous is the currently rendered set. now is used for the deploy-stability
// check. Deploy-triggered immediate renders are handled by the caller; this
// function only decides membership.
func Select(policy Policy, stats map[string]Stats, previous []string, now time.Time) Decision {
	p := policy.Defaults()
	window := p.Window.Seconds()
	if window <= 0 {
		window = 1
	}
	if p.Max <= 0 {
		if len(previous) == 0 {
			return Decision{Reason: "static disabled"}
		}
		return Decision{Remove: sortedCopy(previous), Render: true, Reason: "static disabled"}
	}
	type scored struct {
		host string
		qps  float64
	}
	qps := make(map[string]float64, len(stats))
	var candidates []scored
	for host, st := range stats {
		if host == "" {
			continue
		}
		if !st.LastDeploy.IsZero() && now.Sub(st.LastDeploy) < p.DeployStability {
			continue
		}
		rate := float64(st.Requests) / window
		qps[host] = rate
		if rate < p.MinQPS {
			continue
		}
		candidates = append(candidates, scored{host: host, qps: rate})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].qps != candidates[j].qps {
			return candidates[i].qps > candidates[j].qps
		}
		return candidates[i].host < candidates[j].host
	})
	desired := make([]string, 0, p.Max)
	for _, c := range candidates {
		if len(desired) >= p.Max {
			break
		}
		desired = append(desired, c.host)
	}
	desiredSet := make(map[string]bool, len(desired))
	for _, host := range desired {
		desiredSet[host] = true
	}
	prevSet := make(map[string]bool, len(previous))
	for _, host := range previous {
		prevSet[host] = true
	}
	var kept []string
	for _, host := range previous {
		if desiredSet[host] {
			kept = append(kept, host)
		}
	}
	var challengers, incumbents []string
	for _, host := range desired {
		if !prevSet[host] {
			challengers = append(challengers, host)
		}
	}
	for _, host := range previous {
		if !desiredSet[host] {
			incumbents = append(incumbents, host)
		}
	}
	sort.Slice(challengers, func(i, j int) bool { return qps[challengers[i]] > qps[challengers[j]] })
	sort.Slice(incumbents, func(i, j int) bool { return qps[incumbents[i]] < qps[incumbents[j]] })

	maxSwap := int(p.SwapRatio * float64(p.Max))
	if maxSwap < 1 {
		maxSwap = 1
	}
	// Start from the previous set so a limited swap budget keeps incumbents
	// instead of silently dropping them (the desired list alone would shrink
	// the set whenever the contender ranking shifts).
	static := append([]string(nil), previous...)
	var added, removed []string
	if len(static) < p.Max {
		for _, host := range challengers {
			if len(static) >= p.Max {
				break
			}
			static = append(static, host)
			added = append(added, host)
		}
	}
	var remaining []string
	for _, host := range desired {
		if !contains(static, host) {
			remaining = append(remaining, host)
		}
	}
	swaps := 0
	for i, j := 0, 0; i < len(remaining) && j < len(incumbents) && swaps < maxSwap; {
		challenger, incumbent := remaining[i], incumbents[j]
		if qps[challenger] < p.ReplaceFactor*qps[incumbent] {
			break
		}
		for k, host := range static {
			if host == incumbent {
				static[k] = challenger
				break
			}
		}
		added = append(added, challenger)
		removed = append(removed, incumbent)
		i++
		j++
		swaps++
	}
	// Dead incumbents (no traffic observed in the window) release their slot
	// even when the swap budget is exhausted.
	for _, host := range previous {
		if qps[host] != 0 || contains(removed, host) {
			continue
		}
		for k, existing := range static {
			if existing == host {
				static = append(static[:k], static[k+1:]...)
				removed = append(removed, host)
				break
			}
		}
	}
	static = dedupeSorted(static)
	if len(static) > p.Max {
		static = static[:p.Max]
	}
	add, remove := diff(previous, static)
	if len(add) == 0 && len(remove) == 0 {
		return Decision{Static: static, Reason: "no change"}
	}
	// Tolerance: same cardinality, similar membership, and no clearly dominant
	// challenger means the difference is noise, not a reason to restart workerd.
	slack := int(0.1 * float64(p.Max))
	if slack < 1 {
		slack = 1
	}
	overlap := 0
	for _, host := range static {
		if prevSet[host] {
			overlap++
		}
	}
	if len(static) == len(previous) && overlap >= len(previous)-slack {
		dominant := false
		for _, host := range add {
			for _, incumbent := range remove {
				if qps[host] >= p.ReplaceFactor*qps[incumbent] && qps[incumbent] > 0 {
					dominant = true
					break
				}
			}
		}
		if !dominant {
			return Decision{Static: previous, Reason: "within tolerance"}
		}
	}
	reason := "membership change"
	if len(added) > 0 || len(removed) > 0 {
		reason = "hysteresis swap"
	}
	return Decision{Static: static, Add: add, Remove: remove, Render: true, Reason: reason}
}

func diff(previous, want []string) (add, remove []string) {
	prev := make(map[string]bool, len(previous))
	for _, host := range previous {
		prev[host] = true
	}
	next := make(map[string]bool, len(want))
	for _, host := range want {
		next[host] = true
	}
	for host := range next {
		if !prev[host] {
			add = append(add, host)
		}
	}
	for host := range prev {
		if !next[host] {
			remove = append(remove, host)
		}
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

func sortedCopy(hosts []string) []string {
	out := append([]string(nil), hosts...)
	sort.Strings(out)
	return out
}

func dedupeSorted(hosts []string) []string {
	seen := make(map[string]bool, len(hosts))
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if host == "" || seen[host] {
			continue
		}
		seen[host] = true
		out = append(out, host)
	}
	sort.Strings(out)
	return out
}

func contains(hosts []string, want string) bool {
	for _, host := range hosts {
		if host == want {
			return true
		}
	}
	return false
}
