package autoscaler

import (
	"context"
	"sync"
	"testing"
	"time"

	"cellhive/internal/lease"
)

func sample(leases []lease.NodeLease) func(context.Context) ([]lease.NodeLease, error) {
	return func(context.Context) ([]lease.NodeLease, error) { return leases, nil }
}

func TestAdvisorScalesUpForLoadAndPressure(t *testing.T) {
	now := time.Unix(1000, 0)
	live := func(node string, cells int, pressured bool) lease.NodeLease {
		return lease.NodeLease{Node: node, Expiry: now.Add(time.Minute).UnixMilli(),
			Load: lease.Load{OwnedCells: cells, Pressured: pressured}}
	}
	a := &Advisor{Min: 1, Max: 10, TargetCellsPerNode: 100, Now: func() time.Time { return now }}

	a.Sample = sample([]lease.NodeLease{live("n1", 120, false), live("n2", 120, false)})
	p, err := a.Recommend(context.Background())
	if err != nil || p.DesiredNodes != 3 || p.Action != "scale-up" {
		t.Fatalf("plan = %+v, %v (want desired 3 scale-up)", p, err)
	}

	// Pressure forces at least one extra node even below target.
	a.Sample = sample([]lease.NodeLease{live("n1", 10, true)})
	p, _ = a.Recommend(context.Background())
	if p.DesiredNodes != 2 || p.Action != "scale-up" {
		t.Fatalf("pressure plan = %+v (want desired 2)", p)
	}

	// Expired leases are ignored.
	expired := live("n1", 500, false)
	expired.Expiry = now.Add(-time.Minute).UnixMilli()
	a.Sample = sample([]lease.NodeLease{expired})
	p, _ = a.Recommend(context.Background())
	if p.Nodes != 0 || p.Action != "scale-up" {
		t.Fatalf("expired plan = %+v", p)
	}

	// Scale down when under target.
	a.Sample = sample([]lease.NodeLease{live("n1", 20, false), live("n2", 20, false)})
	p, _ = a.Recommend(context.Background())
	if p.Action != "scale-down" || p.DesiredNodes != 1 {
		t.Fatalf("scale-down plan = %+v", p)
	}
}

type recordingActuator struct {
	mu    sync.Mutex
	plans []Plan
}

func (r *recordingActuator) Apply(_ context.Context, p Plan) error {
	r.mu.Lock()
	r.plans = append(r.plans, p)
	r.mu.Unlock()
	return nil
}

func (r *recordingActuator) snapshot() []Plan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Plan(nil), r.plans...)
}

func TestAdvisorRunAppliesPlan(t *testing.T) {
	now := time.Unix(1000, 0)
	a := &Advisor{Min: 1, Max: 10, TargetCellsPerNode: 10, Now: func() time.Time { return now },
		Sample: sample([]lease.NodeLease{{Node: "n1", Expiry: now.Add(time.Minute).UnixMilli(), Load: lease.Load{OwnedCells: 50}}})}
	rec := &recordingActuator{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Run(ctx, 5*time.Millisecond, rec, nil)
	deadline := time.Now().Add(time.Second)
	for len(rec.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	plans := rec.snapshot()
	if len(plans) == 0 {
		t.Fatal("actuator never invoked")
	}
	if plans[0].Action != "scale-up" {
		t.Fatalf("plan = %+v", plans[0])
	}
}

// TestAdvisorCooldown: an action change is suppressed until the cooldown window
// elapses, so a noisy signal cannot flap scale-up/scale-down every interval
// (ADR-151).
func TestAdvisorCooldown(t *testing.T) {
	now := time.Unix(2000, 0)
	clock := func() time.Time { return now }
	farFuture := time.Unix(1<<40, 0).UnixMilli() // survives advancing the test clock
	live := func(node string, cells int) lease.NodeLease {
		return lease.NodeLease{Node: node, Expiry: farFuture,
			Load: lease.Load{OwnedCells: cells}}
	}
	a := &Advisor{Min: 1, Max: 20, TargetCellsPerNode: 100, Now: clock, Cooldown: time.Minute}

	// First signal: scale up (2 nodes × 120 cells → desired 3).
	a.Sample = sample([]lease.NodeLease{live("n1", 120), live("n2", 120)})
	p, err := a.Recommend(context.Background())
	if err != nil || p.Action != "scale-up" {
		t.Fatalf("first plan = %+v, %v", p, err)
	}
	// Immediately after, an opposite signal must be held by the cooldown.
	a.Sample = sample([]lease.NodeLease{live("n1", 1), live("n2", 1), live("n3", 1)})
	p, _ = a.Recommend(context.Background())
	if p.Action != "hold" || p.Reason != "cooldown" || p.CooldownRemainingMs <= 0 {
		t.Fatalf("cooldown plan = %+v (want hold/cooldown with remaining)", p)
	}
	// After the window, the change is allowed.
	now = now.Add(2 * time.Minute)
	p, _ = a.Recommend(context.Background())
	if p.Action != "scale-down" || p.Reason == "cooldown" {
		t.Fatalf("post-cooldown plan = %+v (want scale-down)", p)
	}
	// A no-cooldown advisor keeps the old behavior.
	b := &Advisor{Min: 1, Max: 20, TargetCellsPerNode: 100, Now: clock}
	b.Sample = sample([]lease.NodeLease{live("n1", 120), live("n2", 120)})
	if p, _ := b.Recommend(context.Background()); p.Action != "scale-up" {
		t.Fatalf("no-cooldown first = %+v", p)
	}
	b.Sample = sample([]lease.NodeLease{live("n1", 1), live("n2", 1), live("n3", 1)})
	if p, _ := b.Recommend(context.Background()); p.Action != "scale-down" {
		t.Fatalf("no-cooldown second = %+v (want scale-down)", p)
	}
}
