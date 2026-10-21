package hybrid

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cellhive/internal/hotset"
	"cellhive/internal/plangen"
	"cellhive/internal/staticgen"
)

type fakeBuilder struct {
	calls  [][]string
	plans  map[string]*staticgen.Plan
	last   func(hosts []string) *staticgen.Plan
	bumped map[string]int
}

func (f *fakeBuilder) Build(_ context.Context, hosts []string) (*staticgen.Plan, []plangen.Skipped, error) {
	f.calls = append(f.calls, append([]string(nil), hosts...))
	if f.last != nil {
		return f.last(hosts), nil, nil
	}
	p := &staticgen.Plan{Routes: map[string]string{}}
	for _, h := range hosts {
		p.Routes[h] = "svc"
	}
	return p, nil, nil
}

type fakeManager struct {
	applied []*staticgen.Plan
	dropped [][]string
}

func (m *fakeManager) Apply(_ context.Context, p *staticgen.Plan) error {
	m.applied = append(m.applied, p)
	return nil
}

func (m *fakeManager) DropHosts(hosts []string) error {
	m.dropped = append(m.dropped, append([]string(nil), hosts...))
	return nil
}

func hosts(self []string) []string { return self }

func TestTickSelectsAndRenders(t *testing.T) {
	hot := &fakeBuilder{}
	mgr := &fakeManager{}
	c := New(Config{Policy: hotset.Policy{Max: 2, Window: time.Minute, MinQPS: 0}, Window: time.Minute}, hot, mgr)
	now := time.Now()
	snap := map[string]uint64{"a": 100, "b": 50}
	d, ok := c.Tick(context.Background(), snap, now)
	if !ok || !d.Render || len(d.Static) != 2 {
		t.Fatalf("tick = %+v ok=%v", d, ok)
	}
	if len(mgr.applied) != 1 || len(hot.calls) != 1 {
		t.Fatalf("applied=%d calls=%d", len(mgr.applied), len(hot.calls))
	}
	// No change -> tolerance, no render.
	d, ok = c.Tick(context.Background(), snap, now.Add(10*time.Second))
	if ok || d.Render {
		t.Fatalf("second tick should not render: %+v ok=%v", d, ok)
	}
	// Dominant challenger replaces the weakest.
	d, ok = c.Tick(context.Background(), map[string]uint64{"a": 200, "b": 60, "c": 500}, now.Add(20*time.Second))
	if !ok || !d.Render {
		t.Fatalf("dominant tick = %+v ok=%v", d, ok)
	}
	if !strings.Contains(fmt.Sprint(hot.calls[len(hot.calls)-1]), "c") {
		t.Fatalf("builder hosts = %v", hot.calls)
	}
}

func TestRefreshAppliesChangedPlan(t *testing.T) {
	state := 0
	hot := &fakeBuilder{last: func(hosts []string) *staticgen.Plan {
		p := &staticgen.Plan{Routes: map[string]string{}}
		for _, h := range hosts {
			p.Routes[h] = fmt.Sprintf("svc-v%d", state)
		}
		return p
	}}
	mgr := &fakeManager{}
	c := New(Config{Policy: hotset.Policy{Max: 1, Window: time.Minute, MinQPS: 1}, Window: time.Minute}, hot, mgr)
	now := time.Now()
	if _, ok := c.Tick(context.Background(), map[string]uint64{"a": 100}, now); !ok {
		t.Fatal("initial render failed")
	}
	state = 1
	applied, err := c.Refresh(context.Background())
	if err != nil || !applied {
		t.Fatalf("refresh applied=%v err=%v", applied, err)
	}
	if len(mgr.dropped) != 1 || len(mgr.dropped[0]) != 1 || mgr.dropped[0][0] != "a" {
		t.Fatalf("changed version should be dropped first, got %+v", mgr.dropped)
	}
	applied, err = c.Refresh(context.Background())
	if err != nil || applied {
		t.Fatalf("unchanged refresh applied=%v err=%v", applied, err)
	}
}

func TestWindowPrunesOldTraffic(t *testing.T) {
	hot := &fakeBuilder{}
	mgr := &fakeManager{}
	c := New(Config{Policy: hotset.Policy{Max: 1, Window: 10 * time.Second, MinQPS: 0}, Window: 10 * time.Second}, hot, mgr)
	now := time.Now()
	if _, ok := c.Tick(context.Background(), map[string]uint64{"a": 1}, now); !ok {
		t.Fatal("initial render failed")
	}
	// After the window expires, the host must drop out and the empty plan is applied.
	_, ok := c.Tick(context.Background(), map[string]uint64{"a": 1}, now.Add(20*time.Second))
	if !ok {
		t.Fatal("expected an empty-set render")
	}
	last := mgr.applied[len(mgr.applied)-1]
	if len(last.Routes) != 0 {
		t.Fatalf("expected empty plan, got %+v", last.Routes)
	}
}
