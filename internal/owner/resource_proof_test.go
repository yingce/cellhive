package owner

import (
	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"fmt"
	"testing"
	"time"
)

func TestResourceProofRetainedUntilRelease(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(b, m.NodeID, m.Session, "", "", time.Minute)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "proof"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	p, ok := m.LocalResourceProof(sc)
	if !ok {
		t.Fatal("missing proof")
	}
	for i := 0; i < 3; i++ {
		if !p.Validate() {
			t.Fatal("valid proof rejected")
		}
	}
	m.mu.Lock()
	guards := m.guards[sc.String()]
	m.mu.Unlock()
	if guards != 1 {
		t.Fatalf("validation released guard: %d", guards)
	}
	m.Invalidate(sc)
	if p.Validate() {
		t.Fatal("proof survived owner invalidation")
	}
	p.Release()
	p.Release()
	if p.Validate() {
		t.Fatal("released proof valid")
	}
	m.mu.Lock()
	guards = m.guards[sc.String()]
	m.mu.Unlock()
	if guards != 0 {
		t.Fatalf("guard leak: %d", guards)
	}
}

func TestResourceProofPinnedRouteCapacity(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(b, m.NodeID, m.Session, "", "", time.Minute)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "pinned"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	p, ok := m.LocalResourceProof(sc)
	if !ok {
		t.Fatal("missing proof")
	}
	defer p.Release()
	m.mu.Lock()
	for i := 1; i < maxOwnerRoutes; i++ {
		key := fmt.Sprintf("app/__kv__/fill%d", i)
		m.routes[key] = m.routes[sc.String()]
		m.guards[key] = 1
	}
	m.mu.Unlock()
	other := cell.Scope{Namespace: "app", Class: "__kv__", ID: "uncached"}
	want, err := m.Claim(t.Context(), other, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := m.ResolveRoute(t.Context(), other)
	if err != nil || got != want {
		t.Fatalf("fresh lookup blocked: %+v %v", got, err)
	}
	if !p.Validate() {
		t.Fatal("unrelated full cache evicted admitted proof")
	}
	m.mu.Lock()
	size := len(m.routes)
	_, cached := m.routes[other.String()]
	delete(m.guards, "app/__kv__/fill1")
	m.mu.Unlock()
	if size != maxOwnerRoutes || cached {
		t.Fatalf("capacity violated: %d cached=%v", size, cached)
	}
	if _, _, err := m.ResolveRoute(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	_, cached = m.routes[other.String()]
	m.mu.Unlock()
	if !cached || !p.Validate() {
		t.Fatal("released slot not reusable or pinned proof invalidated")
	}
}
