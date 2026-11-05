package owner

import (
	"strconv"
	"testing"

	"cellhive/internal/cell"
)

func TestOwnerCacheRevIsBounded(t *testing.T) {
	m := &Manager{}
	for i := 0; i < 20000; i++ {
		m.Invalidate(cell.Scope{Namespace: "demo", Class: "__kv__", ID: strconv.Itoa(i)})
	}
	m.mu.Lock()
	revs := len(m.cacheRev)
	at := len(m.cacheRevAt)
	m.mu.Unlock()
	if revs > maxOwnerCacheRev {
		t.Fatalf("cacheRev grew to %d, want <= %d", revs, maxOwnerCacheRev)
	}
	if at > revs {
		t.Fatalf("cacheRevAt grew to %d for %d revisions", at, revs)
	}
}

func TestOwnerCacheRevKeepsActiveGuards(t *testing.T) {
	m := &Manager{}
	key := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "guarded"}.String()
	m.Invalidate(cell.Scope{Namespace: "demo", Class: "__kv__", ID: "guarded"})
	m.mu.Lock()
	m.guards = map[string]int{key: 1}
	m.mu.Unlock()
	for i := 0; i < maxOwnerCacheRev*2; i++ {
		m.Invalidate(cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other-" + strconv.Itoa(i)})
	}
	m.mu.Lock()
	_, kept := m.cacheRev[key]
	m.mu.Unlock()
	if !kept {
		t.Fatal("revision for an active read guard was dropped")
	}
}

func TestReadFenceRejectsRevisionTrimmedAfterGuardRelease(t *testing.T) {
	m := &Manager{}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "fence-trim"}
	key := sc.String()
	fence, ok := m.ReadFence(sc)
	if !ok {
		t.Fatal("missing fence")
	}
	// Simulate the old closure bug: the guard is released before the revision
	// check, and a concurrent trim then removes the (zero) revision entry.
	m.mu.Lock()
	m.releaseGuardLocked(key)
	m.dropRevLocked(key)
	m.mu.Unlock()
	if fence() {
		t.Fatal("stale fence accepted after its revision was trimmed")
	}
}

func TestReadFenceGuardIsReleasedAndRemainsConservative(t *testing.T) {
	m := &Manager{}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "fence-reuse"}
	key := sc.String()
	m.Invalidate(sc)
	fence, ok := m.ReadFence(sc)
	if !ok {
		t.Fatal("missing fence")
	}
	if !fence() {
		t.Fatal("healthy fence rejected")
	}
	m.mu.Lock()
	_, leaked := m.guards[key]
	m.mu.Unlock()
	if leaked {
		t.Fatal("read fence guard leaked after its first check")
	}
	m.Invalidate(sc)
	if fence() {
		t.Fatal("reusable fence accepted an invalidated revision")
	}
}
