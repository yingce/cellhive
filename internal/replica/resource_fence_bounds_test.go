package replica

import (
	"strconv"
	"testing"
	"time"
)

func TestResourceFenceCacheIsBounded(t *testing.T) {
	m := &Manager{fenceCache: make(map[string]fenceCacheEntry)}
	now := time.Now()
	for i := 0; i < maxFenceCache; i++ {
		m.fenceCache["old-"+strconv.Itoa(i)] = fenceCacheEntry{at: now}
	}
	m.cacheReadFence("new", resourceFence{}, "", 0)
	m.fenceMu.Lock()
	defer m.fenceMu.Unlock()
	if len(m.fenceCache) > maxFenceCache {
		t.Fatalf("fence cache grew to %d, want <= %d", len(m.fenceCache), maxFenceCache)
	}
	if _, ok := m.fenceCache["new"]; !ok {
		t.Fatal("new fence entry missing after sweep")
	}
}
