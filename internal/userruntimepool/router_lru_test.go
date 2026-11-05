package userruntimepool

import (
	"strconv"
	"testing"
	"time"
)

func TestHostCacheEvictsLeastRecentlyUsedNotFullScan(t *testing.T) {
	r := NewRouter("http://unused", "internal", "dispatch")
	r.mu.Lock()
	defer r.mu.Unlock()
	exp := time.Now().Add(time.Minute)
	r.storeLocked("h0.test", hostEntry{expires: exp})
	for i := 1; i < maxCachedHosts; i++ {
		r.storeLocked("h"+strconv.Itoa(i)+".test", hostEntry{expires: exp})
	}
	// Touch the oldest entry, then overflow by one: the untouched oldest should
	// leave, not the recently used entry.
	r.storeLocked("h0.test", hostEntry{expires: exp})
	r.storeLocked("new.test", hostEntry{expires: exp})
	if len(r.hosts) != maxCachedHosts {
		t.Fatalf("cache size = %d, want %d", len(r.hosts), maxCachedHosts)
	}
	if _, ok := r.hosts["h0.test"]; !ok {
		t.Fatal("recently used host was evicted")
	}
	if _, ok := r.hosts["h1.test"]; ok {
		t.Fatal("least recently used host survived")
	}
	if r.hostOrder.Len() != len(r.hosts) || len(r.hostLRU) != len(r.hosts) {
		t.Fatalf("LRU bookkeeping diverged: order=%d index=%d hosts=%d", r.hostOrder.Len(), len(r.hostLRU), len(r.hosts))
	}
}
