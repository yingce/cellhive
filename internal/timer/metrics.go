package timer

import (
	"sync"
	"sync/atomic"
)

// fireStats counts dispatch outcomes by timer kind for /metrics
// (cellhive_waker_fires_total). It is package-level like pagedvfs.SnapshotStatsAll
// so the metrics handler needs no extra wiring.
var fireStats sync.Map // "kind|outcome" -> *atomic.Int64

func observeFire(kind, outcome string) {
	v, _ := fireStats.LoadOrStore(kind+"|"+outcome, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

// FireStats reports timer dispatch counts keyed "kind|outcome".
func FireStats() map[string]int64 {
	out := map[string]int64{}
	fireStats.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}
