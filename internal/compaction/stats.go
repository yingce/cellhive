package compaction

import (
	"context"
	"sort"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

// ScopeMetrics is the last recorded diagnostic snapshot for one scope. It is
// refreshed by explicit diagnostics/admin maintenance; compaction and metrics
// scrapes do not trigger broad diagnostic listings.
type ScopeMetrics struct {
	Stats     replica.BacklogStats `json:"stats"`
	Error     string               `json:"error,omitempty"`
	UpdatedMs int64                `json:"updated_ms"`
}

var (
	metricsMu sync.RWMutex
	metrics   = map[string]ScopeMetrics{}
)

// RecordStats refreshes the diagnostic snapshot for a scope. Errors are kept so
// a scrape can expose a stale/unavailable state instead of reporting zeros as
// truth.
func RecordStats(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64) {
	st, err := m.BacklogStats(ctx, s, epoch)
	RecordStatsSnapshot(st, err)
}

// RecordStatsSnapshot reuses a diagnostic result without another bucket scan.
func RecordStatsSnapshot(st replica.BacklogStats, err error) {
	rec := ScopeMetrics{Stats: st, UpdatedMs: time.Now().UnixMilli()}
	if err != nil {
		rec.Error = err.Error()
	}
	metricsMu.Lock()
	metrics[st.Scope] = rec
	metricsMu.Unlock()
}

// SnapshotMetrics returns a deterministic copy for the metrics endpoint.
func SnapshotMetrics() []ScopeMetrics {
	metricsMu.RLock()
	out := make([]ScopeMetrics, 0, len(metrics))
	for _, rec := range metrics {
		out = append(out, rec)
	}
	metricsMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Stats.Scope < out[j].Stats.Scope })
	return out
}

// ResetMetricsForTest clears the process-wide registry. Tests that assert exact
// metric sets call it; production never does.
func ResetMetricsForTest() {
	metricsMu.Lock()
	metrics = map[string]ScopeMetrics{}
	metricsMu.Unlock()
}
