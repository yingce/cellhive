package bucket

import "sync/atomic"

type authorityReclaimCounters struct {
	retiredDeleted      atomic.Uint64
	retiredDeleteFailed atomic.Uint64
	gcPageScans         atomic.Uint64
	gcPageCandidates    atomic.Uint64
	gcPageDeleted       atomic.Uint64
	gcPageDeleteFailed  atomic.Uint64
}

var authorityReclaim authorityReclaimCounters

// AuthorityStats exposes process-wide retired-segment GC counters for metrics.
// Counters are diagnostic only; they never affect authority decisions.
func AuthorityStats() map[string]uint64 {
	return map[string]uint64{
		"retired_deleted":       authorityReclaim.retiredDeleted.Load(),
		"retired_delete_failed": authorityReclaim.retiredDeleteFailed.Load(),
		"gc_page_scans":         authorityReclaim.gcPageScans.Load(),
		"gc_page_candidates":    authorityReclaim.gcPageCandidates.Load(),
		"gc_page_deleted":       authorityReclaim.gcPageDeleted.Load(),
		"gc_page_delete_failed": authorityReclaim.gcPageDeleteFailed.Load(),
	}
}

func resetAuthorityStatsForTest() {
	authorityReclaim.retiredDeleted.Store(0)
	authorityReclaim.retiredDeleteFailed.Store(0)
	authorityReclaim.gcPageScans.Store(0)
	authorityReclaim.gcPageCandidates.Store(0)
	authorityReclaim.gcPageDeleted.Store(0)
	authorityReclaim.gcPageDeleteFailed.Store(0)
}
