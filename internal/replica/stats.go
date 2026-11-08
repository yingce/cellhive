package replica

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

// BacklogStats is a diagnostic snapshot of one scope/epoch's compaction and GC
// backlog. It is a cold-path listing; it must never run on a write hot path.
type BacklogStats struct {
	Scope                    string `json:"scope"`
	Epoch                    uint64 `json:"epoch"`
	L0BacklogObjects         int    `json:"l0_backlog_objects"`
	L0BacklogBytes           int64  `json:"l0_backlog_bytes"`
	L0BacklogBytesKnown      bool   `json:"l0_backlog_bytes_known"`
	OldestL0AgeSeconds       int64  `json:"oldest_l0_age_seconds"`
	OldestL0AgeKnown         bool   `json:"oldest_l0_age_known"`
	LastCompactionLagSeconds int64  `json:"last_compaction_lag_seconds"`
	LastCompactionKnown      bool   `json:"last_compaction_known"`
	L1Generations            int    `json:"l1_generations"`
	DeferredGCBytes          int64  `json:"deferred_gc_bytes"`
	DeferredBytesKnown       bool   `json:"deferred_bytes_known"`
	RetainedEpochs           int    `json:"retained_epochs"`
	RetiredEpochs            int    `json:"retired_epochs"`
	Truncated                bool   `json:"truncated"`
}

const maxStatsObjects = 50000

// BacklogStats lists at most maxStatsObjects objects under the epoch prefix. A
// truncated result is deterministic (lexicographic prefix) and sets Truncated so
// callers do not treat it as a complete byte total.
func (m *Manager) BacklogStats(ctx context.Context, s cell.Scope, epoch uint64) (BacklogStats, error) {
	out := BacklogStats{Scope: s.String(), Epoch: epoch}
	man, haveMan, err := m.ReadManifest(ctx, s, epoch)
	if err != nil {
		return out, err
	}
	floor := uint64(0)
	if haveMan {
		floor = man.MaxTxID
		out.LastCompactionKnown = man.CreatedMs > 0
		if out.LastCompactionKnown {
			out.LastCompactionLagSeconds = time.Now().UnixMilli() - man.CreatedMs
			if out.LastCompactionLagSeconds < 0 {
				out.LastCompactionLagSeconds = 0
			}
		}
	}
	keep := map[string]bool{}
	for _, k := range man.Objects {
		keep[k] = true
	}
	infos, sizesKnown, truncated, err := m.listObjectInfos(ctx, Prefix(s, epoch))
	if err != nil {
		return out, err
	}
	out.Truncated = truncated
	now := time.Now().UnixMilli()
	var oldest int64
	l1Gen := map[uint64]bool{}
	l1 := L1Prefix(s, epoch)
	for _, info := range infos {
		name := strings.TrimPrefix(info.Key, Prefix(s, epoch))
		if strings.HasPrefix(info.Key, l1) {
			short := strings.TrimPrefix(info.Key, l1)
			if strings.HasSuffix(short, ".snapshot") {
				base := short[:strings.IndexByte(short, '.')]
				if gen, perr := strconv.ParseUint(base, 10, 64); perr == nil {
					l1Gen[gen] = true
				}
			}
			if !keep[info.Key] {
				if sizesKnown {
					out.DeferredGCBytes += info.Size
				}
			}
			continue
		}
		if !isSegmentKey(info.Key) {
			continue
		}
		sp, known := parseSegmentSpan(name)
		if known {
			if sp.marker {
				continue
			}
			if floor > 0 && sp.end <= floor {
				if sizesKnown {
					out.DeferredGCBytes += info.Size
				}
				continue
			}
		}
		out.L0BacklogObjects++
		if sizesKnown {
			out.L0BacklogBytes += info.Size
			out.L0BacklogBytesKnown = true
		}
		if info.LastModifiedMs > 0 {
			age := (now - info.LastModifiedMs) / 1000
			if age < 0 {
				age = 0
			}
			if !out.OldestL0AgeKnown || age > out.OldestL0AgeSeconds {
				out.OldestL0AgeSeconds = age
				out.OldestL0AgeKnown = true
			}
			if oldest == 0 || info.LastModifiedMs < oldest {
				oldest = info.LastModifiedMs
			}
		}
	}
	out.L1Generations = len(l1Gen)
	if sizesKnown {
		out.DeferredBytesKnown = true
	}
	if oldest > 0 {
		out.OldestL0AgeKnown = true
	}
	epochs, err := m.Epochs(ctx, s, 0)
	if err != nil {
		return out, err
	}
	for _, e := range epochs {
		retired, rerr := m.EpochRetired(ctx, s, e)
		if rerr != nil {
			return out, rerr
		}
		if retired {
			out.RetiredEpochs++
		} else {
			out.RetainedEpochs++
		}
	}
	return out, nil
}

func (m *Manager) listObjectInfos(ctx context.Context, prefix string) ([]bucket.ObjectInfo, bool, bool, error) {
	if pl, ok := m.B.(bucket.PagedLister); ok {
		var out []bucket.ObjectInfo
		after := ""
		for {
			page, next, err := pl.ListPage(ctx, prefix, after, 1000)
			if err != nil {
				return nil, false, false, err
			}
			for _, info := range page {
				if len(out) >= maxStatsObjects {
					return out, true, true, nil
				}
				out = append(out, info)
			}
			if next == "" || next == after {
				return out, true, false, nil
			}
			after = next
		}
	}
	keys, err := m.B.List(ctx, prefix)
	if err != nil {
		return nil, false, false, err
	}
	sort.Strings(keys)
	truncated := len(keys) > maxStatsObjects
	if truncated {
		keys = keys[:maxStatsObjects]
	}
	out := make([]bucket.ObjectInfo, 0, len(keys))
	for _, key := range keys {
		out = append(out, bucket.ObjectInfo{Key: key, Size: -1})
	}
	return out, false, truncated, nil
}
