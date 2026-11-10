package replica

import (
	"context"
	"fmt"
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
	infos, sizesKnown, truncated, err := m.listObjectInfos(ctx, EpochPrefix(s, epoch))
	if err != nil {
		return out, err
	}
	out.Truncated = truncated
	out.L0BacklogBytesKnown = sizesKnown
	out.DeferredBytesKnown = sizesKnown
	type file struct {
		info   bucket.ObjectInfo
		level  uint16
		lo, hi uint64
	}
	var files []file
	var higher [10][]txRange
	var latest int64
	for _, info := range infos {
		rel := strings.TrimPrefix(info.Key, EpochPrefix(s, epoch))
		if len(rel) != 42 || rel[4] != '/' {
			continue
		}
		level, err := strconv.ParseUint(rel[:4], 16, 16)
		if err != nil || !validLevel(uint16(level)) || fmt.Sprintf("%04x", level) != rel[:4] {
			continue
		}
		sp, ok := parseSegmentSpan(rel[5:])
		if !ok || sp.start == 0 || sp.end < sp.start {
			continue
		}
		files = append(files, file{info, uint16(level), sp.start, sp.end})
		if level == 1 {
			out.L1Generations++
		}
		if level > 0 && info.LastModifiedMs > latest {
			latest = info.LastModifiedMs
		}
		for lower := uint16(0); lower < uint16(level); lower++ {
			higher[lower] = append(higher[lower], txRange{sp.start, sp.end})
		}
	}
	for i := range higher {
		higher[i] = mergeTXRanges(higher[i])
	}
	now := time.Now().UnixMilli()
	if latest > 0 {
		out.LastCompactionKnown = true
		out.LastCompactionLagSeconds = max(0, (now-latest)/1000)
	}
	for _, f := range files {
		covered := rangeCovered(higher[f.level], f.lo, f.hi)
		if covered {
			if sizesKnown {
				out.DeferredGCBytes += f.info.Size
			}
			continue
		}
		if f.level != 0 {
			continue
		}
		out.L0BacklogObjects++
		if sizesKnown {
			out.L0BacklogBytes += f.info.Size
		}
		if f.info.LastModifiedMs > 0 {
			out.OldestL0AgeKnown = true
			out.OldestL0AgeSeconds = max(out.OldestL0AgeSeconds, max(0, (now-f.info.LastModifiedMs)/1000))
		}
	}
	// Report physically present native epochs. Do not read private retired,
	// manifest or seal objects to infer whether an epoch is retained.
	epochs, err := m.epochDirs(ctx, s, 0)
	if err != nil {
		return out, err
	}
	out.RetainedEpochs = len(epochs)
	return out, nil
}

func (m *Manager) listObjectInfos(ctx context.Context, prefix string) ([]bucket.ObjectInfo, bool, bool, error) {
	if _, ok := m.B.(bucket.PagedLister); ok {
		var out []bucket.ObjectInfo
		after := ""
		for {
			page, next, err := m.resourceObjects().ListPage(ctx, prefix, after, 1000)
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
	keys, err := m.resourceObjects().ListPrefix(ctx, prefix)
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

// txRange coverage is a union, not a maximum watermark: gaps must remain backlog.
type txRange struct{ lo, hi uint64 }

func mergeTXRanges(in []txRange) []txRange {
	sort.Slice(in, func(i, j int) bool { return in[i].lo < in[j].lo })
	out := in[:0]
	for _, r := range in {
		if len(out) > 0 {
			p := &out[len(out)-1]
			if r.lo <= p.hi || p.hi != ^uint64(0) && r.lo == p.hi+1 {
				p.hi = max(p.hi, r.hi)
				continue
			}
		}
		out = append(out, r)
	}
	return out
}
func rangeCovered(in []txRange, lo, hi uint64) bool {
	i := sort.Search(len(in), func(i int) bool { return in[i].hi >= lo })
	return i < len(in) && in[i].lo <= lo && in[i].hi >= hi
}
