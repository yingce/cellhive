package replica

import (
	"context"
	"fmt"
	"path"
	"sort"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// epochFile is derived solely from the native object path. No manifest or
// custom marker is needed to identify snapshots, levels, or continuation cuts.
type epochFile struct {
	key      string
	epoch    uint64
	level    int
	min, max uint64
}

// epochChain follows celld's client/epochs.rs: newest epoch first, clip older
// epochs strictly below the successor's first TXID, and accept only a
// predecessor ending exactly at that cut. A late intermediate epoch is skipped.
func (m *Manager) epochChain(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]epochFile, error) {
	epochs, err := m.epochDirs(ctx, s, maxEpoch)
	if err != nil {
		return nil, err
	}
	fence, _, err := m.readResourceFence(ctx, s)
	if err != nil {
		return nil, err
	}
	if fence.Deleted {
		return nil, ErrResourceDeleted
	}
	var files []epochFile
	var above uint64
	for i := len(epochs) - 1; i >= 0; i-- {
		epoch := epochs[i]
		if fence.JobID != "" && epoch <= fence.Epoch {
			continue
		}
		// The successor fixes a historical cut. Higher levels may prove that
		// entire prefix without listing covered lower-level inventory. For
		// the newest epoch there is no upper bound: keep crossing-range discovery.
		upper := uint64(0)
		if above != 0 {
			upper = above - 1
		}
		levels, err := m.listCanonicalLevelsBounded(ctx, s, epoch, upper)
		if err != nil {
			return nil, err
		}
		var current []epochFile
		lo, hi := ^uint64(0), uint64(0)
		for _, level := range celldLevels {
			for _, key := range levels[level] {
				sp, ok := parseSegmentSpan(path.Base(key))
				if !ok || sp.start == 0 || sp.end < sp.start {
					return nil, fmt.Errorf("replica: invalid native LTX path %s", key)
				}
				if above != 0 && sp.end >= above {
					continue
				}
				current = append(current, epochFile{key: key, epoch: epoch, level: int(level), min: sp.start, max: sp.end})
				if sp.start < lo {
					lo = sp.start
				}
				if sp.end > hi {
					hi = sp.end
				}
			}
		}
		if len(current) == 0 || (above != 0 && hi != above-1) {
			continue
		}
		files = append(files, current...)
		if lo == 1 {
			return files, nil
		}
		above = lo
	}
	if len(files) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("replica: continuation at txid %d has no complete predecessor", above)
}

// planEpochFiles ports celld's longest contiguous coverage selection across
// levels. L9 provides the newest snapshot; 3/2/1/0 extend it, preferring the
// longest reach, then the lowest minimum and highest compaction level.
func planEpochFiles(files []epochFile) ([]epochFile, error) {
	var plan []epochFile
	var current uint64
	for _, f := range files {
		if f.level == 9 && f.min != 1 {
			return nil, fmt.Errorf("replica: L9 is not a snapshot: %s", f.key)
		}
		if f.level == 9 && f.max > current {
			plan = []epochFile{f}
			current = f.max
		}
	}
	candidates := make([]epochFile, 0, len(files))
	for _, f := range files {
		if f.level != 9 {
			candidates = append(candidates, f)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.min != b.min {
			return a.min < b.min
		}
		if a.max != b.max {
			return a.max > b.max
		}
		return a.level > b.level
	})
	pos := 0
	for current != ^uint64(0) {
		var best epochFile
		found := false
		for pos < len(candidates) && candidates[pos].min <= current+1 {
			f := candidates[pos]
			pos++
			if f.max <= current {
				continue
			}
			if !found || f.max > best.max || (f.max == best.max && (f.min < best.min || (f.min == best.min && f.level > best.level))) {
				best = f
				found = true
			}
		}
		if !found {
			break
		}
		plan = append(plan, best)
		current = best.max
	}
	if pos < len(candidates) {
		return nil, fmt.Errorf("replica: non-contiguous LTX chain: have %d next %d", current, candidates[pos].min)
	}
	if len(files) > 0 && len(plan) == 0 {
		return nil, fmt.Errorf("replica: no snapshot in chain")
	}
	return plan, nil
}

func (m *Manager) nativeRestorePlan(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]epochFile, error) {
	files, err := m.epochChain(ctx, s, maxEpoch)
	if err != nil {
		return nil, err
	}
	return planEpochFiles(files)
}

// readNativePlan fetches only the chosen files and validates their actual
// headers against the listed ranges before exposing any recovered image.
func (m *Manager) readNativePlan(ctx context.Context, plan []epochFile) ([]Segment, error) {
	if len(plan) == 0 {
		return nil, nil
	}
	parallel := m.RestoreParallel
	if parallel <= 0 {
		parallel = 64
	}
	if parallel > len(plan) {
		parallel = len(plan)
	}
	out := make([]Segment, len(plan))
	errs := make([]error, len(plan))
	jobs := make(chan int)
	var wg sync.WaitGroup
	objects := m.resourceObjects()
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				f := plan[i]
				raw, _, err := objects.GetVersion(ctx, f.key)
				if err != nil {
					errs[i] = err
					continue
				}
				h, err := ltx.ValidateNative(raw)
				if err != nil {
					errs[i] = fmt.Errorf("replica: decode %s: %w", f.key, err)
					continue
				}
				if h.MinTXID != f.min || h.MaxTXID != f.max {
					errs[i] = fmt.Errorf("replica: header range differs from path %s", f.key)
					continue
				}
				kind := ltx.KindDelta
				if h.IsSnapshot() {
					kind = ltx.KindSnapshot
				}
				out[i] = Segment{Key: f.key, Header: ltx.Header{Kind: kind, Epoch: f.epoch, StartTxID: h.MinTXID, EndTxID: h.MaxTXID}, Raw: raw}
			}
		}()
	}
	for i := range plan {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RestoreChainKeys exposes the native selected chain for credential-free
// readers. It includes required predecessors, not merely the current epoch.
func (m *Manager) RestoreChainKeys(ctx context.Context, s cell.Scope, epoch uint64) ([]string, error) {
	plan, err := m.nativeRestorePlan(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(plan))
	for i, f := range plan {
		keys[i] = f.key
	}
	return keys, nil
}
