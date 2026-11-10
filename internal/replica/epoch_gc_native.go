package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type listedEpoch struct {
	epoch    uint64
	newestMS int64
}
type epochGCPlan struct {
	below  uint64
	record bool
	delete []uint64
}

// planEpochGC follows celld crates/logic/epoch_gc.rs. The chain must come from
// the restore planner, never a second "newest snapshot" heuristic.
func planEpochGC(own uint64, chain []uint64, listed []listedEpoch, recorded *uint64, pinned bool, nowMS, graceMS int64) *epochGCPlan {
	if pinned || len(chain) == 0 || chain[len(chain)-1] != own {
		return nil
	}
	base := chain[0]
	if recorded != nil && *recorded > base {
		return nil
	}
	p := &epochGCPlan{below: base, record: recorded == nil || *recorded != base}
	for _, e := range listed {
		// Subtraction avoids overflow at MaxUint64.
		if e.epoch < base && own > 0 && e.epoch < own-1 && max(int64(0), nowMS-e.newestMS) >= graceMS {
			p.delete = append(p.delete, e.epoch)
		}
	}
	return p
}

type retiredMark struct {
	Below uint64 `json:"retired_below"`
}

func retiredMarkKey(s cell.Scope) string { return s.Key() + "/ltx/retired.json" }
func (m *Manager) readRetiredMark(ctx context.Context, s cell.Scope) (*uint64, error) {
	raw, _, err := m.resourceObjects().GetVersion(ctx, retiredMarkKey(s))
	if errors.Is(err, bucket.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var mark retiredMark
	if err = json.Unmarshal(raw, &mark); err != nil || mark.Below == 0 {
		return nil, fmt.Errorf("replica: invalid retired mark: %q", raw)
	}
	return &mark.Below, nil
}

// retireNativeEpochs uses only celld's stream-wide retired.json. No per-epoch
// seal, tombstone, reader-pin or retirement objects are read or written.
func (m *Manager) retireNativeEpochs(ctx context.Context, s cell.Scope, current uint64, keepPrevious int, opts RetireOptions) (int, error) {
	if !opts.OfflineQuiescent {
		if !opts.Safe || opts.Verify == nil || opts.Ready == nil {
			return 0, ErrGCDeferred
		}
		ready, err := opts.Ready(ctx)
		if err != nil || !ready {
			return 0, err
		}
	}
	files, err := m.epochChain(ctx, s, 0)
	if err != nil {
		return 0, err
	}
	if _, err := planEpochFiles(files); err != nil {
		return 0, err
	}
	seen := map[uint64]bool{}
	for _, f := range files {
		seen[f.epoch] = true
	}
	chain := make([]uint64, 0, len(seen))
	for e := range seen {
		chain = append(chain, e)
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i] < chain[j] })
	if len(chain) == 0 || chain[len(chain)-1] != current {
		return 0, nil
	}
	epochs, err := m.epochDirs(ctx, s, 0)
	if err != nil {
		return 0, err
	}
	// Match scan_epochs' newest-epoch guard even when an unlinked late epoch
	// is excluded by the continuation chain.
	if len(epochs) == 0 || epochs[len(epochs)-1] != current {
		return 0, nil
	}
	mark, err := m.readRetiredMark(ctx, s)
	if err != nil {
		return 0, err
	}
	var listed []listedEpoch
	victims := map[uint64][]string{}
	scanned := 0
	for _, e := range epochs {
		if e >= chain[0] {
			continue
		}
		if scanned == 64 {
			break
		}
		scanned++
		infos, known, truncated, err := m.listObjectInfos(ctx, EpochPrefix(s, e))
		if err != nil {
			return 0, err
		}
		if !known || truncated {
			continue
		} // no complete upload-age evidence
		var newest int64
		valid := true
		for _, info := range infos {
			rel := info.Key[len(EpochPrefix(s, e)):]
			if len(rel) != 42 || rel[4] != '/' || !validLevelName(rel[:4]) || info.LastModifiedMs <= 0 {
				valid = false
				break
			}
			sp, ok := parseSegmentSpan(path.Base(info.Key))
			if !ok || sp.start == 0 || sp.end < sp.start {
				valid = false
				break
			}
			newest = max(newest, info.LastModifiedMs)
			victims[e] = append(victims[e], info.Key)
		}
		if valid && len(infos) > 0 {
			listed = append(listed, listedEpoch{e, newest})
		} else {
			delete(victims, e)
		}
	}
	grace := opts.Grace
	if grace == 0 {
		grace = time.Hour
	}
	if grace < 0 {
		grace = 0
	}
	plan := planEpochGC(current, chain, listed, mark, false, opts.now().UnixMilli(), grace.Milliseconds())
	if plan == nil {
		return 0, nil
	}
	// The ownership read MUST follow the chain scan and precede the mark/delete.
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return 0, err
		}
	}
	if !opts.OfflineQuiescent {
		ready, err := opts.Ready(ctx)
		if err != nil || !ready {
			return 0, err
		}
	}
	if plan.record {
		raw, _ := json.Marshal(retiredMark{Below: plan.below})
		if _, err := m.resourceObjects().Put(ctx, retiredMarkKey(s), raw); err != nil {
			return 0, err
		}
		m.rememberRetiredMark(s, &plan.below, time.Now())
	}
	deleted := 0
	for _, e := range plan.delete {
		if keepPrevious > 1 && current-e <= uint64(keepPrevious) {
			continue
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return deleted, err
			}
		}
		for _, key := range victims[e] {
			if err := m.resourceObjects().Delete(ctx, key); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}
func validLevelName(s string) bool {
	return s == "0000" || s == "0001" || s == "0002" || s == "0003" || s == "0009"
}

// celld caches present retirement marks for five minutes and absence for thirty
// seconds. This cache is used only by recovery/coverage, never by write admission.
type retiredCacheEntry struct {
	below uint64
	at    time.Time
}

func (m *Manager) cachedRetiredMark(ctx context.Context, s cell.Scope) (*uint64, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	m.retiredMu.Lock()
	e, ok := m.retiredCache[s.String()]
	ttl := 5 * time.Minute
	if e.below == 0 {
		ttl = 30 * time.Second
	}
	if ok && time.Since(e.at) < ttl {
		m.retiredMu.Unlock()
		if e.below == 0 {
			return nil, nil
		}
		return &e.below, nil
	}
	m.retiredMu.Unlock()
	mark, err := m.readRetiredMark(ctx, s)
	if err != nil {
		return nil, err
	}
	return m.rememberRetiredMark(s, mark, time.Now()), nil
}
func (m *Manager) rememberRetiredMark(s cell.Scope, mark *uint64, at time.Time) *uint64 {
	m.retiredMu.Lock()
	defer m.retiredMu.Unlock()
	if m.retiredCache == nil {
		m.retiredCache = map[string]retiredCacheEntry{}
	}
	key := s.String()
	e, exists := m.retiredCache[key]
	if !exists && len(m.retiredCache) >= 4096 {
		for k := range m.retiredCache {
			delete(m.retiredCache, k)
			break
		}
	}
	if mark != nil {
		e.below = max(e.below, *mark)
	}
	e.at = at
	m.retiredCache[key] = e
	if e.below == 0 {
		return nil
	}
	v := e.below
	return &v
}

// EpochRetired reports whether the epoch payload was retired.
func (m *Manager) EpochRetired(ctx context.Context, s cell.Scope, epoch uint64) (bool, error) {
	mark, err := m.cachedRetiredMark(ctx, s)
	return mark != nil && epoch < *mark, err
}
