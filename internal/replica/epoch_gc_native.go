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
	// scannedKeys holds the exact object identity proven by the age scan. The
	// deletion pass must use this set; re-listing the prefix and clearing it
	// would also delete objects written after the scan, whose age was never
	// checked (and unknown-type keys).
	scannedKeys := map[uint64][]string{}
	eligible := map[uint64]bool{}
	scanned := 0
	for _, e := range epochs {
		if e >= chain[0] {
			continue
		}
		if scanned == 64 {
			break
		}
		scanned++
		newest, n, nativeOnly, keys, err := m.scanEpochGCAge(ctx, EpochPrefix(s, e))
		if err != nil {
			return 0, err
		}
		// Mixed/unknown keys or an object with no trustworthy upload time:
		// keep the whole epoch (fail closed on delete), same as the prior
		// truncated/invalid skip, without aborting the pass.
		if n == 0 || !nativeOnly {
			continue
		}
		listed = append(listed, listedEpoch{e, newest})
		eligible[e] = true
		scannedKeys[e] = keys
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
		if !eligible[e] {
			continue
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return deleted, err
			}
		}
		n, err := m.deleteEpochGCKeys(ctx, scannedKeys[e])
		if err != nil {
			return deleted, err
		}
		deleted += n
		m.LoseL0(s, e)
	}
	return deleted, nil
}

// scanEpochGCAge pages the whole epoch prefix for upload-age evidence. Unlike
// listObjectInfos, a large prefix is never treated as "skip this epoch" just
// because it has many objects. nativeOnly is false when any non-LTX key is
// present, or when any object reports no trustworthy upload time, so the caller
// can refuse deletion of that epoch. keys lists the exact objects that were
// checked; deletion must use them instead of re-listing the prefix.
func (m *Manager) scanEpochGCAge(ctx context.Context, prefix string) (newestMS int64, objects int, nativeOnly bool, keys []string, err error) {
	nativeOnly = true
	err = m.forEachEpochGCObject(ctx, prefix, func(info bucket.ObjectInfo) error {
		objects++
		rel := info.Key[len(prefix):]
		if len(rel) != 42 || rel[4] != '/' || !validLevelName(rel[:4]) {
			nativeOnly = false
			return nil
		}
		sp, ok := parseSegmentSpan(path.Base(info.Key))
		if !ok || sp.start == 0 || sp.end < sp.start {
			nativeOnly = false
			return nil
		}
		// A zero time is not evidence of age: treating it as infinitely old
		// would delete an epoch whose objects were never aged. Backends that
		// page without LastModified therefore keep the whole epoch.
		if info.LastModifiedMs <= 0 {
			nativeOnly = false
			return nil
		}
		newestMS = max(newestMS, info.LastModifiedMs)
		keys = append(keys, info.Key)
		return nil
	})
	if err != nil {
		return 0, 0, false, nil, err
	}
	if newestMS <= 0 {
		nativeOnly = false
	}
	return newestMS, objects, nativeOnly, keys, nil
}

// deleteEpochGCKeys removes only the objects the age scan approved. It never
// re-lists the prefix, so keys written after the scan (or keys whose age was
// never proven) survive.
func (m *Manager) deleteEpochGCKeys(ctx context.Context, keys []string) (int, error) {
	deleted := 0
	for _, key := range keys {
		if err := m.resourceObjects().Delete(ctx, key); err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (m *Manager) forEachEpochGCObject(ctx context.Context, prefix string, fn func(bucket.ObjectInfo) error) error {
	if _, ok := m.B.(bucket.PagedLister); ok {
		after := ""
		for {
			page, next, err := m.resourceObjects().ListPage(ctx, prefix, after, 1000)
			if err != nil {
				return err
			}
			for _, info := range page {
				if err := fn(info); err != nil {
					return err
				}
			}
			if next == "" || next == after {
				return nil
			}
			after = next
		}
	}
	keys, err := m.resourceObjects().ListPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := fn(bucket.ObjectInfo{Key: key, Size: -1}); err != nil {
			return err
		}
	}
	return nil
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
