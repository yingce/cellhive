// Package compaction folds a cell's replication chain (the current L1 snapshot,
// if any, plus newer L0 deltas) into a single compacted L1 snapshot and a
// manifest. A takeover restore then reads only the manifest's objects plus newer
// L0 deltas, instead of the whole L0 chain (docs/cell-protocol.md §6).
//
// Compaction is a cold-path operation: it Lists object keys. It must never run
// on a write-ack hot path.
package compaction

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// Options controls when and how a compaction runs.
type Options struct {
	// MinSegments and MinBytes gate the fold: compaction runs only when the
	// uncovered L0 tail has at least MinSegments segments, or its raw size is at least
	// MinBytes. The thresholds are an OR so either a long or a large chain
	// triggers a fold.
	MinSegments int
	MinBytes    int64
	// MinObjects is an OR threshold on L0 objects with an uncovered tail:
	// cold-restore RTT is driven by object count, and a .batch may hold many
	// segments. 0 disables this threshold.
	MinObjects int
	// MaxPartBytes bounds one compacted snapshot part (0 uses the ltx default).
	MaxPartBytes int
	// Verify rechecks ownership before publication and each offline Delete.
	// It is not atomic with bucket operations; it cannot establish quiescence.
	Verify func(context.Context) error
	// GC requests cleanup of superseded old L1/covered L0 objects. Without
	// OfflineQuiescent it reports deferral and never authorizes deletion.
	GC bool
	// OfflineQuiescent is an external assertion that ALL writers and readers
	// remain stopped/drained throughout deletion. False defers physical GC even
	// when GC is true. Production background work must leave this false.
	OfflineQuiescent bool
}

// Result reports what a compaction did.
type Result struct {
	Compacted  bool
	Skipped    string
	Inputs     int
	InputBytes int64
	NewL0      int
	Parts      int
	MinTxID    uint64
	MaxTxID    uint64
	Commit     uint32
	DeletedL1  int
	DeletedL0  int
	// DeferredGC counts candidate payloads retained by a safety gate. GCSkipped
	// explains why; counts are not deleted objects or reclaimed bytes.
	DeferredGC int
	GCSkipped  string
	// Epoch and object accounting are metadata-only observations. They are
	// populated for deferred GC; bytes are unknown when the bucket cannot return
	// sizes from its listing API.
	Epoch              uint64
	ObjectCount        int
	ObjectBytes        int64
	ObjectBytesKnown   bool
	RetainedBytes      int64
	RetainedBytesKnown bool
	CoveredBytes       int64
	DeferredBytes      int64
	Covered            int
	Deferred           int
	Unknown            int
	MetadataTruncated  bool
}

const accountingObjectLimit = 10000

func deferredMetadataAccounting(ctx context.Context, m *replica.Manager, s cell.Scope, epoch, floor uint64) Result {
	r := Result{Epoch: epoch, ObjectBytesKnown: true}
	infos, known, truncated := listObjectMetadata(ctx, m.B, replica.Prefix(s, epoch))
	r.ObjectBytesKnown = known
	r.MetadataTruncated = truncated
	for _, info := range infos {
		r.ObjectCount++
		if info.Size >= 0 {
			r.ObjectBytes += info.Size
		}
		name := info.Key
		if i := strings.LastIndexByte(name, '/'); i >= 0 {
			name = name[i+1:]
		}
		end, _, recognized := strictCoveredNameEnd(name)
		if recognized {
			if end <= floor {
				r.Covered++
				if info.Size >= 0 {
					r.CoveredBytes += info.Size
				}
			} else {
				r.Deferred++
				if info.Size >= 0 {
					r.DeferredBytes += info.Size
				}
			}
		} else {
			r.Unknown++
		}
	}
	if truncated {
		r.Unknown++
	}
	r.RetainedBytes = r.CoveredBytes + r.DeferredBytes
	r.RetainedBytesKnown = r.ObjectBytesKnown
	return r
}

func addDeferredMetadataAccounting(ctx context.Context, m *replica.Manager, s cell.Scope, epoch, floor uint64, r *Result) {
	if r == nil || r.GCSkipped == "" {
		return
	}
	a := deferredMetadataAccounting(ctx, m, s, epoch, floor)
	r.Epoch = a.Epoch
	r.ObjectCount = a.ObjectCount
	r.ObjectBytes = a.ObjectBytes
	r.ObjectBytesKnown = a.ObjectBytesKnown
	r.RetainedBytes = a.RetainedBytes
	r.RetainedBytesKnown = a.RetainedBytesKnown
	r.CoveredBytes = a.CoveredBytes
	r.DeferredBytes = a.DeferredBytes
	r.Covered = a.Covered
	r.Deferred = a.Deferred
	r.Unknown = a.Unknown
	r.MetadataTruncated = a.MetadataTruncated
}

// listObjectMetadata uses only listing responses. It is deliberately bounded:
// observability must not turn a damaged or unbounded prefix into a large read.
func listObjectMetadata(ctx context.Context, b bucket.Bucket, prefix string) ([]bucket.ObjectInfo, bool, bool) {
	if pl, ok := b.(bucket.PagedLister); ok {
		out := make([]bucket.ObjectInfo, 0, 256)
		after := ""
		for len(out) < accountingObjectLimit {
			infos, next, err := pl.ListPage(ctx, prefix, after, 1000)
			if err != nil {
				return out, false, false
			}
			remaining := accountingObjectLimit - len(out)
			if len(infos) > remaining {
				out = append(out, infos[:remaining]...)
				return out, true, true
			}
			out = append(out, infos...)
			if next == "" {
				return out, true, false
			}
			if next == after {
				return out, false, false
			}
			after = next
		}
		return out, true, true
	}
	keys, err := b.List(ctx, prefix)
	if err != nil {
		return nil, false, false
	}
	truncated := len(keys) > accountingObjectLimit
	if truncated {
		keys = keys[:accountingObjectLimit]
	}
	infos := make([]bucket.ObjectInfo, len(keys))
	for i, key := range keys {
		infos[i] = bucket.ObjectInfo{Key: key, Size: -1}
	}
	return infos, false, truncated
}

// Compact folds the chain for scope/epoch into L1 and writes a manifest. When the
// chain has no snapshot baseline it cannot be folded into a full-page snapshot,
// so it is skipped rather than failed.
func Compact(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
	man, haveMan, err := m.ReadManifest(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}

	var segs [][]byte
	var inputBytes int64
	var minTx, maxTx uint64
	haveSnap := false
	var oldL1 []string
	seen := map[string]bool{}

	floor := uint64(0)
	if haveMan {
		floor = man.MaxTxID
	}

	l0, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}

	// Cheap preflight: a strict canonical name proves the whole object is at or
	// below the manifest watermark (or wholly above it) without downloading it.
	// Unknown, mixed, and cross-floor names stay on the decode path.
	var pre l0Preflight
	if haveMan {
		pre = preflightL0(ctx, m, s, epoch, l0, floor)
		if pre.complete {
			if len(pre.newKeys) == 0 {
				res := Result{Skipped: "already compacted"}
				var gerr error
				res.DeletedL0, res.DeferredGC, res.GCSkipped, gerr = gcCoveredL0(ctx, m, s, epoch, floor, opts, pre.covered)
				if !opts.OfflineQuiescent {
					addDeferredMetadataAccounting(ctx, m, s, epoch, floor, &res)
				}
				return res, gerr
			}
			if pre.belowThreshold(opts) {
				res := Result{Skipped: "below threshold", Inputs: pre.newSegments, InputBytes: pre.newBytes}
				var gerr error
				res.DeletedL0, res.DeferredGC, res.GCSkipped, gerr = gcCoveredL0(ctx, m, s, epoch, floor, opts, pre.covered)
				if !opts.OfflineQuiescent {
					addDeferredMetadataAccounting(ctx, m, s, epoch, floor, &res)
				}
				return res, gerr
			}
		}
	}

	newL0 := 0
	newObjects := 0
	for _, k := range l0 {
		objectHasNew := false
		raw, err := m.ReadSegment(ctx, k)
		if err != nil {
			return Result{}, err
		}
		parts, err := ltx.Split(raw)
		if err != nil {
			return Result{}, fmt.Errorf("compaction: corrupt L0 object %s: %w", k, err)
		}
		for _, part := range parts {
			h, _, derr := ltx.Decode(part)
			if derr != nil {
				return Result{}, derr
			}
			if haveMan && h.EndTxID <= floor {
				continue // already folded into the L1 baseline
			}
			objectHasNew = true // Count physical objects even when their tail overlaps.
			if seen[h.ID()] {
				continue // duplicate from mixed batch/single storage
			}
			seen[h.ID()] = true
			newL0++
			if h.Kind == ltx.KindSnapshot {
				haveSnap = true
			}
			if minTx == 0 || h.StartTxID < minTx {
				minTx = h.StartTxID
			}
			if h.EndTxID > maxTx {
				maxTx = h.EndTxID
			}
			segs = append(segs, part)
			inputBytes += int64(len(part))
		}
		if objectHasNew {
			newObjects++
		}
	}

	if !haveSnap && !haveMan {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	if newL0 == 0 && haveMan {
		res := Result{Skipped: "already compacted"}
		var gerr error
		res.DeletedL0, res.DeferredGC, res.GCSkipped, gerr = gcCoveredL0(ctx, m, s, epoch, floor, opts, pre.covered)
		if !opts.OfflineQuiescent {
			addDeferredMetadataAccounting(ctx, m, s, epoch, floor, &res)
		}
		return res, gerr
	}
	belowObjects := opts.MinObjects <= 0 || newObjects < opts.MinObjects
	if len(segs) < opts.MinSegments && inputBytes < opts.MinBytes && belowObjects {
		res := Result{Skipped: "below threshold", Inputs: len(segs), InputBytes: inputBytes}
		var gerr error
		res.DeletedL0, res.DeferredGC, res.GCSkipped, gerr = gcCoveredL0(ctx, m, s, epoch, floor, opts, pre.covered)
		if !opts.OfflineQuiescent {
			addDeferredMetadataAccounting(ctx, m, s, epoch, floor, &res)
		}
		return res, gerr
	}
	// Download the baseline only after the new tail passes the threshold.
	if haveMan {
		oldL1 = append([]string(nil), man.Objects...)
		for _, k := range man.Objects {
			raw, err := m.ReadSegment(ctx, k)
			if err != nil {
				return Result{}, err
			}
			parts, err := ltx.Split(raw)
			if err != nil {
				return Result{}, fmt.Errorf("compaction: corrupt L1 object %s: %w", k, err)
			}
			for _, part := range parts {
				h, _, derr := ltx.Decode(part)
				if derr != nil {
					return Result{}, derr
				}
				if seen[h.ID()] {
					continue
				}
				seen[h.ID()] = true
				if h.Kind == ltx.KindSnapshot {
					haveSnap = true
				}
				if minTx == 0 || h.StartTxID < minTx {
					minTx = h.StartTxID
				}
				if h.EndTxID > maxTx {
					maxTx = h.EndTxID
				}
				segs = append(segs, part)
				inputBytes += int64(len(part))
			}
		}
	}

	if !haveSnap {
		return Result{Skipped: "no snapshot baseline"}, nil
	}

	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}

	parts, err := restore.Compact(segs, opts.MaxPartBytes)
	if err != nil {
		// Older detached baselines can straddle a delta batch. Recover from
		// a retained predecessor, with the same strict chain validation; do
		// not discard the overlapping delta's newer tail.
		chain, rerr := m.RestoreChain(ctx, s, epoch)
		if rerr != nil {
			return Result{}, fmt.Errorf("compaction: current chain: %v; predecessor recovery: %w", err, rerr)
		}
		segs = nil
		inputBytes = 0
		for _, seg := range chain {
			segs = append(segs, seg.Raw)
			inputBytes += int64(len(seg.Raw))
		}
		parts, err = restore.Compact(segs, opts.MaxPartBytes)
		if err != nil {
			return Result{}, err
		}
		for i, part := range parts {
			h, payload, err := ltx.Decode(part)
			if err != nil {
				return Result{}, err
			}
			h.Epoch = epoch
			parts[i] = ltx.Encode(h, payload)
			maxTx = h.EndTxID
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return Result{}, err
			}
		}
	}
	_, payload, err := ltx.Decode(parts[0])
	if err != nil {
		return Result{}, err
	}
	pageSize, commit, _, _, err := ltx.DecodeWALPageMap(payload)
	if err != nil {
		return Result{}, err
	}

	man2 := replica.Manifest{MinTxID: minTx, MaxTxID: maxTx, Commit: commit, PageSize: pageSize}
	for _, part := range parts {
		key, _, err := m.AppendL1(ctx, s, epoch, part)
		if err != nil {
			return Result{}, err
		}
		man2.Objects = append(man2.Objects, key)
	}
	// The page index lets a cold start fetch single pages with ranged reads
	// instead of downloading the whole L1 snapshot.
	idx := replica.PageIndex{Watermark: maxTx}
	for i, part := range parts {
		_, payload, err := ltx.Decode(part)
		if err != nil {
			return Result{}, err
		}
		ps, c, _, _, err := ltx.DecodeWALPageMap(payload)
		if err != nil {
			return Result{}, err
		}
		nums, err := ltx.PageMapNumbers(payload)
		if err != nil {
			return Result{}, err
		}
		locs, err := ltx.PageLocs(payload)
		if err != nil {
			return Result{}, err
		}
		idx.Entries = append(idx.Entries, replica.PageIndexEntry{
			Key: man2.Objects[i], PageSize: ps, Commit: c, Pages: nums, Locs: locs,
		})
	}
	if err := m.PutIndex(ctx, s, epoch, idx); err != nil {
		return Result{}, err
	}
	// Re-check ownership right before publishing the new manifest: the initial
	// Verify may have passed while a takeover was in progress, and a stale owner
	// must not replace the manifest/GC a chain a new owner is restoring.
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	if err := m.PutManifest(ctx, s, epoch, man2); err != nil {
		return Result{}, err
	}
	res := Result{
		Compacted: true, Inputs: len(segs), InputBytes: inputBytes, NewL0: newL0,
		Parts: len(parts), MinTxID: minTx, MaxTxID: maxTx, Commit: commit,
	}
	if opts.GC {
		if !opts.OfflineQuiescent {
			res.DeferredGC = len(oldL1) + len(preflightL0(ctx, m, s, epoch, l0, maxTx).covered)
			res.GCSkipped = replica.ErrGCDeferred.Error()
			addDeferredMetadataAccounting(ctx, m, s, epoch, maxTx, &res)
			return res, nil
		}
		complete, err := m.HasCompleteSnapshot(ctx, s, epoch)
		if err != nil {
			return res, err
		}
		if !complete {
			res.GCSkipped = "complete snapshot proof missing"
			return res, nil
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return res, err
			}
		}
		keep := make(map[string]bool, len(man2.Objects))
		for _, k := range man2.Objects {
			keep[k] = true
		}
		for _, k := range oldL1 {
			if keep[k] {
				continue
			}
			deleted, err := m.DeleteObjectWithOptions(ctx, s, epoch, k, replica.RetireOptions{OfflineQuiescent: true, Verify: opts.Verify})
			if err != nil {
				return res, fmt.Errorf("compaction: gc L1 %s: %w", k, err)
			}
			if deleted {
				res.DeletedL1++
			}
		}
		covered := preflightL0(ctx, m, s, epoch, l0, maxTx).covered
		res.DeletedL0, res.DeferredGC, res.GCSkipped, err = gcCoveredL0(ctx, m, s, epoch, maxTx, opts, covered)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

// l0Preflight is the result of classifying L0 object names against the
// manifest watermark. complete is false when any name is not strict canonical
// (unknown, mixed, or cross-floor), so the caller must decode that chain.
type l0Preflight struct {
	complete       bool
	covered        []string
	newKeys        []string
	newSegments    int
	newCountsKnown bool
	newObjects     int
	newBytes       int64
	newBytesKnown  bool
}

func preflightL0(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, l0 []string, floor uint64) l0Preflight {
	p := l0Preflight{complete: true, newCountsKnown: true}
	for _, key := range l0 {
		name := key
		if i := strings.LastIndexByte(name, '/'); i >= 0 {
			name = name[i+1:]
		}
		end, countable, known := strictCoveredNameEnd(name)
		if !known {
			p.complete = false
			continue
		}
		if end <= floor {
			p.covered = append(p.covered, key)
			continue
		}
		p.newKeys = append(p.newKeys, key)
		if countable {
			p.newSegments++
		} else {
			p.newCountsKnown = false
		}
		p.newObjects++
	}
	// Size listing is only needed to prove a below-threshold skip, and it compares
	// the same stored-byte count the decode path accumulates via len(part). An
	// empty or fully covered chain, an unknown/mixed chain, and a batch-only tail
	// either skip already or must decode, so they never pay for a second list.
	if !p.complete || len(p.newKeys) == 0 || !p.newCountsKnown {
		return p
	}
	sizes, sized := listObjectSizes(ctx, m.B, replica.Prefix(s, epoch))
	p.newBytesKnown = sized
	for _, key := range p.newKeys {
		if size, ok := sizes[key]; ok && size >= ltx.HeaderSize {
			p.newBytes += size
		} else {
			// A missing size, or one too small to hold even an LTX header, cannot
			// support the cheap skip: decode it so corruption still fails closed.
			p.newBytesKnown = false
		}
	}
	return p
}

// belowThreshold proves the skip from names and list sizes alone. A strict
// canonical .ltx or snapshot object is one logical segment, so newSegments is an
// upper bound. inputBytes accumulates len(part) of the stored (possibly LZ4)
// LTX bytes, not decoded pages; for one single-segment object the listed size is
// exactly that value, so the comparison uses the same unit as the decode path.
func (p l0Preflight) belowThreshold(opts Options) bool {
	if !p.complete || len(p.newKeys) == 0 || !p.newCountsKnown || !p.newBytesKnown {
		return false
	}
	belowObjects := opts.MinObjects <= 0 || p.newObjects < opts.MinObjects
	return opts.MinSegments > 0 && p.newSegments < opts.MinSegments &&
		opts.MinBytes > 0 && p.newBytes < opts.MinBytes && belowObjects
}

// listObjectSizes reads names and sizes through PagedLister without object
// bodies. A backend without it leaves sizes unknown and callers fall back to
// the decode path.
func listObjectSizes(ctx context.Context, b bucket.Bucket, prefix string) (map[string]int64, bool) {
	pl, ok := b.(bucket.PagedLister)
	if !ok {
		return nil, false
	}
	sizes := map[string]int64{}
	after := ""
	for page := 0; page < 100000; page++ {
		infos, next, err := pl.ListPage(ctx, prefix, after, 1000)
		if err != nil {
			return nil, false
		}
		for _, info := range infos {
			sizes[info.Key] = info.Size
		}
		if next == "" {
			return sizes, true
		}
		if next == after {
			return sizes, false
		}
		after = next
	}
	return sizes, false
}

// strictCoveredNameEnd recognizes names ltx.SegmentName and ltx.BatchName emit
// for a single segment, one paged-snapshot part, or a delta-only batch. The
// bool reports whether the name proves the object holds exactly one segment.
// Mixed (.m.batch), snapshot-batch (.s.batch), marker, link, legacy, and
// malformed names return ok=false so they stay on the decode path.
func strictCoveredNameEnd(name string) (end uint64, countable, known bool) {
	if strings.HasSuffix(name, ".snapshot") {
		end, known = strictSnapshotEnd(name)
		return end, true, known
	}
	if base, ok := strings.CutSuffix(name, ".ltx"); ok {
		end, known = strictRangeEnd(base)
		return end, true, known
	}
	if base, ok := strings.CutSuffix(name, ".d.batch"); ok {
		end, known = strictRangeEnd(base)
		return end, false, known
	}
	return 0, false, false
}

func strictSnapshotEnd(name string) (uint64, bool) {
	base, ok := strings.CutSuffix(name, ".snapshot")
	if !ok || base == "" {
		return 0, false
	}
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		txid, part := base[:dot], base[dot+1:]
		if !strings.HasPrefix(part, "p") {
			return 0, false
		}
		part = part[1:]
		if !canonicalUint(txid) || !canonicalUint(part) {
			return 0, false
		}
		n, err := strconv.ParseUint(txid, 10, 64)
		if err != nil {
			return 0, false
		}
		p, err := strconv.ParseUint(part, 10, 32)
		if err != nil || p >= ltx.MaxSnapshotParts {
			return 0, false
		}
		return n, true
	}
	if !canonicalUint(base) {
		return 0, false
	}
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func strictRangeEnd(base string) (uint64, bool) {
	i := strings.IndexByte(base, '-')
	if i <= 0 || i == len(base)-1 {
		return 0, false
	}
	startText, endText := base[:i], base[i+1:]
	if !canonicalUint(startText) || !canonicalUint(endText) {
		return 0, false
	}
	start, err := strconv.ParseUint(startText, 10, 64)
	if err != nil {
		return 0, false
	}
	end, err := strconv.ParseUint(endText, 10, 64)
	if err != nil || end < start {
		return 0, false
	}
	return end, true
}

func canonicalUint(s string) bool {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// gcCoveredL0 deletes only objects whose strict canonical name proved they are
// at or below the manifest watermark. Before deleting, it requires the persisted
// page index to cover that same floor: paged restore trusts PageIndex.Watermark,
// and a stale or replaced index behind the manifest could still need the L0
// tail. Unknown, mixed, and cross-floor objects are never touched, and no
// manifest or index is rewritten. A missing or lower index defers GC; a read
// error fails closed without deleting anything.
func gcCoveredL0(ctx context.Context, m *replica.Manager, s cell.Scope, epoch, floor uint64, opts Options, keys []string) (deleted, deferred int, skipped string, err error) {
	if !opts.GC || len(keys) == 0 {
		return 0, 0, "", nil
	}
	if !opts.OfflineQuiescent {
		return 0, len(keys), replica.ErrGCDeferred.Error(), nil
	}
	idx, ok, err := m.ReadIndex(ctx, s, epoch)
	if err != nil {
		return 0, 0, "", fmt.Errorf("compaction: read index before covered gc: %w", err)
	}
	if !ok {
		return 0, len(keys), "index missing", nil
	}
	if idx.Watermark < floor {
		return 0, len(keys), fmt.Sprintf("index watermark %d behind manifest %d", idx.Watermark, floor), nil
	}
	complete, err := m.HasCompleteSnapshot(ctx, s, epoch)
	if err != nil {
		return 0, 0, "", err
	}
	if !complete {
		return 0, len(keys), "complete snapshot proof missing", nil
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return 0, 0, "", err
		}
	}
	for _, key := range keys {
		didDelete, err := m.DeleteObjectWithOptions(ctx, s, epoch, key, replica.RetireOptions{OfflineQuiescent: true, Verify: opts.Verify})
		if err != nil {
			return deleted, 0, "", fmt.Errorf("compaction: gc covered L0 %s: %w", key, err)
		}
		if didDelete {
			deleted++
		}
	}
	return deleted, 0, "", nil
}
