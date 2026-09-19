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

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// Options controls when and how a compaction runs.
type Options struct {
	// MinSegments and MinBytes gate the fold: compaction runs only when the
	// chain has at least MinSegments segments, or its raw size is at least
	// MinBytes. The thresholds are an OR so either a long or a large chain
	// triggers a fold.
	MinSegments int
	MinBytes    int64
	// MaxPartBytes bounds one compacted snapshot part (0 uses the ltx default).
	MaxPartBytes int
	// Verify, when non-nil, is called immediately before the L1 objects are
	// written so the caller can re-check ownership/epoch. A non-nil error aborts
	// before any write, so a stale node never publishes a manifest.
	Verify func(context.Context) error
	// GC deletes superseded objects after the new manifest is committed: old L1
	// objects no longer referenced by the manifest, and L0 objects whose segments
	// are all folded (EndTxID <= the new watermark).
	GC bool
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

	// The current L1 snapshot is the fold baseline.
	floor := uint64(0)
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
		floor = man.MaxTxID
	}

	l0, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}
	l0Max := map[string]uint64{}
	newL0 := 0
	for _, k := range l0 {
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
			if h.EndTxID > l0Max[k] {
				l0Max[k] = h.EndTxID
			}
			if floor > 0 && h.StartTxID <= floor {
				continue // already folded into the L1 baseline
			}
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
	}

	if !haveSnap {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	if newL0 == 0 && haveMan {
		return Result{Skipped: "already compacted"}, nil
	}
	if len(segs) < opts.MinSegments && inputBytes < opts.MinBytes {
		return Result{Skipped: "below threshold", Inputs: len(segs), InputBytes: inputBytes}, nil
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}

	parts, err := restore.Compact(segs, opts.MaxPartBytes)
	if err != nil {
		return Result{}, err
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
	if err := m.PutManifest(ctx, s, epoch, man2); err != nil {
		return Result{}, err
	}
	res := Result{
		Compacted: true, Inputs: len(segs), InputBytes: inputBytes, NewL0: newL0,
		Parts: len(parts), MinTxID: minTx, MaxTxID: maxTx, Commit: commit,
	}
	if opts.GC {
		keep := make(map[string]bool, len(man2.Objects))
		for _, k := range man2.Objects {
			keep[k] = true
		}
		for _, k := range oldL1 {
			if keep[k] {
				continue
			}
			if err := m.DeleteObject(ctx, k); err != nil {
				return res, fmt.Errorf("compaction: gc L1 %s: %w", k, err)
			}
			res.DeletedL1++
		}
		for _, k := range l0 {
			if l0Max[k] <= maxTx {
				if err := m.DeleteObject(ctx, k); err != nil {
					return res, fmt.Errorf("compaction: gc L0 %s: %w", k, err)
				}
				res.DeletedL0++
			}
		}
	}
	return res, nil
}
