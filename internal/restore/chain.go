package restore

import (
	"fmt"
	"sort"

	"cellhive/internal/ltx"
)

// selectChain chooses the newest snapshot that has an exact, contiguous delta
// suffix. A delta spanning a snapshot watermark cannot be discarded: its tail
// contains transactions newer than that snapshot. An older snapshot may provide
// a complete chain without replaying an ambiguous overlap.
func selectChain(headers []ltx.Header) (snapshots, deltas []int, watermark uint64, err error) {
	type setKey struct {
		epoch uint64
		base  uint64
	}
	// Epoch markers truncate tails from older epochs. A marker says epoch e
	// starts at StartTxID; any older epoch object ending above EndTxID is from
	// a fenced writer and must not enter the new lineage (ADR-275).
	cut := map[uint64]uint64{}
	for _, h := range headers {
		if h.Kind == ltx.KindMarker {
			cut[h.Epoch] = h.EndTxID
		}
	}
	var keep []int
	for i, h := range headers {
		truncated := false
		for e, c := range cut {
			if e > h.Epoch && h.EndTxID > c {
				truncated = true
				break
			}
		}
		if !truncated {
			keep = append(keep, i)
		}
	}

	sets := map[setKey][]int{}
	baseSeen := map[uint64]bool{}
	var bases []uint64
	for _, i := range keep {
		h := headers[i]
		if h.Kind != ltx.KindSnapshot {
			continue
		}
		k := setKey{epoch: h.Epoch, base: h.StartTxID}
		sets[k] = append(sets[k], i)
		if !baseSeen[h.StartTxID] {
			baseSeen[h.StartTxID] = true
			bases = append(bases, h.StartTxID)
		}
	}
	if len(bases) == 0 {
		return nil, nil, 0, fmt.Errorf("restore: no snapshot in chain")
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] > bases[j] })
	var firstErr error
	for _, base := range bases {
		// A later epoch can legitimately snapshot the same txid as its
		// predecessor when the cell was idle between restarts. Those snapshots
		// are alternatives, not parts of one snapshot: pick one epoch's complete
		// set, never concatenate sets from different epochs (the duplicate-page
		// failure on an idle cron cell).
		var epochs []uint64
		for k := range sets {
			if k.base == base {
				epochs = append(epochs, k.epoch)
			}
		}
		sort.Slice(epochs, func(i, j int) bool { return epochs[i] > epochs[j] })
		for _, epoch := range epochs {
			snap := append([]int(nil), sets[setKey{epoch: epoch, base: base}]...)
			if !completeSnapshotSet(headers, snap) {
				if firstErr == nil {
					firstErr = fmt.Errorf("restore: incomplete snapshot set for txid %d epoch %d", base, epoch)
				}
				continue
			}
			sort.SliceStable(snap, func(i, j int) bool {
				a, _ := ltx.SnapshotPart(headers[snap[i]])
				b, _ := ltx.SnapshotPart(headers[snap[j]])
				return a < b
			})
			var tail []int
			for _, i := range keep {
				h := headers[i]
				if h.Kind == ltx.KindDelta && h.EndTxID > base {
					tail = append(tail, i)
				}
			}
			sort.SliceStable(tail, func(i, j int) bool {
				a, b := headers[tail[i]], headers[tail[j]]
				if a.StartTxID != b.StartTxID {
					return a.StartTxID < b.StartTxID
				}
				return a.EndTxID < b.EndTxID
			})
			expected := base + 1
			var chainErr error
			for _, i := range tail {
				h := headers[i]
				if h.EndTxID < h.StartTxID {
					chainErr = fmt.Errorf("restore: invalid delta txid range %d..%d", h.StartTxID, h.EndTxID)
					break
				}
				if h.StartTxID != expected {
					chainErr = fmt.Errorf("restore: non-contiguous chain: expected txid %d, got %d", expected, h.StartTxID)
					break
				}
				expected = h.EndTxID + 1
			}
			if chainErr == nil {
				return snap, tail, expected - 1, nil
			}
			if firstErr == nil {
				firstErr = chainErr
			}
		}
	}
	return nil, nil, 0, firstErr
}

// completeSnapshotSet reports whether idx is one complete, non-overlapping
// snapshot set. Paged parts must be a unique 0..N-1 sequence; an unpaged
// snapshot is one part. Multiple unpaged copies are accepted only when their
// payload CRC matches (restoreOnce dedupes identical headers by ID).
func completeSnapshotSet(headers []ltx.Header, idx []int) bool {
	if len(idx) == 0 {
		return false
	}
	parts := map[int]bool{}
	pagedSeen := false
	unpaged := 0
	var crc uint32
	for _, i := range idx {
		h := headers[i]
		part, paged := ltx.SnapshotPart(h)
		if !paged {
			unpaged++
			if unpaged > 1 && h.CRC != crc {
				return false
			}
			crc = h.CRC
			continue
		}
		pagedSeen = true
		if parts[part] {
			return false
		}
		parts[part] = true
	}
	if unpaged > 0 {
		return !pagedSeen
	}
	if !pagedSeen {
		return false
	}
	total := len(parts)
	for i := 0; i < total; i++ {
		if !parts[i] {
			return false
		}
	}
	return true
}
