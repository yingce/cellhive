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
	seen := make(map[uint64]bool)
	var candidates []uint64
	for _, h := range headers {
		if h.Kind == ltx.KindSnapshot && !seen[h.StartTxID] {
			seen[h.StartTxID] = true
			candidates = append(candidates, h.StartTxID)
		}
	}
	if len(candidates) == 0 {
		return nil, nil, 0, fmt.Errorf("restore: no snapshot in chain")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })
	var firstErr error
	for _, base := range candidates {
		var snap, tail []int
		for i, h := range headers {
			if h.Kind == ltx.KindSnapshot && h.StartTxID == base {
				snap = append(snap, i)
			}
			if h.Kind == ltx.KindDelta && h.EndTxID > base {
				tail = append(tail, i)
			}
		}
		sort.SliceStable(snap, func(i, j int) bool {
			a, _ := ltx.SnapshotPart(headers[snap[i]])
			b, _ := ltx.SnapshotPart(headers[snap[j]])
			return a < b
		})
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
	return nil, nil, 0, firstErr
}
