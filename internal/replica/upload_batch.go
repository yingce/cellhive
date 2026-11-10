package replica

import (
	"context"
	"sort"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// UploadBatch accepts an arrival-order upload queue. Only contiguous ranges may
// share a native file; holes are published as separate immutable LTX objects.
// AppendBatch remains the strict single-file compaction primitive.
func (m *Manager) UploadBatch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) error {
	type input struct {
		raw      []byte
		min, max uint64
	}
	inputs := make([]input, len(segments))
	for i, raw := range segments {
		h, err := ltx.ParseNativeHeader(raw)
		if err != nil {
			return err
		}
		inputs[i] = input{raw, h.MinTXID, h.MaxTXID}
	}
	sort.SliceStable(inputs, func(i, j int) bool {
		if inputs[i].min != inputs[j].min {
			return inputs[i].min < inputs[j].min
		}
		return inputs[i].max > inputs[j].max
	})
	for start := 0; start < len(inputs); {
		end := start + 1
		high := inputs[start].max
		for end < len(inputs) && (high == ^uint64(0) || inputs[end].min <= high+1) {
			high = max(high, inputs[end].max)
			end++
		}
		group := make([][]byte, end-start)
		for i := start; i < end; i++ {
			group[i-start] = inputs[i].raw
		}
		if _, _, err := m.AppendBatch(ctx, s, epoch, group); err != nil {
			return err
		}
		start = end
	}
	return nil
}
