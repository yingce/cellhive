package recovery

import (
	"context"
	"errors"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

// recoveryBatch keeps a bounded contiguous run. Snapshot/marker objects must
// retain their standalone names: restore discovers baselines from those names.
type recoveryBatch struct {
	rep      *replica.Manager
	scope    cell.Scope
	epoch    uint64
	last     uint64
	segments [][]byte
	bytes    int
	uploaded int
}

func (b *recoveryBatch) flush(ctx context.Context) error {
	if len(b.segments) == 0 {
		return nil
	}
	_, _, err := b.rep.AppendBatch(ctx, b.scope, b.epoch, b.segments)
	if err != nil && !errors.Is(err, replica.ErrResourceDeleted) {
		return err
	}
	if err == nil {
		b.uploaded += len(b.segments)
	}
	b.segments = nil
	b.bytes = 0
	return nil
}

func (b *recoveryBatch) add(ctx context.Context, scope cell.Scope, epoch uint64, raw []byte) error {
	h, _, err := ltx.Decode(raw)
	if err != nil {
		return err
	}
	// AppendBatch validates header epochs before writing, including singles.
	if len(b.segments) > 0 && (scope != b.scope || epoch != b.epoch || h.Kind != ltx.KindDelta || b.last == ^uint64(0) || h.StartTxID != b.last+1 || len(b.segments) >= 64 || b.bytes+len(raw) > 1<<20) {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}
	b.scope, b.epoch, b.last = scope, epoch, h.EndTxID
	b.segments = append(b.segments, raw)
	b.bytes += len(raw)
	if h.Kind != ltx.KindDelta || len(b.segments) >= 64 || b.bytes >= 1<<20 {
		return b.flush(ctx)
	}
	return nil
}
