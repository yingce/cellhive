package compaction

import (
	"cellhive/internal/cell"
	"cellhive/internal/replica"
	"context"
)

// DryRun reports bounded native L0 ranges selected for additive promotion. It neither
// downloads LTX bodies nor writes metadata; compaction never deletes sources.
func DryRun(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
	res := Result{Skipped: "dry-run", Epoch: epoch}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return res, err
		}
	}
	selected, seek, err := planLevel(ctx, m, s, epoch, 1)
	if err != nil {
		return res, err
	}
	selected, err = boundInputs(ctx, m, selected, opts)
	if err != nil {
		return res, err
	}
	res.Inputs = len(selected)
	res.NewL0 = len(selected)
	if len(selected) > 0 {
		res.MinTxID = seek
		res.MaxTxID = selected[len(selected)-1].max
	}
	return res, nil
}
