package compaction

import (
	"context"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

// DryRun classifies what a Safe compaction pass would delete without writing a
// tombstone or deleting an object. It is intentionally conservative: only
// strict canonical covered L0 objects and the previous L1 generation beyond the
// requested retention are counted.
func DryRun(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
	res := Result{Skipped: "dry-run"}
	man, haveMan, err := m.ReadManifest(ctx, s, epoch)
	if err != nil {
		return res, err
	}
	l0, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return res, err
	}
	floor := uint64(0)
	if haveMan {
		floor = man.MaxTxID
		res.Epoch = man.Epoch
	}
	pre := preflightL0(ctx, m, s, epoch, l0, floor)
	res.Covered = len(pre.covered)
	res.Deferred = len(pre.newKeys)
	res.NewL0 = len(pre.newKeys)
	res.Inputs = len(pre.newKeys)
	res.DeferredGC = len(pre.covered)
	generations := opts.l1Generations()
	if haveMan && generations == 1 {
		res.DeferredGC += len(man.Objects)
	}
	if !pre.complete {
		res.GCSkipped = "dry-run; unknown L0 names"
	} else {
		res.GCSkipped = "dry-run"
	}
	res.Compacted = false
	return res, nil
}
