package compaction

import (
	"context"
	"path"
	"strconv"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

// PruneSupersededL9 deletes same-epoch L9 snapshots dominated by a higher max
// TXID. All candidates must be minTXID==1 snapshots. Requires IdleDeleteOK
// (drain / no paged reader pinned to an older L9). Proves the retained L9 and
// restore chain before deleting dominated copies. Manual prune-l9 admin/CLI is
// intentionally not restored.
func PruneSupersededL9(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (int, error) {
	if !opts.IdleDeleteOK {
		return 0, nil
	}
	keys, err := m.ListLevelFrom(ctx, s, epoch, 9, 0)
	if err != nil {
		return 0, err
	}
	if len(keys) <= 1 {
		return 0, nil
	}
	bestKey, bestMax, err := selectRetainedL9(ctx, m, s, epoch)
	if err != nil {
		return 0, err
	}
	if err := proveRetainedIdleChain(ctx, m, s, epoch, bestKey, bestMax); err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range keys {
		if key == bestKey {
			continue
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return deleted, err
			}
		}
		ok, err := m.DeleteObjectWithOptions(ctx, s, epoch, key, replica.RetireOptions{
			OfflineQuiescent: true,
			Verify:           opts.Verify,
		})
		if err != nil {
			return deleted, err
		}
		if ok {
			deleted++
		}
	}
	return deleted, nil
}

func l9Span(key string) (min, max uint64, ok bool) {
	name := path.Base(key)
	if len(name) != 37 || name[16] != '-' || path.Ext(name) != ".ltx" {
		return 0, 0, false
	}
	var err error
	min, err = strconv.ParseUint(name[:16], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	max, err = strconv.ParseUint(name[17:33], 16, 64)
	if err != nil || min == 0 || max < min {
		return 0, 0, false
	}
	return min, max, true
}
