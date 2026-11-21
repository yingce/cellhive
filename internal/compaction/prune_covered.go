package compaction

import (
	"context"
	"fmt"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// PruneCoveredBelowL9 deletes same-epoch L0/L1/L2/L3 objects whose max TXID is
// <= the highest L9 max (minTXID==1). Requires IdleDeleteOK (drain / no pinned
// readers). Proves the retained L9 and restore chain before any delete.
func PruneCoveredBelowL9(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (int, error) {
	if !opts.IdleDeleteOK {
		return 0, nil
	}
	bestKey, floor, err := selectRetainedL9(ctx, m, s, epoch)
	if err != nil || floor == 0 {
		return 0, err
	}
	if err := proveRetainedIdleChain(ctx, m, s, epoch, bestKey, floor); err != nil {
		return 0, err
	}
	deleted := 0
	for _, level := range []uint16{0, 1, 2, 3} {
		keys, err := m.ListLevelFrom(ctx, s, epoch, level, 0)
		if err != nil {
			return deleted, err
		}
		for _, key := range keys {
			_, max, ok := l9Span(key)
			if !ok {
				return deleted, fmt.Errorf("compaction: invalid level key %s", key)
			}
			if max > floor {
				continue
			}
			if opts.Verify != nil {
				if err := opts.Verify(ctx); err != nil {
					return deleted, err
				}
			}
			okDel, err := m.DeleteObjectWithOptions(ctx, s, epoch, key, replica.RetireOptions{
				OfflineQuiescent: true,
				Verify:           opts.Verify,
			})
			if err != nil {
				return deleted, err
			}
			if okDel {
				deleted++
			}
		}
	}
	return deleted, nil
}

// PruneIdleEpoch removes dominated same-epoch L9 copies, then lower-level
// objects fully covered by the retained L9 watermark. Intended for idle fold
// / eviction paths only when IdleDeleteOK is set.
func PruneIdleEpoch(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (l9Deleted, coveredDeleted int, err error) {
	if !opts.IdleDeleteOK {
		return 0, 0, nil
	}
	l9Deleted, err = PruneSupersededL9(ctx, m, s, epoch, opts)
	if err != nil {
		return l9Deleted, 0, err
	}
	coveredDeleted, err = PruneCoveredBelowL9(ctx, m, s, epoch, opts)
	return l9Deleted, coveredDeleted, err
}

func selectRetainedL9(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64) (bestKey string, bestMax uint64, err error) {
	l9Keys, err := m.ListLevelFrom(ctx, s, epoch, 9, 0)
	if err != nil {
		return "", 0, err
	}
	for _, key := range l9Keys {
		min, max, ok := l9Span(key)
		if !ok || min != 1 {
			return "", 0, fmt.Errorf("compaction: invalid L9 key %s", key)
		}
		if bestKey == "" || max > bestMax || (max == bestMax && key < bestKey) {
			bestKey, bestMax = key, max
		}
	}
	return bestKey, bestMax, nil
}

func proveRetainedIdleChain(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, bestKey string, bestMax uint64) error {
	raw, err := m.ReadSegment(ctx, bestKey)
	if err != nil {
		return fmt.Errorf("compaction: retained L9 unreadable: %w", err)
	}
	h, err := ltx.ValidateNative(raw)
	if err != nil {
		return fmt.Errorf("compaction: retained L9 invalid: %w", err)
	}
	if h.MinTXID != 1 || h.MaxTXID != bestMax {
		return fmt.Errorf("compaction: retained L9 header/path mismatch")
	}
	keys, err := m.RestoreChainKeys(ctx, s, epoch)
	if err != nil || len(keys) == 0 {
		return fmt.Errorf("compaction: retained chain not restorable: %w", err)
	}
	found := false
	for _, key := range keys {
		if key == bestKey {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("compaction: restore plan does not use retained L9")
	}
	refs := make([]replica.NativeFileRef, len(keys))
	for i, key := range keys {
		min, max, ok := l9Span(key)
		if !ok {
			return fmt.Errorf("compaction: invalid retained chain key %s", key)
		}
		refs[i] = replica.NativeFileRef{Key: key, MinTXID: min, MaxTXID: max}
	}
	readers, _, cleanup, err := m.OpenNativeFiles(ctx, refs)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, err := restore.ValidateChainReaders(readers); err != nil {
		return fmt.Errorf("compaction: retained chain failed validation: %w", err)
	}
	return nil
}
