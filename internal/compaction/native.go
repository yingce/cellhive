package compaction

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
)

type nativeRange struct {
	key      string
	min, max uint64
}

func nativeRanges(keys []string) ([]nativeRange, error) {
	out := make([]nativeRange, 0, len(keys))
	for _, key := range keys {
		name := path.Base(key)
		if len(name) != 37 || name[16] != '-' || !strings.HasSuffix(name, ".ltx") || !canonicalHex(name[:16]) || !canonicalHex(name[17:33]) {
			return nil, fmt.Errorf("compaction: invalid native range %s", key)
		}
		min, _ := strconv.ParseUint(name[:16], 16, 64)
		max, _ := strconv.ParseUint(name[17:33], 16, 64)
		if min == 0 || max < min {
			return nil, fmt.Errorf("compaction: invalid transaction range %s", key)
		}
		out = append(out, nativeRange{key, min, max})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].min != out[j].min {
			return out[i].min < out[j].min
		}
		return out[i].max > out[j].max
	})
	return out, nil
}

// CompactLevel promotes only uncovered ranges from the adjacent source level.
// It does not rebuild the database or re-read the previous destination image.
func CompactLevel(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, level uint16, opts Options) (Result, error) {
	if level < 1 || level > 3 {
		return Result{}, fmt.Errorf("compaction: invalid destination level %d", level)
	}
	release, err := acquireJob(ctx)
	if err != nil {
		return Result{}, err
	}
	defer release()
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	selected, seek, err := planLevel(ctx, m, s, epoch, level)
	if err != nil {
		return Result{}, err
	}
	if len(selected) == 0 {
		return Result{Skipped: "already compacted"}, nil
	}
	countEnough := (opts.MinSegments > 0 && len(selected) >= opts.MinSegments) || (opts.MinObjects > 0 && len(selected) >= opts.MinObjects)
	thresholds := opts.MinSegments > 0 || opts.MinObjects > 0 || opts.MinBytes > 0
	if thresholds && !countEnough && opts.MinBytes <= 0 {
		return Result{Skipped: "below threshold", Inputs: len(selected)}, nil
	}
	unboundedCount := len(selected)
	selected, err = boundInputs(ctx, m, selected, opts)
	if err != nil {
		return Result{}, err
	}
	refs := make([]replica.NativeFileRef, len(selected))
	for i, f := range selected {
		refs[i] = replica.NativeFileRef{Key: f.key, MinTXID: f.min, MaxTXID: f.max}
	}
	readers, inputBytes, cleanup, err := m.OpenNativeFiles(ctx, refs)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	// Reaching a batch budget must make progress even when the requested
	// minimum byte threshold is larger than that budget.
	if thresholds && !countEnough && inputBytes < opts.MinBytes && len(selected) == unboundedCount {
		return Result{Skipped: "below threshold", Inputs: len(selected), InputBytes: inputBytes}, nil
	}
	if err := checkSpoolSpace(inputBytes); err != nil {
		return Result{}, err
	}
	out, err := os.CreateTemp("", "cellhive-compaction-*.ltx")
	if err != nil {
		return Result{}, err
	}
	defer os.Remove(out.Name())
	defer out.Close()
	buffered := bufio.NewWriterSize(out, 256<<10)
	h, err := ltx.CompactNativeFrom(buffered, readers, seek)
	if err != nil {
		return Result{}, err
	}
	if err := buffered.Flush(); err != nil {
		return Result{}, err
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	if _, _, err := publishSpool(ctx, m, s, epoch, level, out); err != nil {
		return Result{}, err
	}
	r := Result{Compacted: true, Inputs: len(selected), InputBytes: inputBytes, Parts: 1, MinTxID: h.MinTXID, MaxTxID: h.MaxTXID, Commit: h.Commit, Epoch: epoch}
	if level == 1 {
		r.NewL0 = len(selected)
	}

	return r, nil
}

// Snapshot publishes the complete native chain at L9. Ordinary level
// promotions never need this database-wide merge.
func Snapshot(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
	release, err := acquireJob(ctx)
	if err != nil {
		return Result{}, err
	}
	defer release()
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	watermark, err := m.DurableWatermarkChain(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}
	if watermark == 0 {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	keys, err := m.ListLevelFrom(ctx, s, epoch, 9, 0)
	if err != nil {
		return Result{}, err
	}
	files, err := nativeRanges(keys)
	if err != nil {
		return Result{}, err
	}
	for _, f := range files {
		if f.max >= watermark {
			return Result{Skipped: "already compacted"}, nil
		}
	}
	// Keep the canonical plan fixed between budget checking and opening inputs.
	chainKeys, err := m.RestoreChainKeys(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}
	if len(chainKeys) == 0 {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	latest, err := nativeRanges(chainKeys[len(chainKeys)-1:])
	if err != nil {
		return Result{}, err
	}
	raw, _, err := objects.RangedGet(ctx, latest[0].key, 0, ltx.NativeHeaderSize)
	if err != nil {
		return Result{}, err
	}
	header, err := ltx.ParseNativeHeader(raw)
	if err != nil {
		return Result{}, err
	}
	if header.MinTXID != latest[0].min || header.MaxTXID != latest[0].max {
		return Result{}, fmt.Errorf("compaction: snapshot source range/header mismatch")
	}
	budget := opts.SnapshotMaxBytes
	if budget <= 0 {
		budget = DefaultSnapshotMaxBytes
	}
	if uint64(header.Commit)*uint64(header.PageSize) > uint64(budget) {
		return Result{Skipped: "snapshot logical size exceeds budget"}, nil
	}
	maxInputs, _ := inputLimits(opts)
	if len(chainKeys) > maxInputs {
		return Result{Skipped: "snapshot input count exceeds budget", Inputs: len(chainKeys)}, nil
	}
	snapshotFiles, err := nativeRanges(chainKeys)
	if err != nil {
		return Result{}, err
	}
	bounded, err := boundInputs(ctx, m, snapshotFiles, opts)
	if err != nil {
		return Result{}, err
	}
	if len(bounded) != len(snapshotFiles) {
		return Result{Skipped: "snapshot input bytes exceed budget", Inputs: len(chainKeys)}, nil
	}
	refs := make([]replica.NativeFileRef, len(chainKeys))
	for i, key := range chainKeys {
		f, err := nativeRanges([]string{key})
		if err != nil {
			return Result{}, err
		}
		refs[i] = replica.NativeFileRef{Key: key, MinTXID: f[0].min, MaxTXID: f[0].max}
	}
	readers, size, cleanup, err := m.OpenNativeFiles(ctx, refs)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	if err := checkSpoolSpace(size); err != nil {
		return Result{}, err
	}
	out, err := os.CreateTemp("", "cellhive-compaction-*.ltx")
	if err != nil {
		return Result{}, err
	}
	defer os.Remove(out.Name())
	defer out.Close()
	buffered := bufio.NewWriterSize(out, 256<<10)
	h, err := ltx.CompactNative(buffered, readers)
	if err != nil {
		return Result{}, err
	}
	if err := buffered.Flush(); err != nil {
		return Result{}, err
	}
	if !h.IsSnapshot() {
		return Result{}, fmt.Errorf("compaction: incomplete snapshot chain")
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	if _, _, err := publishSpool(ctx, m, s, epoch, 9, out); err != nil {
		return Result{}, err
	}
	return Result{Compacted: true, Inputs: len(readers), InputBytes: size, Parts: 1, MinTxID: 1, MaxTxID: h.MaxTXID, Commit: h.Commit, Epoch: epoch}, nil
}

// planLevel shares the native name-only coverage plan with maintenance dry runs.
func planLevel(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, level uint16) ([]nativeRange, uint64, error) {
	destKeys, err := m.ListLevelFrom(ctx, s, epoch, level, 0)
	if err != nil {
		return nil, 0, err
	}
	dest, err := nativeRanges(destKeys)
	if err != nil {
		return nil, 0, err
	}
	var floor uint64
	for _, f := range dest {
		if floor != 0 && f.min > floor+1 {
			return nil, 0, fmt.Errorf("compaction: destination level has a gap")
		}
		if f.max > floor {
			floor = f.max
		}
	}
	if floor == ^uint64(0) {
		return nil, 0, nil
	}
	seek := floor + 1
	keys, err := m.ListLevelFrom(ctx, s, epoch, level-1, seek)
	if err != nil {
		return nil, 0, err
	}
	sources, err := nativeRanges(keys)
	if err != nil {
		return nil, 0, err
	}
	// A source starting before the seek may contain its uncovered suffix.
	if len(sources) == 0 || sources[0].min != seek {
		keys, err = m.ListLevelFrom(ctx, s, epoch, level-1, 0)
		if err != nil {
			return nil, 0, err
		}
		sources, err = nativeRanges(keys)
		if err != nil {
			return nil, 0, err
		}
	}
	if floor == 0 && len(sources) > 0 {
		seek = sources[0].min
	} // continued epoch base
	next := seek
	selected := make([]nativeRange, 0, len(sources))
	for _, f := range sources {
		if f.max < next {
			continue
		}
		if f.min > next {
			if len(selected) == 0 {
				return nil, 0, fmt.Errorf("compaction: source does not continue destination")
			}
			break
		}
		selected = append(selected, f)
		if f.max == ^uint64(0) {
			break
		}
		next = f.max + 1
	}
	if len(selected) == 0 {
		return nil, 0, nil
	}
	return selected, seek, nil
}

func publishSpool(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, level uint16, spool *os.File) (string, string, error) {
	if err := spool.Sync(); err != nil {
		return "", "", err
	}
	info, err := spool.Stat()
	if err != nil {
		return "", "", err
	}
	return m.AppendLevelFile(ctx, s, epoch, level, spool.Name(), info.Size())
}
