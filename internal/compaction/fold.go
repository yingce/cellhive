package compaction

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
)

// FoldBaseline publishes the durable restore chain into epoch so this epoch
// becomes the chain base. Predecessor epochs can then be retired by native
// epoch GC. A new baseline is L9 when its logical image and stored inputs fit
// the epoch snapshot budget. When they do not, that same baseline is published
// at L2. Increments after the snapshot are L2 chunks. Fold does not publish L3.
// An existing remote L9 is streamed into the current epoch without re-checking
// that budget.
func FoldBaseline(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
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
	// A partial fold (baseline copied, an L2 chunk interrupted, then restart or a
	// new epoch) must not be treated as done: only skip when the current epoch
	// alone restores.
	if selfContained, err := epochSelfContained(ctx, m, s, epoch); err != nil {
		return Result{}, err
	} else if selfContained {
		l9n, covn, perr := maybePruneIdleEpoch(ctx, m, s, epoch, opts)
		if perr != nil {
			return Result{}, perr
		}
		return Result{Skipped: "already compacted", PrunedL9: l9n, PrunedCovered: covn, Epoch: epoch}, nil
	}
	chainKeys, err := m.RestoreChainKeys(ctx, s, epoch)
	if err != nil {
		return Result{}, err
	}
	if len(chainKeys) == 0 {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	files, err := nativeRangesOrdered(chainKeys)
	if err != nil {
		return Result{}, err
	}
	snapIdx := -1
	for i, f := range files {
		if f.min == 1 && levelOfKey(f.key) == 9 {
			snapIdx = i
			break
		}
	}
	if snapIdx < 0 {
		for i, f := range files {
			if f.min == 1 {
				snapIdx = i
				break
			}
		}
	}
	if snapIdx < 0 {
		return Result{Skipped: "no snapshot baseline"}, nil
	}
	snap := files[snapIdx]
	parts := 0
	inputs := 0
	var inputBytes int64

	// Plan post-snapshot coverage into bounded L2 chunks, then publish newest
	// chunk first. Oldest-first PUT makes epochChain raise `above` to the new
	// lo and hide still-needed predecessor ranges (crash → non-contiguous).
	// Newest-first keeps the cut recoverable after every successful PUT. The
	// baseline still lands last so lo==1 never appears with a gap.
	//
	// Predecessor deltas may overlap the snapshot or each other (L9[1,5], then
	// [6,10] and [9,15]). Publishing a later chunk verbatim makes the current
	// epoch start inside the still-unmoved older range, and epochChain then
	// requires a predecessor that ends exactly one tx earlier. Clip each chunk
	// to the watermark already covered by the snapshot and by older chunks.
	snapMax := snap.max
	rest := make([]nativeRange, 0, len(files))
	for _, f := range files[snapIdx+1:] {
		if f.max <= snapMax {
			continue
		}
		rest = append(rest, f)
	}
	var chunks [][]nativeRange
	for len(rest) > 0 {
		chunk, err := boundInputs(ctx, m, rest, opts)
		if err != nil {
			return Result{}, err
		}
		if len(chunk) == 0 {
			break
		}
		chunks = append(chunks, chunk)
		if len(chunk) >= len(rest) {
			break
		}
		rest = rest[len(chunk):]
	}
	for i := len(chunks) - 1; i >= 0; i-- {
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return Result{}, err
			}
		}
		chunk := chunks[i]
		covered := snapMax
		for j := 0; j < i; j++ {
			for _, f := range chunks[j] {
				if f.max > covered {
					covered = f.max
				}
			}
		}
		for len(chunk) > 0 && chunk[0].max <= covered {
			chunk = chunk[1:]
		}
		if len(chunk) == 0 {
			continue
		}
		var clip uint64
		if chunk[0].min <= covered {
			clip = covered + 1
			if clip > chunk[0].max {
				return Result{Skipped: "post-snapshot range cannot be trimmed"}, nil
			}
		}
		r, err := mergePublishFrom(ctx, m, s, epoch, 2, chunk, clip, false, opts)
		if err != nil {
			return Result{}, err
		}
		if r.Skipped != "" {
			return r, nil
		}
		parts += r.Parts
		inputs += r.Inputs
		inputBytes += r.InputBytes
		if opts.AfterChunk != nil {
			if err := opts.AfterChunk(r); err != nil {
				return Result{Inputs: inputs, InputBytes: inputBytes, Parts: parts, Epoch: epoch}, err
			}
		}
	}

	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	needBaseline := epochOfKey(snap.key) != epoch || levelOfKey(snap.key) != 9
	if needBaseline {
		if levelOfKey(snap.key) == 9 && epochOfKey(snap.key) != epoch {
			n, err := copyNativeToEpoch(ctx, m, s, epoch, 9, snap.key)
			if err != nil {
				return Result{}, err
			}
			parts++
			inputs++
			inputBytes += n
		} else {
			prefix := files[:snapIdx+1]
			level := uint16(9)
			allowSnapshot := false
			fits, ferr := newL9Fits(ctx, m, prefix, opts.SnapshotMaxBytes)
			if ferr != nil {
				return Result{}, ferr
			}
			if !fits {
				level = 2
				allowSnapshot = true
			}
			r, err := mergePublishFrom(ctx, m, s, epoch, level, prefix, 0, allowSnapshot, opts)
			if err != nil {
				return Result{}, err
			}
			if r.Skipped != "" {
				return r, nil
			}
			parts += r.Parts
			inputs += r.Inputs
			inputBytes += r.InputBytes
		}
	}

	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	ok, err := epochSelfContained(ctx, m, s, epoch)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, fmt.Errorf("compaction: fold left predecessor dependency")
	}
	wm, err := m.DurableWatermarkChain(ctx, s, epoch)
	if err != nil || wm == 0 {
		return Result{}, fmt.Errorf("compaction: fold restore watermark: %w", err)
	}
	l9n, covn, err := maybePruneIdleEpoch(ctx, m, s, epoch, opts)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Compacted:     true,
		Inputs:        inputs,
		InputBytes:    inputBytes,
		Parts:         parts,
		MinTxID:       1,
		MaxTxID:       wm,
		Epoch:         epoch,
		PrunedL9:      l9n,
		PrunedCovered: covn,
	}, nil
}

func maybePruneIdleEpoch(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (int, int, error) {
	if !opts.IdleDeleteOK {
		return 0, 0, nil
	}
	return PruneIdleEpoch(ctx, m, s, epoch, opts)
}

// epochSnapshotMax is the ceiling for a newly synthesized fold L9 when the
// caller does not pass SnapshotMaxBytes. It matches CELLHIVE_EPOCH_SNAPSHOT_MAX.
const epochSnapshotMax int64 = 1 << 30

// newL9Fits reports whether prefix can be published as a new L9. The logical
// image and the stored input bytes share one budget. An existing L9 copy does
// not call this.
func newL9Fits(ctx context.Context, m *replica.Manager, files []nativeRange, budget int64) (bool, error) {
	if len(files) == 0 {
		return false, nil
	}
	if budget <= 0 {
		budget = epochSnapshotMax
	}
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	var input int64
	snap := files[0]
	for _, f := range files {
		size, _, err := objects.Stat(ctx, f.key)
		if err != nil {
			return false, err
		}
		if size < 0 {
			return false, fmt.Errorf("compaction: unknown source size for %s", f.key)
		}
		input += size
		if f.max > snap.max {
			snap = f
		}
	}
	if input > budget {
		return false, nil
	}
	raw, _, err := objects.RangedGet(ctx, snap.key, 0, ltx.NativeHeaderSize)
	if err != nil {
		return false, err
	}
	h, err := ltx.ParseNativeHeader(raw)
	if err != nil {
		return false, err
	}
	if uint64(h.Commit)*uint64(h.PageSize) > uint64(budget) {
		return false, nil
	}
	return true, nil
}

// mergePublishFrom publishes files at level, clipping the first input to start
// at clip when clip > 0 (the caller proved that prefix is already covered by
// the snapshot being rehomed). allowSnapshot permits a tx-1 image at L2 when
// the L9 budget was not met.
func mergePublishFrom(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, level uint16, files []nativeRange, clip uint64, allowSnapshot bool, opts Options) (Result, error) {
	if len(files) == 0 {
		return Result{}, nil
	}
	refs := make([]replica.NativeFileRef, len(files))
	for i, f := range files {
		refs[i] = replica.NativeFileRef{Key: f.key, MinTXID: f.min, MaxTXID: f.max}
	}
	readers, size, cleanup, err := m.OpenNativeFiles(ctx, refs)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	if err := spoolSpaceFn(size); err != nil {
		return Result{}, err
	}
	out, err := os.CreateTemp("", "cellhive-fold-*.ltx")
	if err != nil {
		return Result{}, err
	}
	defer os.Remove(out.Name())
	defer out.Close()
	buffered := bufio.NewWriterSize(out, 256<<10)
	var h ltx.NativeHeader
	if clip != 0 {
		h, err = ltx.CompactNativeFrom(buffered, readers, clip)
	} else {
		h, err = ltx.CompactNative(buffered, readers)
	}
	if err != nil {
		return Result{}, err
	}
	if err := buffered.Flush(); err != nil {
		return Result{}, err
	}
	if level == 9 && !h.IsSnapshot() {
		return Result{}, fmt.Errorf("compaction: fold L9 is not a snapshot")
	}
	if !allowSnapshot && level != 9 && h.IsSnapshot() && h.MinTXID == 1 {
		// An incremental chunk that starts at tx 1 should have been the baseline.
		return Result{}, fmt.Errorf("compaction: refusing to publish snapshot at L%d", level)
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return Result{}, err
		}
	}
	if _, _, err := publishSpool(ctx, m, s, epoch, level, out); err != nil {
		return Result{}, err
	}
	return Result{Compacted: true, Inputs: len(readers), InputBytes: size, Parts: 1, MinTxID: h.MinTXID, MaxTxID: h.MaxTXID, Commit: h.Commit, Epoch: epoch}, nil
}

func copyNativeToEpoch(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, level uint16, srcKey string) (int64, error) {
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	// Stat first: a large L9 must be refused before its body is downloaded.
	size, _, err := objects.Stat(ctx, srcKey)
	if err != nil {
		return 0, err
	}
	if size < 0 {
		return 0, fmt.Errorf("compaction: unknown source size for %s", srcKey)
	}
	if err := spoolSpaceFn(size); err != nil {
		return 0, err
	}
	r, opened, _, err := objects.OpenReader(ctx, srcKey)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if opened >= 0 && opened != size {
		return 0, fmt.Errorf("compaction: source size changed for %s", srcKey)
	}
	out, err := os.CreateTemp("", "cellhive-fold-copy-*.ltx")
	if err != nil {
		return 0, err
	}
	defer os.Remove(out.Name())
	defer out.Close()
	written, err := io.Copy(out, r)
	if err != nil {
		return 0, err
	}
	if written != size {
		return 0, fmt.Errorf("compaction: copied %d bytes, expected %d", written, size)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	header := make([]byte, ltx.NativeHeaderSize)
	if _, err := io.ReadFull(out, header); err != nil {
		return 0, err
	}
	h, err := ltx.ParseNativeHeader(header)
	if err != nil {
		return 0, err
	}
	if level == 9 && !h.IsSnapshot() {
		return 0, fmt.Errorf("compaction: copied L9 is not a snapshot")
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if _, _, err := publishSpool(ctx, m, s, epoch, level, out); err != nil {
		return 0, err
	}
	return written, nil
}

func epochSelfContained(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64) (bool, error) {
	keys, err := m.RestoreChainKeys(ctx, s, epoch)
	if err != nil || len(keys) == 0 {
		return false, err
	}
	for _, key := range keys {
		if epochOfKey(key) != epoch {
			return false, nil
		}
	}
	return true, nil
}

func nativeRangesOrdered(keys []string) ([]nativeRange, error) {
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
	return out, nil
}

func levelOfKey(key string) uint16 {
	base := path.Base(path.Dir(key))
	if len(base) != 4 {
		return 0
	}
	v, err := strconv.ParseUint(base, 16, 16)
	if err != nil {
		return 0
	}
	return uint16(v)
}

func epochOfKey(key string) uint64 {
	const marker = "/ltx/e"
	i := strings.Index(key, marker)
	if i < 0 {
		return 0
	}
	rest := key[i+len(marker):]
	j := strings.IndexByte(rest, '/')
	if j <= 0 {
		return 0
	}
	v, err := strconv.ParseUint(rest[:j], 10, 64)
	if err != nil {
		return 0
	}
	return v
}
