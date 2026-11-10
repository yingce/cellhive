// Package replica stores and retrieves cell replication segments (P0.3).
//
// Segments live under cells/<scope>/ltx/e<epoch>/0000/<name>. Append validates the
// LTX header and (for backend A) the caller's epoch before writing. Listing is
// a cold-path operation used only by restore/recovery, never by hot paths.
package replica

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"cellhive/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// Manager reads and writes replication segments for cells.
type Manager struct {
	localCache      *localLTXCache
	B               bucket.Bucket
	RestoreParallel int
	resourceLocks   [64]sync.RWMutex

	retiredMu    sync.Mutex
	retiredCache map[string]retiredCacheEntry

	fenceMu      sync.Mutex
	fenceCache   map[string]fenceCacheEntry
	fenceRev     uint64
	fenceFlights map[string]*fenceFlight
}

// New creates a Manager.
func New(b bucket.Bucket) *Manager { return &Manager{B: b} }

// Prefix returns the segment key prefix for a scope/epoch.
func Prefix(s cell.Scope, epoch uint64) string {
	return fmt.Sprintf("%s/ltx/e%d/", s.Key(), epoch)
}

// EpochPrefix is the celld epoch root. Level listings are performed below
// 0000/ and 0001/ separately; callers that need metadata/GC use this root.
func EpochPrefix(s cell.Scope, epoch uint64) string {
	return fmt.Sprintf("%s/ltx/e%d/", s.Key(), epoch)
}

func celldL0Prefix(s cell.Scope, epoch uint64) string { return EpochPrefix(s, epoch) + "0000/" }
func celldL1Prefix(s cell.Scope, epoch uint64) string { return EpochPrefix(s, epoch) + "0001/" }

// celldLevels is the sparse level set used by the celld replica client.  The
// snapshot level is 0009; levels 0004 through 0008 are not probed.  Keeping
// this list explicit avoids empty object-store scans for levels that do not
// exist in the configured compaction policy.
var celldLevels = [...]uint16{0, 1, 2, 3, 9}

func (m *Manager) listCanonicalLevels(ctx context.Context, s cell.Scope, epoch uint64) (map[uint16][]string, error) {
	return m.listCanonicalLevelsBounded(ctx, s, epoch, 0)
}

// upper is an exact predecessor cut established by a successor epoch, never a
// guessed latest TXID. Without it, earlier starts may hide an uncovered tail.
func (m *Manager) listCanonicalLevelsBounded(ctx context.Context, s cell.Scope, epoch, upper uint64) (map[uint16][]string, error) {
	out := make(map[uint16][]string, len(celldLevels))
	base := EpochPrefix(s, epoch)
	var present map[uint16]bool
	if pl, ok := m.B.(bucket.PrefixLister); ok {
		prefixes, err := pl.CommonPrefixes(ctx, base, "/")
		if err != nil {
			return nil, err
		}
		present = make(map[uint16]bool)
		for _, child := range prefixes {
			rest, ok := strings.CutPrefix(child, base)
			if !ok || len(rest) != 5 || rest[4] != '/' {
				continue
			}
			parsed, err := strconv.ParseUint(rest[:4], 16, 16)
			if err == nil && validLevel(uint16(parsed)) {
				present[uint16(parsed)] = true
			}
		}
	}
	var bounded []epochFile
	for _, level := range [...]uint16{9, 3, 2, 1, 0} {
		if present != nil && !present[level] {
			continue
		}
		prefix := base + fmt.Sprintf("%04x/", level)
		keys, err := m.listLevel(ctx, prefix)
		if err != nil {
			return nil, err
		}
		out[level] = keys
		if upper == 0 {
			continue
		}
		for _, key := range keys {
			span, ok := parseSegmentSpan(strings.TrimPrefix(key, prefix))
			if !ok || span.start == 0 || span.end < span.start {
				return nil, fmt.Errorf("replica: invalid native LTX path %s", key)
			}
			if span.end <= upper {
				bounded = append(bounded, epochFile{key: key, epoch: epoch, level: int(level), min: span.start, max: span.end})
			}
		}
		// Missing spans are not errors here: lower levels may still fill them.
		// An exact complete chain proves every skipped eligible range is covered.
		plan, err := planEpochFiles(bounded)
		if err == nil && len(plan) > 0 && plan[len(plan)-1].max == upper {
			return out, nil
		}
	}
	return out, nil
}

// CelldL1Prefix exposes the canonical level-1 path to compaction and tooling.
func CelldL1Prefix(s cell.Scope, epoch uint64) string { return celldL1Prefix(s, epoch) }

// L1Prefix is the compacted (L1) segment prefix for a scope/epoch. It is nested
// under Prefix so epoch fencing still applies, and is excluded from L0 listings.
func L1Prefix(s cell.Scope, epoch uint64) string {
	return celldL1Prefix(s, epoch)
}

// Covers reports whether the native chain has contiguous durable coverage
// through txid. It is a cold-path check used before reclaiming a local file.
func (m *Manager) Covers(ctx context.Context, s cell.Scope, epoch, txid uint64) (bool, error) {
	watermark, err := m.DurableWatermarkChain(ctx, s, epoch)
	if err == nil && watermark != 0 && watermark >= txid {
		return true, nil
	}
	if errors.Is(err, ErrResourceDeleted) {
		return false, err
	}
	retired, markErr := m.EpochRetired(ctx, s, epoch)
	if markErr != nil {
		return false, markErr
	}
	if retired {
		return true, nil
	}
	return false, err
}

// DurableWatermark returns the native chain watermark through epoch. Epochs
// that continue a predecessor share the same validated chain, not a private
// per-epoch manifest. Listing is restricted to cold activation/recovery.
func (m *Manager) DurableWatermark(ctx context.Context, s cell.Scope, epoch uint64) (uint64, error) {
	return m.DurableWatermarkChain(ctx, s, epoch)
}

// Epochs lists the epochs (<= maxEpoch, 0=unbounded) that have LTX objects for
// the scope, ascending. Cold path.
func (m *Manager) Epochs(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]uint64, error) {
	return m.epochDirs(ctx, s, maxEpoch)
}

// epochDirs returns the epochs that have a prefix under <scope>/ltx/, ascending.
// A delimiter listing returns only the e<N>/ directories, so its cost does not
// grow with the number of objects in the chain; a bucket without it falls back
// to listing every key.
func (m *Manager) epochDirs(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]uint64, error) {
	base := s.Key() + "/ltx/"
	set := map[uint64]bool{}
	add := func(rest string) {
		rest = strings.TrimSuffix(rest, "/")
		if !strings.HasPrefix(rest, "e") {
			return
		}
		n, err := strconv.ParseUint(rest[1:], 10, 64)
		if err != nil || (maxEpoch > 0 && n > maxEpoch) {
			return
		}
		set[n] = true
	}
	if pl, ok := m.B.(bucket.PrefixLister); ok {
		prefixes, err := pl.CommonPrefixes(ctx, base, "/")
		if err != nil {
			return nil, err
		}
		for _, p := range prefixes {
			if rest, ok := strings.CutPrefix(p, base); ok {
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					rest = rest[:i]
				}
				add(rest)
			}
		}
	} else {
		keys, err := m.B.List(ctx, base)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			rest, ok := strings.CutPrefix(k, base)
			if !ok {
				continue
			}
			if i := strings.IndexByte(rest, '/'); i > 0 {
				add(rest[:i])
			}
		}
	}
	out := make([]uint64, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// DurableWatermarkChain plans the native chain from canonical range names.
// It stops scanning predecessors at the first complete snapshot epoch and
// does not download LTX bodies to discover snapshots or continuation cuts.
func (m *Manager) DurableWatermarkChain(ctx context.Context, s cell.Scope, maxEpoch uint64) (uint64, error) {
	plan, err := m.nativeRestorePlan(ctx, s, maxEpoch)
	if err != nil {
		return 0, err
	}
	if len(plan) == 0 {
		return 0, nil
	}
	return plan[len(plan)-1].max, nil
}

// HasCompleteSnapshot proves a native current-epoch chain independent of all
// predecessors. Snapshot identity is MinTXID==1 at any native level.
func (m *Manager) HasCompleteSnapshot(ctx context.Context, s cell.Scope, epoch uint64) (bool, error) {
	plan, err := m.nativeRestorePlan(ctx, s, epoch)
	if err != nil {
		return false, err
	}
	if len(plan) == 0 || plan[0].min != 1 {
		return false, nil
	}
	for _, f := range plan {
		if f.epoch != epoch {
			return false, nil
		}
	}
	refs := make([]NativeFileRef, len(plan))
	for i, f := range plan {
		refs[i] = NativeFileRef{Key: f.key, MinTXID: f.min, MaxTXID: f.max}
	}
	readers, _, cleanup, err := m.OpenNativeFiles(ctx, refs)
	if err != nil {
		return false, err
	}
	defer cleanup()
	if _, err := restore.ValidateChainReaders(readers); err != nil {
		return false, err
	}
	return true, nil
}

// RetireEpochs uses the native epoch-retirement safety gates. Callers must
// satisfy the ownership, materialization and age proofs before physical deletion.
func (m *Manager) RetireEpochs(ctx context.Context, s cell.Scope, current uint64, keepPrevious int) (int, error) {
	return m.RetireEpochsWithOptions(ctx, s, current, keepPrevious, RetireOptions{})
}

// RetireEpochsWithOptions uses the native restore chain and stream retirement mark.
func (m *Manager) RetireEpochsWithOptions(ctx context.Context, s cell.Scope, current uint64, keepPrevious int, opts RetireOptions) (int, error) {
	return m.retireNativeEpochs(ctx, s, current, keepPrevious, opts)
}

// RestoreChain buffers the native planner's selected cross-epoch chain.
// Large-cell paths should use PageFetcher or OpenRestoreChain instead.
func (m *Manager) RestoreChain(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]Segment, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		segs, err := m.restoreChainOnce(ctx, s, maxEpoch)
		if err == nil {
			return segs, nil
		}
		if !errors.Is(err, bucket.ErrNotFound) {
			return nil, err
		}
		// Epoch GC may have retired an epoch that this attempt planned to read.
		// Re-list: a newer complete baseline can move the start epoch above it.
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func (m *Manager) restoreChainOnce(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]Segment, error) {
	plan, err := m.nativeRestorePlan(ctx, s, maxEpoch)
	if err != nil {
		return nil, err
	}
	return m.readNativePlan(ctx, plan)
}

// segmentSpan is one L0 object's txid coverage.
type segmentSpan struct {
	start, end uint64
	snapshot   bool
	// marker is an epoch marker (ADR-275): the epoch starts at start and every
	// older-epoch object ending above end (= start-1) is dead.
	marker bool
}

// parseSegmentSpan decodes an L0 object name into its txid range. Snapshot parts
// share the watermark EndTxID; batches and deltas carry Start-End.
func parseSegmentSpan(name string) (segmentSpan, bool) {
	var out segmentSpan
	if strings.HasSuffix(name, ".ltx") {
		return parseSegmentRange(strings.TrimSuffix(name, ".ltx"))
	}
	return out, false
}

// isSegmentKey reports whether an object under the epoch prefix is an LTX
// segment (markers such as "<txid>.snapshot-done" are not).
func isSegmentKey(key string) bool {
	name := key
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	return strings.HasSuffix(name, ".ltx")
}

func parseSegmentRange(base string) (segmentSpan, bool) {
	var out segmentSpan
	i := strings.IndexByte(base, '-')
	if i <= 0 || i == len(base)-1 {
		return out, false
	}
	start, err := parseCanonicalHexTXID(base[:i])
	if err != nil {
		return out, false
	}
	end, err := parseCanonicalHexTXID(base[i+1:])
	if err != nil {
		return out, false
	}
	out.start, out.end = start, end
	if end+1 == start {
		out.marker = true
	}
	return out, true
}

func parseCanonicalHexTXID(s string) (uint64, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("non-canonical txid")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return 0, fmt.Errorf("non-canonical txid")
		}
	}
	return strconv.ParseUint(s, 16, 64)
}

// LatestEpoch returns the highest epoch that has any replica object for the
// scope (optionally bounded by maxEpoch, 0 = unbounded). ok is false when the
// scope has no replica objects. It lists the bucket, so it is a cold-path
// operation for restore/takeover only.
func (m *Manager) LatestEpoch(ctx context.Context, s cell.Scope, maxEpoch uint64) (uint64, bool, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "replica.latest_epoch")
	defer span.End()
	span.SetAttributes(attribute.String("cell.scope", s.String()))
	epochs, err := m.epochDirs(ctx, s, maxEpoch)
	if err != nil || len(epochs) == 0 {
		return 0, false, err
	}
	return epochs[len(epochs)-1], true, nil
}

// Append validates and stores a segment, returning its object key and etag.
func (m *Manager) Append(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) (string, string, error) {
	return m.AppendLevel(ctx, s, epoch, 0, segment)
}

// AppendBatch folds native files into one native range, as celld compaction
// does. Byte-identical retries are deduplicated; conflicting equal ranges fail.
func (m *Manager) AppendBatch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
	return m.appendNativeBatch(ctx, s, epoch, 0, segments)
}

func (m *Manager) appendNativeBatch(ctx context.Context, s cell.Scope, epoch uint64, level uint16, segments [][]byte) (string, string, error) {
	if len(segments) == 0 {
		return "", "", fmt.Errorf("replica: empty batch")
	}
	type txRange struct{ min, max uint64 }
	seen := make(map[txRange][]byte, len(segments))
	readers := make([]io.Reader, 0, len(segments))
	var only []byte
	for _, raw := range segments {
		h, err := ltx.ParseNativeHeader(raw)
		if err != nil {
			return "", "", err
		}
		span := txRange{h.MinTXID, h.MaxTXID}
		if previous, ok := seen[span]; ok {
			if !bytes.Equal(previous, raw) {
				return "", "", fmt.Errorf("replica: conflicting batch range: %w", bucket.ErrPrecondition)
			}
			continue
		}
		seen[span] = raw
		readers = append(readers, bytes.NewReader(raw))
		only = raw
	}
	if len(readers) == 1 {
		return m.AppendLevel(ctx, s, epoch, level, only)
	}
	var out bytes.Buffer
	if _, err := ltx.CompactNative(&out, readers); err != nil {
		return "", "", err
	}
	return m.AppendLevel(ctx, s, epoch, level, out.Bytes())
}

// AppendSnapshotFile publishes a fully assembled snapshot as one canonical
// celld range object. The caller has already fsynced the spool, so the only
// bucket-visible operation is the immutable final create.
func (m *Manager) AppendSnapshotFile(ctx context.Context, s cell.Scope, epoch uint64, path string, size int64) (string, string, error) {
	return m.appendLevelFile(ctx, s, epoch, 0, path, size, true)
}

// AppendLevelFile publishes a validated native spool without buffering its body.
func (m *Manager) AppendLevelFile(ctx context.Context, s cell.Scope, epoch uint64, level uint16, path string, size int64) (string, string, error) {
	return m.appendLevelFile(ctx, s, epoch, level, path, size, level == 9)
}

// File validation and multipart upload share a process-wide budget. Byte-sized
// foreground incrementals use their existing path and do not wait behind this.
var nativeFileUploads = make(chan struct{}, 2)

func (m *Manager) appendLevelFile(ctx context.Context, s cell.Scope, epoch uint64, level uint16, path string, size int64, snapshot bool) (string, string, error) {
	select {
	case nativeFileUploads <- struct{}{}:
		defer func() { <-nativeFileUploads }()
	case <-ctx.Done():
		return "", "", ctx.Err()
	}

	if !validLevel(level) {
		return "", "", fmt.Errorf("replica: invalid level %d", level)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", "", err
	}
	if info.Size() != size {
		return "", "", fmt.Errorf("replica: snapshot spool size changed: got %d want %d", info.Size(), size)
	}
	d, err := ltx.NewNativeDecoder(bufio.NewReaderSize(f, 256<<10))
	if err != nil {
		return "", "", fmt.Errorf("replica: invalid native snapshot spool: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		_, err := d.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", fmt.Errorf("replica: invalid native snapshot spool: %w", err)
		}
	}
	h := d.Header
	if snapshot && !h.IsSnapshot() {
		return "", "", fmt.Errorf("replica: snapshot spool is not a snapshot")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", "", err
	}

	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return "", "", err
	}
	defer release()
	key := fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(s, epoch), level, h.MinTXID, h.MaxTXID)
	etag, err := m.uploadFile(ctx, s, epoch, key, f, size)

	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// ListSegments returns the keys of all segments for a scope/epoch (cold path).
func (m *Manager) ListSegments(ctx context.Context, s cell.Scope, epoch uint64) ([]string, error) {
	levels, err := m.listCanonicalLevels(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, level := range celldLevels {
		out = append(out, levels[level]...)
	}
	return out, nil
}

// ListL0Segments returns the L0 (non-compacted) segment keys for a scope/epoch.
// Only canonical L0 objects are included.
func (m *Manager) ListL0Segments(ctx context.Context, s cell.Scope, epoch uint64) ([]string, error) {
	return m.ListL0SegmentsFrom(ctx, s, epoch, 0)
}

// ListL0SegmentsFrom lists canonical L0 objects whose minimum transaction ID
// is at least seek. The offset is pushed into the provider listing cursor;
// continuation pages then advance from that point instead of rescanning the
// retained prefix.
func (m *Manager) ListL0SegmentsFrom(ctx context.Context, s cell.Scope, epoch, seek uint64) ([]string, error) {
	after := ""
	if seek > 0 {
		after = celldL0Prefix(s, epoch) + fmt.Sprintf("%016x", seek)
	}
	keys, err := m.listLevelAfter(ctx, celldL0Prefix(s, epoch), after)
	if err != nil {
		return nil, err
	}
	if seek > 0 {
		filtered := keys[:0]
		for _, k := range keys {
			name := k
			if i := strings.LastIndexByte(name, '/'); i >= 0 {
				name = name[i+1:]
			}
			if sp, ok := parseSegmentSpan(name); ok && sp.start >= seek {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if !isSegmentKey(k) {
			continue
		}
		out = append(out, k)
	}
	return out, nil
}

// ListLevel returns objects in one canonical celld level. The prefix is
// fetched through the provider's continuation cursor when available.
func (m *Manager) ListLevel(ctx context.Context, s cell.Scope, epoch uint64, level uint16) ([]string, error) {
	canonicalPrefix := EpochPrefix(s, epoch) + fmt.Sprintf("%04x/", level)
	keys, err := m.listLevel(ctx, canonicalPrefix)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// listLevel uses the provider continuation token/offset when available. LTX
// restore never needs a whole-epoch LIST: each canonical level is independently
// paged, matching celld's ltx_files(level, seek) behavior.
func (m *Manager) listLevel(ctx context.Context, prefix string) ([]string, error) {
	return m.listLevelAfter(ctx, prefix, "")
}

func (m *Manager) listLevelAfter(ctx context.Context, prefix, after string) ([]string, error) {
	if pl, ok := m.B.(bucket.PagedLister); ok {
		var out []string
		for pages := 0; pages < 100000; pages++ {
			items, next, err := pl.ListPage(ctx, prefix, after, 1000)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				out = append(out, item.Key)
			}
			if next == "" {
				return out, nil
			}
			if next == after {
				return nil, fmt.Errorf("replica: non-advancing list continuation for %s", prefix)
			}
			after = next
		}
		return nil, fmt.Errorf("replica: list continuation limit exceeded for %s", prefix)
	}
	return m.B.List(ctx, prefix)
}

// AppendL1 stores one complete native LTX file at level1.
func (m *Manager) AppendL1(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) (string, string, error) {
	return m.AppendLevel(ctx, s, epoch, 1, segment)
}

// AppendL1Batch compacts its native inputs to one indexed LTX file.
func (m *Manager) AppendL1Batch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
	return m.appendNativeBatch(ctx, s, epoch, 1, segments)
}

// DeleteObject defers physical replication GC. Offline callers must explicitly
// authorize quiescent deletion with DeleteObjectWithOptions.
func (m *Manager) DeleteObject(ctx context.Context, key string) error {
	return ErrGCDeferred
}

// ReadSegment returns the raw bytes of a segment.
func (m *Manager) ReadSegment(ctx context.Context, key string) ([]byte, error) {
	data, _, err := m.resourceObjects().GetVersion(ctx, key)
	return data, err
}

// ValidateKey rejects keys that escape the expected prefix (defense in depth).
func ValidateKey(key, prefix string) error {
	if !strings.HasPrefix(key, prefix) {
		return fmt.Errorf("replica: key %q not under %q", key, prefix)
	}
	return nil
}

// Segment is a decoded replication segment (cold-path restore).
type Segment struct {
	Key    string
	Header ltx.Header
	Raw    []byte
}

// Restore reads the native cross-epoch chain at the requested epoch cut.
func (m *Manager) Restore(ctx context.Context, s cell.Scope, epoch uint64) ([]Segment, error) {
	return m.RestoreChain(ctx, s, epoch)
}
