// Package replica stores and retrieves cell replication segments (P0.3).
//
// Segments live under cells/<scope>/ltx/e<epoch>/<name>. Append validates the
// LTX header and (for backend A) the caller's epoch before writing. Listing is
// a cold-path operation used only by restore/recovery, never by hot paths.
package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	B               bucket.Bucket
	RestoreParallel int
	resourceLocks   [64]sync.RWMutex

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

// L1Prefix is the compacted (L1) segment prefix for a scope/epoch. It is nested
// under Prefix so epoch fencing still applies, and is excluded from L0 listings.
func L1Prefix(s cell.Scope, epoch uint64) string {
	return Prefix(s, epoch) + "L1/"
}

// ManifestName is the key of the L1 manifest that points at the current
// compacted snapshot for a scope/epoch.
func ManifestName(s cell.Scope, epoch uint64) string {
	return L1Prefix(s, epoch) + "manifest.json"
}

// SnapshotStartedName marks that a full-page snapshot at txid has begun.
// Together with SnapshotDoneName it distinguishes a crash-partial snapshot from
// a legacy snapshot that never had markers (ADR-272).
func SnapshotStartedName(s cell.Scope, epoch, txid uint64) string {
	return Prefix(s, epoch) + strconv.FormatUint(txid, 10) + ".snapshot-started"
}

// SnapshotDoneName marks that every part of a full-page snapshot at txid has
// been committed. Without it a partial snapshot (crash between part uploads)
// must not be treated as a resume baseline.
func SnapshotDoneName(s cell.Scope, epoch, txid uint64) string {
	return Prefix(s, epoch) + strconv.FormatUint(txid, 10) + ".snapshot-done"
}

// MarkSnapshotStarted records that a snapshot at txid has begun.
func (m *Manager) MarkSnapshotStarted(ctx context.Context, s cell.Scope, epoch, txid uint64) error {
	_, err := m.B.Put(ctx, SnapshotStartedName(s, epoch, txid), []byte("{}"))
	return err
}

// SnapshotStarted reports whether a snapshot at txid began.
func (m *Manager) SnapshotStarted(ctx context.Context, s cell.Scope, epoch, txid uint64) (bool, error) {
	_, _, err := m.B.Get(ctx, SnapshotStartedName(s, epoch, txid))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// ClearSnapshotStarted drops the started marker after a completed snapshot
// whose done marker could not be written, so restore treats it as a complete
// legacy snapshot instead of skipping it forever.
func (m *Manager) ClearSnapshotStarted(ctx context.Context, s cell.Scope, epoch, txid uint64) error {
	return m.B.Delete(ctx, SnapshotStartedName(s, epoch, txid))
}

// MarkSnapshotDone records that the snapshot at txid is complete. Idempotent:
// an overwrite of the same marker is harmless.
func (m *Manager) MarkSnapshotDone(ctx context.Context, s cell.Scope, epoch, txid uint64) error {
	_, err := m.B.Put(ctx, SnapshotDoneName(s, epoch, txid), []byte("{}"))
	return err
}

// SnapshotDone reports whether the snapshot at txid was fully committed.
func (m *Manager) SnapshotDone(ctx context.Context, s cell.Scope, epoch, txid uint64) (bool, error) {
	_, _, err := m.B.Get(ctx, SnapshotDoneName(s, epoch, txid))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// Manifest describes the current L1 compaction for a scope/epoch: the objects
// that make up the compacted snapshot and the txid range they cover. Restore
// reads only these objects plus L0 deltas newer than MaxTxID, so a takeover
// reads a small, bounded set of objects instead of the whole L0 chain.
type Manifest struct {
	Epoch     uint64   `json:"epoch"`
	MinTxID   uint64   `json:"min_txid"`
	MaxTxID   uint64   `json:"max_txid"`
	Commit    uint32   `json:"commit"`
	PageSize  uint32   `json:"page_size"`
	Objects   []string `json:"objects"`
	CreatedMs int64    `json:"created_ms"`
}

// Covers reports whether the scope/epoch's L1 manifest covers txid, i.e. the
// bucket holds a full baseline for every write up to txid. The disk janitor uses
// it to decide that an idle owned cell file is safe to reclaim (ADR-123).
func (m *Manager) Covers(ctx context.Context, s cell.Scope, epoch, txid uint64) (bool, error) {
	man, ok, err := m.ReadManifest(ctx, s, epoch)
	if err != nil || !ok {
		return false, err
	}
	return man.MaxTxID >= txid, nil
}

// DurableWatermark returns the highest txid whose bucket chain is durably
// contiguous for scope/epoch: the L1 manifest baseline plus the contiguous L0
// spans above it (or the newest snapshot when it is above the manifest).
//
// It is the join point a restarted capture can resume from: when it equals the
// local cell txid, deltas may continue from there and no new whole-database
// snapshot is needed (ADR-272). This lists the bucket, so it is a cold-path
// operation used only when a capture is created; 0 means there is no durable
// baseline yet (a fresh cell still needs a snapshot).
func (m *Manager) DurableWatermark(ctx context.Context, s cell.Scope, epoch uint64) (uint64, error) {
	floor := uint64(0)
	man, ok, err := m.ReadManifest(ctx, s, epoch)
	if err != nil {
		return 0, err
	}
	if ok {
		floor = man.MaxTxID
	}
	keys, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return 0, err
	}
	spans := make([]segmentSpan, 0, len(keys))
	for _, key := range keys {
		name := key
		if i := strings.LastIndexByte(name, '/'); i >= 0 {
			name = name[i+1:]
		}
		// A group-committed object can contain snapshot parts (and/or deltas);
		// its name alone cannot tell, so decode the batch's headers.
		if strings.HasSuffix(name, ".batch") && !batchSpanKnown(name) {
			raw, err := m.ReadSegment(ctx, key)
			if err != nil {
				return 0, err
			}
			parts, err := ltx.Split(raw)
			if err != nil {
				return 0, fmt.Errorf("replica: durable watermark split %s: %w", key, err)
			}
			for _, part := range parts {
				h, _, derr := ltx.Decode(part)
				if derr != nil {
					return 0, fmt.Errorf("replica: durable watermark decode %s: %w", key, derr)
				}
				if h.Kind == ltx.KindSnapshot || h.Kind == ltx.KindDelta {
					spans = append(spans, segmentSpan{start: h.StartTxID, end: h.EndTxID, snapshot: h.Kind == ltx.KindSnapshot})
				}
			}
			continue
		}
		sp, ok := parseSegmentSpan(name)
		if !ok {
			continue
		}
		spans = append(spans, sp)
	}
	// A snapshot is a complete page image at its EndTxID: the newest *complete*
	// one is a newer baseline than the manifest (it re-anchors the chain). A
	// snapshot without its done marker may be missing parts and is ignored.
	var snapEnds []uint64
	for _, sp := range spans {
		if sp.snapshot && sp.end > floor {
			snapEnds = append(snapEnds, sp.end)
		}
	}
	sort.Slice(snapEnds, func(i, j int) bool { return snapEnds[i] > snapEnds[j] })
	for _, end := range snapEnds {
		ok, err := m.SnapshotDone(ctx, s, epoch, end)
		if err != nil {
			return 0, err
		}
		if ok {
			floor = end
			break
		}
	}
	if floor == 0 {
		return 0, nil
	}
	// Extend only through contiguous deltas; the first gap stops the watermark
	// (a later span cannot be restored across it).
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].start == spans[j].start {
			return spans[i].end < spans[j].end
		}
		return spans[i].start < spans[j].start
	})
	for _, sp := range spans {
		if sp.snapshot || sp.end <= floor || sp.start > floor+1 {
			continue
		}
		floor = sp.end
	}
	return floor, nil
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

// DurableWatermarkChain returns the durable contiguous watermark across the
// epoch chain up to maxEpoch. A restarted owner gets a new epoch; the previous
// epoch's snapshot/deltas are its baseline, and the new epoch's deltas continue
// the same txid sequence (ADR-272, celld epoch-chain restore).
//
// It lists the scope's LTX prefix once and groups keys by epoch. The previous
// implementation listed once per epoch and decoded every batch object, which on
// a long-lived scope (many owner epochs) made the first write after a restart
// take tens of seconds.
func (m *Manager) DurableWatermarkChain(ctx context.Context, s cell.Scope, maxEpoch uint64) (uint64, error) {
	listCtx, listSpan := telemetry.Tracer().Start(ctx, "replica.list_watermark_chain")
	keys, err := m.B.List(listCtx, s.Key()+"/ltx/")
	listSpan.SetAttributes(attribute.Int("restore.listed_objects", len(keys)))
	listSpan.End()
	if err != nil {
		return 0, err
	}
	base := s.Key() + "/ltx/"
	type epochData struct {
		spans    []segmentSpan
		manifest string
	}
	groups := map[uint64]*epochData{}
	for _, key := range keys {
		rest, ok := strings.CutPrefix(key, base)
		if !ok {
			continue
		}
		// rest is "e<epoch>/<name>" (L1 objects live under ".../L1/").
		if !strings.HasPrefix(rest, "e") {
			continue
		}
		slash := strings.IndexByte(rest, '/')
		if slash <= 1 {
			continue
		}
		n, perr := strconv.ParseUint(rest[1:slash], 10, 64)
		if perr != nil || (maxEpoch > 0 && n > maxEpoch) {
			continue
		}
		name := rest[slash+1:]
		g := groups[n]
		if g == nil {
			g = &epochData{}
			groups[n] = g
		}
		if strings.HasSuffix(name, "L1/manifest.json") {
			g.manifest = key
			continue
		}
		if strings.Contains(name, "L1/") {
			continue // compacted objects are not L0 spans
		}
		if sp, ok := parseSegmentSpan(name); ok {
			g.spans = append(g.spans, sp)
		}
	}
	epochs := make([]uint64, 0, len(groups))
	for e := range groups {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	floor := uint64(0)
	haveBase := false
	// An epoch marker (ADR-275) kills every older-epoch object that ends above
	// its cut. A fenced old owner's late tail must not extend the watermark.
	cuts := map[uint64]uint64{}
	for e, g := range groups {
		for _, sp := range g.spans {
			if sp.marker {
				if c, ok := cuts[e]; !ok || sp.end < c {
					cuts[e] = sp.end
				}
			}
		}
	}
	deadAbove := func(epoch uint64) (uint64, bool) {
		var lim uint64
		found := false
		for e, c := range cuts {
			if e > epoch && (!found || c < lim) {
				lim, found = c, true
			}
		}
		return lim, found
	}
	for _, epoch := range epochs {
		g := groups[epoch]
		if lim, ok := deadAbove(epoch); ok {
			kept := g.spans[:0]
			for _, sp := range g.spans {
				if sp.end <= lim || sp.marker {
					kept = append(kept, sp)
				}
			}
			g.spans = kept
		}
		if g.manifest != "" {
			man, ok, merr := m.ReadManifest(ctx, s, epoch)
			if merr != nil {
				return 0, merr
			}
			if ok {
				g.spans = append(g.spans, segmentSpan{start: 1, end: man.MaxTxID, snapshot: true})
			}
		}
		var snapEnds []uint64
		for _, sp := range g.spans {
			if sp.snapshot && sp.end > floor {
				snapEnds = append(snapEnds, sp.end)
			}
		}
		sort.Slice(snapEnds, func(i, j int) bool { return snapEnds[i] > snapEnds[j] })
		for _, end := range snapEnds {
			done, derr := m.SnapshotDone(ctx, s, epoch, end)
			if derr != nil {
				return 0, derr
			}
			if done {
				floor = end
				haveBase = true
				break
			}
		}
		sort.Slice(g.spans, func(i, j int) bool {
			if g.spans[i].start == g.spans[j].start {
				return g.spans[i].end < g.spans[j].end
			}
			return g.spans[i].start < g.spans[j].start
		})
		for _, sp := range g.spans {
			if sp.snapshot || sp.end <= floor || sp.start > floor+1 {
				continue
			}
			floor = sp.end
		}
	}
	if !haveBase {
		return 0, nil
	}
	return floor, nil
}

// HasCompleteSnapshot reports whether an epoch has at least one snapshot
// completion marker. Only then may older epochs be retired: a restore of the
// newest epoch starts from this snapshot.
func (m *Manager) HasCompleteSnapshot(ctx context.Context, s cell.Scope, epoch uint64) (bool, error) {
	keys, err := m.B.List(ctx, Prefix(s, epoch))
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		if strings.HasSuffix(key, ".snapshot-done") {
			segs, err := m.Restore(ctx, s, epoch)
			if err != nil {
				return false, err
			}
			raws := make([][]byte, len(segs))
			for i, seg := range segs {
				raws[i] = seg.Raw
			}
			if err := restore.ValidateChain(raws); err != nil {
				return false, fmt.Errorf("replica: epoch GC requires independently restorable chain: %w", err)
			}
			return true, nil
		}
	}
	return false, nil
}

// RetireEpochs deletes epoch prefixes older than current-keepPrevious after the
// current epoch has opened with a complete snapshot (celld epoch GC). Restore
// only ever reads the newest snapshot base, so older prefixes are dead weight;
// keeping the previous epoch makes an in-flight restore or a rollback safe.
func (m *Manager) RetireEpochs(ctx context.Context, s cell.Scope, current uint64, keepPrevious int) (int, error) {
	ok, err := m.HasCompleteSnapshot(ctx, s, current)
	if err != nil || !ok {
		return 0, err
	}
	if keepPrevious < 0 {
		keepPrevious = 0
	}
	keepFrom := uint64(0)
	if uint64(keepPrevious) < current {
		keepFrom = current - uint64(keepPrevious)
	}
	epochs, err := m.Epochs(ctx, s, current)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, epoch := range epochs {
		if epoch >= keepFrom {
			continue
		}
		keys, lerr := m.B.List(ctx, Prefix(s, epoch))
		if lerr != nil {
			return deleted, lerr
		}
		for _, key := range keys {
			if derr := m.B.Delete(ctx, key); derr != nil && !errors.Is(derr, bucket.ErrNotFound) {
				return deleted, derr
			}
			deleted++
		}
	}
	return deleted, nil
}

// lastBase returns the index of the newest epoch with a complete restore base,
// or -1 when no epoch has a snapshot/manifest.
func lastBase(epochs []uint64, hasBase map[uint64]bool) int {
	for i := len(epochs) - 1; i >= 0; i-- {
		if hasBase[epochs[i]] {
			return i
		}
	}
	return -1
}

// RestoreChain reads the segments of every epoch up to maxEpoch (oldest first),
// so a new epoch that only contains deltas can be restored on top of its
// predecessor's snapshot. Cold path; the caller's restore planner selects the
// newest complete snapshot and validates delta contiguity.
func (m *Manager) RestoreChain(ctx context.Context, s cell.Scope, maxEpoch uint64) ([]Segment, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "replica.restore_chain")
	defer span.End()
	span.SetAttributes(attribute.String("cell.scope", s.String()))
	// One listing for the whole chain: cold hydrate used to list once for
	// Epochs and once per epoch for L0. restoreOnceKeys below never lists.
	listCtx, listSpan := telemetry.Tracer().Start(ctx, "replica.list_chain")
	keys, err := m.B.List(listCtx, s.Key()+"/ltx/")
	listSpan.SetAttributes(attribute.Int("restore.listed_objects", len(keys)))
	listSpan.End()
	if err != nil {
		return nil, err
	}
	base := s.Key() + "/ltx/e"
	byEpoch := map[uint64][]string{}
	hasBase := map[uint64]bool{}
	var epochs []uint64
	for _, key := range keys {
		rest, ok := strings.CutPrefix(key, base)
		if !ok {
			continue
		}
		slash := strings.IndexByte(rest, '/')
		if slash <= 0 {
			continue
		}
		e, perr := strconv.ParseUint(rest[:slash], 10, 64)
		if perr != nil || (maxEpoch > 0 && e > maxEpoch) {
			continue
		}
		// An epoch whose L0 was fully GC'd after compaction may contain only
		// L1/manifest.json and L1 objects. It is still a complete restore base.
		// Handle the manifest before isSegmentKey filters metadata keys out.
		if rest[slash+1:] == "L1/manifest.json" {
			if _, seen := byEpoch[e]; !seen {
				epochs = append(epochs, e)
				byEpoch[e] = nil
			}
			hasBase[e] = true
			continue
		}
		if !isSegmentKey(key) {
			continue
		}
		if _, seen := byEpoch[e]; !seen {
			epochs = append(epochs, e)
			byEpoch[e] = nil
		}
		if strings.HasPrefix(rest[slash+1:], "L1/") {
			continue
		}
		if strings.HasSuffix(key, ".snapshot") || strings.HasSuffix(key, ".snapshot-done") {
			hasBase[e] = true
		}
		byEpoch[e] = append(byEpoch[e], key)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	// Restore only needs the newest complete baseline and the epochs above it.
	// Older epochs are dead weight; restoring every historical epoch made a
	// cron/invocation cell with 100+ epochs exceed the hydrate deadline.
	start := lastBase(epochs, hasBase)
	if start < 0 {
		start = 0
	}
	loaded := map[uint64][]Segment{}
	var firstErr error
	for {
		var out []Segment
		seen := map[string]bool{}
		for _, epoch := range epochs[start:] {
			segs, ok := loaded[epoch]
			if !ok {
				var err error
				segs, err = m.restoreOnceKeys(ctx, s, epoch, byEpoch[epoch])
				if err != nil {
					return nil, err
				}
				loaded[epoch] = segs
			}
			for _, sg := range segs {
				if !seen[sg.Key] {
					seen[sg.Key] = true
					out = append(out, sg)
				}
			}
		}
		// Marker-only epochs still rely on their predecessor; preserve the
		// legacy no-base result for callers which explicitly handle no data.
		if lastBase(epochs, hasBase) < 0 {
			return out, nil
		}
		raws := make([][]byte, len(out))
		for i, seg := range out {
			raws[i] = seg.Raw
		}
		_, validateSpan := telemetry.Tracer().Start(ctx, "replica.validate_chain")
		validateErr := restore.ValidateChain(raws)
		validateSpan.End()
		if err := validateErr; err == nil {
			return out, nil
		} else if firstErr == nil {
			firstErr = err
		}
		if start == 0 {
			return nil, firstErr
		}
		previous := lastBase(epochs[:start], hasBase)
		if previous < 0 {
			previous = 0
		}
		start = previous
	}
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
	switch {
	case strings.HasSuffix(name, ".snapshot"):
		base := strings.TrimSuffix(name, ".snapshot")
		if i := strings.Index(base, ".p"); i > 0 {
			base = base[:i]
		}
		n, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			return out, false
		}
		out.start, out.end, out.snapshot = n, n, true
		return out, true
	case strings.HasSuffix(name, ".batch"):
		base := strings.TrimSuffix(name, ".batch")
		for _, suffix := range []string{".d", ".m", ".s"} {
			base = strings.TrimSuffix(base, suffix)
		}
		if i := strings.Index(base, ".p"); i > 0 {
			n, err := strconv.ParseUint(base[:i], 10, 64)
			if err != nil {
				return out, false
			}
			out.start, out.end, out.snapshot = n, n, true
			return out, true
		}
		return parseSegmentRange(base)
	case strings.HasSuffix(name, ".ltx"):
		return parseSegmentRange(strings.TrimSuffix(name, ".ltx"))
	case strings.HasSuffix(name, ".marker"):
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".marker"), 10, 64)
		if err != nil || n == 0 {
			return out, false
		}
		out.start, out.end, out.marker = n, n-1, true
		return out, true
	}
	return out, false
}

// batchSpanKnown reports whether a .batch object's name alone says what it
// holds: ".d.batch" is all deltas and ".s.batch" is snapshot parts of one
// watermark. Legacy and ".m.batch" names need their headers decoded.
func batchSpanKnown(name string) bool {
	return strings.HasSuffix(name, ".d.batch") || strings.HasSuffix(name, ".s.batch")
}

// isSegmentKey reports whether an object under the epoch prefix is an LTX
// segment (markers such as "<txid>.snapshot-done" are not).
func isSegmentKey(key string) bool {
	name := key
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	return strings.HasSuffix(name, ".snapshot") ||
		strings.HasSuffix(name, ".ltx") ||
		strings.HasSuffix(name, ".batch") ||
		strings.HasSuffix(name, ".link") ||
		strings.HasSuffix(name, ".marker")
}

func parseSegmentRange(base string) (segmentSpan, bool) {
	var out segmentSpan
	i := strings.IndexByte(base, '-')
	if i <= 0 || i == len(base)-1 {
		return out, false
	}
	start, err := strconv.ParseUint(base[:i], 10, 64)
	if err != nil {
		return out, false
	}
	end, err := strconv.ParseUint(base[i+1:], 10, 64)
	if err != nil || end < start {
		return out, false
	}
	out.start, out.end = start, end
	return out, true
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
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return "", "", err
	}
	defer release()
	h, _, err := ltx.Decode(segment)
	if err != nil {
		return "", "", fmt.Errorf("replica: invalid segment: %w", err)
	}
	if h.Epoch != epoch {
		return "", "", fmt.Errorf("replica: segment epoch %d != requested %d", h.Epoch, epoch)
	}
	key := Prefix(s, epoch) + ltx.SegmentName(h)
	etag, err := m.putImmutableSegment(ctx, s, epoch, key, segment)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// AppendBatch concatenates several segments into one object (group commit) and
// returns its key and etag.
func (m *Manager) AppendBatch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
	release, guardErr := m.resourceWriteGuard(ctx, s, epoch)
	if guardErr != nil {
		return "", "", guardErr
	}
	defer release()
	if len(segments) == 0 {
		return "", "", fmt.Errorf("replica: empty batch")
	}
	var first, last ltx.Header
	var buf []byte
	for i, seg := range segments {
		h, _, err := ltx.Decode(seg)
		if err != nil {
			return "", "", fmt.Errorf("replica: invalid segment in batch: %w", err)
		}
		if h.Epoch != epoch {
			return "", "", fmt.Errorf("replica: segment epoch %d != requested %d", h.Epoch, epoch)
		}
		if i == 0 {
			first = h
		}
		last = h
		buf = append(buf, seg...)
	}
	name := ltx.BatchName(first, last)
	if len(segments) == 1 {
		name = ltx.SegmentName(first)
	}
	key := Prefix(s, epoch) + name
	etag, err := m.putImmutableSegment(ctx, s, epoch, key, buf)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// MarkEpoch durably records that epoch starts at cut+1. It is idempotent and
// written before the new epoch's capture can serve (ADR-275).
func (m *Manager) MarkEpoch(ctx context.Context, s cell.Scope, epoch, cut uint64) (string, error) {
	h := ltx.Header{Kind: ltx.KindMarker, Epoch: epoch, StartTxID: cut + 1, EndTxID: cut}
	seg := ltx.Encode(h, nil)
	key := Prefix(s, epoch) + ltx.SegmentName(h)
	if _, err := m.B.Put(ctx, key, seg); err != nil {
		return "", err
	}
	return key, nil
}

// ListSegments returns the keys of all segments for a scope/epoch (cold path).
func (m *Manager) ListSegments(ctx context.Context, s cell.Scope, epoch uint64) ([]string, error) {
	keys, err := m.B.List(ctx, Prefix(s, epoch))
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// ListL0Segments returns the L0 (non-compacted) segment keys for a scope/epoch.
// L1 objects and the manifest are excluded.
func (m *Manager) ListL0Segments(ctx context.Context, s cell.Scope, epoch uint64) ([]string, error) {
	keys, err := m.ListSegments(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	l1 := L1Prefix(s, epoch)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, l1) || !isSegmentKey(k) {
			continue
		}
		out = append(out, k)
	}
	return out, nil
}

// AppendL1 stores a compacted snapshot part under the L1 prefix.
func (m *Manager) AppendL1(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) (string, string, error) {
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return "", "", err
	}
	defer release()
	h, _, err := ltx.Decode(segment)
	if err != nil {
		return "", "", fmt.Errorf("replica: invalid L1 segment: %w", err)
	}
	if h.Epoch != epoch {
		return "", "", fmt.Errorf("replica: L1 segment epoch %d != requested %d", h.Epoch, epoch)
	}
	key := L1Prefix(s, epoch) + ltx.SegmentName(h)
	etag, err := m.putImmutableSegment(ctx, s, epoch, key, segment)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// PutManifest writes the L1 manifest for a scope/epoch.
func (m *Manager) PutManifest(ctx context.Context, s cell.Scope, epoch uint64, man Manifest) error {
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return err
	}
	defer release()
	man.Epoch = epoch
	if man.CreatedMs == 0 {
		man.CreatedMs = time.Now().UnixMilli()
	}
	data, err := json.Marshal(man)
	if err != nil {
		return err
	}
	_, err = m.putResourceObject(ctx, s, epoch, ManifestName(s, epoch), data)
	return err
}

// ReadManifest returns the L1 manifest for a scope/epoch. ok is false when no
// manifest exists, meaning the cell has never been compacted.
func (m *Manager) ReadManifest(ctx context.Context, s cell.Scope, epoch uint64) (Manifest, bool, error) {
	data, _, err := m.B.Get(ctx, ManifestName(s, epoch))
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return Manifest{}, false, nil
		}
		return Manifest{}, false, err
	}
	var man Manifest
	if err := json.Unmarshal(data, &man); err != nil {
		return Manifest{}, false, fmt.Errorf("replica: decode manifest: %w", err)
	}
	return man, true, nil
}

// DeleteObject removes a replication object (GC of superseded L0/L1 objects). A
// missing key is not an error.
func (m *Manager) DeleteObject(ctx context.Context, key string) error {
	return m.B.Delete(ctx, key)
}

// ReadSegment returns the raw bytes of a segment.
func (m *Manager) ReadSegment(ctx context.Context, key string) ([]byte, error) {
	data, _, err := m.B.Get(ctx, key)
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

// Restore reads and orders the segments needed to rebuild a scope/epoch (cold
// path). It retries if compaction replaces the manifest between the read and
// the L0 listing, so a concurrent GC cannot leave the returned chain pointing at
// a deleted L1 object.
func (m *Manager) Restore(ctx context.Context, s cell.Scope, epoch uint64) ([]Segment, error) {
	for attempt := 0; attempt < 3; attempt++ {
		before, beforeOK, err := m.ReadManifest(ctx, s, epoch)
		if err != nil {
			return nil, err
		}
		segs, err := m.restoreOnce(ctx, s, epoch)
		if err != nil {
			return nil, err
		}
		after, afterOK, err := m.ReadManifest(ctx, s, epoch)
		if err != nil {
			return nil, err
		}
		if beforeOK == afterOK && (!afterOK || reflect.DeepEqual(before, after)) {
			return segs, nil
		}
	}
	return nil, fmt.Errorf("replica: manifest changed during restore")
}

// skipPartialSnapshot reports whether a crash-partial snapshot (started marker
// without a done marker) must be ignored by restore. A legacy snapshot with no
// markers is assumed complete, so pre-marker chains still restore.
func (m *Manager) skipPartialSnapshot(ctx context.Context, s cell.Scope, epoch, txid uint64, cache map[uint64]bool) (bool, error) {
	if skip, ok := cache[txid]; ok {
		return skip, nil
	}
	started, err := m.SnapshotStarted(ctx, s, epoch, txid)
	if err != nil {
		return false, err
	}
	if !started {
		cache[txid] = false
		return false, nil
	}
	done, err := m.SnapshotDone(ctx, s, epoch, txid)
	if err != nil {
		return false, err
	}
	skip := !done
	cache[txid] = skip
	return skip, nil
}

// restoreOnce reads and orders the segments needed to rebuild a scope/epoch
// (cold path). When an L1 manifest exists it reads only the compacted snapshot
// objects plus L0 deltas newer than the manifest's MaxTxID; otherwise it reads
// the whole L0 chain. Ordering: snapshot(s) by EndTxID, then link markers, then
// deltas by StartTxID. Restore wraps it with a manifest-change retry.
// restoreOnce is the per-epoch form used by diagnostics; RestoreChain passes a
// pre-listed key set so a cold hydration lists the scope only once.
func (m *Manager) restoreOnce(ctx context.Context, s cell.Scope, epoch uint64) ([]Segment, error) {
	l0, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	return m.restoreOnceKeys(ctx, s, epoch, l0)
}

func (m *Manager) restoreOnceKeys(ctx context.Context, s cell.Scope, epoch uint64, l0 []string) ([]Segment, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "replica.restore_epoch")
	defer span.End()
	span.SetAttributes(attribute.String("cell.scope", s.String()))
	fctx, fspan := telemetry.Tracer().Start(ctx, "replica.restore_fence")
	fence, _, ferr := m.readResourceFence(fctx, s)
	fspan.End()
	if ferr != nil {
		return nil, ferr
	}
	if fence.Deleted || (fence.JobID != "" && epoch <= fence.Epoch) {
		return nil, nil
	}
	mctx, mspan := telemetry.Tracer().Start(ctx, "replica.restore_manifest")
	man, ok, err := m.ReadManifest(mctx, s, epoch)
	mspan.End()
	if err != nil {
		return nil, err
	}
	l1Set := map[string]bool{}
	var keys []string
	var floor uint64
	if ok {
		for _, k := range man.Objects {
			if l1Set[k] {
				continue
			}
			l1Set[k] = true
			keys = append(keys, k)
		}
		floor = man.MaxTxID
	}
	for _, key := range l0 {
		name := key[strings.LastIndexByte(key, '/')+1:]
		span, known := parseSegmentSpan(name)
		// A trusted manifest already covers these objects. Filtering after
		// download wastes one remote GET per obsolete L0 file. Keep epoch
		// fences and unknown/mixed batches: their names do not prove that all
		// contents are covered. A span crossing the floor must stay intact.
		if strings.HasSuffix(name, ".batch") && !batchSpanKnown(name) {
			known = false
		}
		if floor > 0 && known && !span.marker && span.end <= floor {
			continue
		}
		keys = append(keys, key)
	}

	// Fetch objects with a bounded worker pool; decoding and ordering stay
	// deterministic. Do not spawn one goroutine per key: a slow COS List/GET can
	// leave hundreds of goroutines parked on a semaphore (observed as an OOM
	// after the full replacement).
	parallel := m.RestoreParallel
	if parallel <= 0 {
		parallel = 4
	}
	if parallel > len(keys) {
		parallel = len(keys)
	}
	fetchCtx, fetchSpan := telemetry.Tracer().Start(ctx, "replica.download_objects")
	fetchSpan.SetAttributes(attribute.Int("restore.objects", len(keys)), attribute.Int("restore.parallel", parallel))
	raws := make([][]byte, len(keys))
	var wg sync.WaitGroup
	var fetchErr error
	var fetchMu sync.Mutex
	jobs := make(chan int)
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				objCtx, objSpan := telemetry.Tracer().Start(fetchCtx, "replica.download_object")
				raw, err := m.ReadSegment(objCtx, keys[i])
				objSpan.SetAttributes(attribute.Int("restore.bytes", len(raw)))
				if err != nil {
					objSpan.RecordError(err)
				}
				objSpan.End()
				if err != nil {
					fetchMu.Lock()
					if fetchErr == nil {
						fetchErr = err
					}
					fetchMu.Unlock()
					continue
				}
				raws[i] = raw
			}
		}()
	}
	for i := range keys {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			fetchSpan.End()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	fetchSpan.End()
	if fetchErr != nil {
		return nil, fetchErr
	}
	ctx, decodeSpan := telemetry.Tracer().Start(ctx, "replica.decode_objects")
	decodedParts, duplicateParts := 0, 0
	defer func() {
		decodeSpan.SetAttributes(attribute.Int("restore.decoded_segments", decodedParts), attribute.Int("restore.duplicate_segments", duplicateParts))
		decodeSpan.End()
	}()
	segs := make([]Segment, 0, len(keys))
	seen := map[string]bool{}
	partialSnapshots := map[uint64]bool{}
	for i, k := range keys {
		raw := raws[i]
		parts, err := ltx.Split(raw)
		if err != nil {
			return nil, fmt.Errorf("replica: corrupt object %s: %w", k, err)
		}
		decodedParts += len(parts)
		for i, part := range parts {
			h, _, err := ltx.Decode(part)
			if err != nil {
				return nil, fmt.Errorf("replica: corrupt segment in %s: %w", k, err)
			}
			if h.Kind == ltx.KindSnapshot {
				skip, serr := m.skipPartialSnapshot(ctx, s, epoch, h.EndTxID, partialSnapshots)
				if serr != nil {
					return nil, serr
				}
				if skip {
					continue
				}
			}
			// Recovery re-uploads a follower's single segments, which may also be
			// present inside an owner-uploaded batch. Keep one logical copy.
			if id := h.ID(); seen[id] {
				duplicateParts++
				continue
			} else {
				seen[id] = true
			}
			// An L0 delta crossing the compaction floor still contains newer
			// transactions. Keep it so restore can reject an ambiguous overlap
			// instead of silently losing its tail.
			if !l1Set[k] && floor > 0 && h.EndTxID <= floor && h.Kind != ltx.KindMarker {
				continue
			}
			segs = append(segs, Segment{Key: fmt.Sprintf("%s#%d", k, i), Header: h, Raw: part})
		}
	}
	sort.Slice(segs, func(i, j int) bool {
		ri, rj := rank(segs[i].Header), rank(segs[j].Header)
		if ri != rj {
			return ri < rj
		}
		return segs[i].Header.StartTxID < segs[j].Header.StartTxID
	})
	return segs, nil
}

func rank(h ltx.Header) int {
	switch h.Kind {
	case ltx.KindSnapshot:
		return 0
	case ltx.KindLink:
		return 1
	case ltx.KindMarker:
		return 1
	default:
		return 2
	}
}
