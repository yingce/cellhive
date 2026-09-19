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
	"sort"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// Manager reads and writes replication segments for cells.
type Manager struct {
	B bucket.Bucket
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

// LatestEpoch returns the highest epoch that has any replica object for the
// scope (optionally bounded by maxEpoch, 0 = unbounded). ok is false when the
// scope has no replica objects. It lists the bucket, so it is a cold-path
// operation for restore/takeover only.
func (m *Manager) LatestEpoch(ctx context.Context, s cell.Scope, maxEpoch uint64) (uint64, bool, error) {
	keys, err := m.B.List(ctx, s.Key()+"/ltx/")
	if err != nil {
		return 0, false, err
	}
	base := s.Key() + "/ltx/e"
	var best uint64
	found := false
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, base)
		if !ok {
			continue
		}
		i := strings.IndexByte(rest, '/')
		if i <= 0 {
			continue
		}
		n, err := strconv.ParseUint(rest[:i], 10, 64)
		if err != nil {
			continue
		}
		if maxEpoch > 0 && n > maxEpoch {
			continue
		}
		if n > best {
			best, found = n, true
		}
	}
	return best, found, nil
}

// Append validates and stores a segment, returning its object key and etag.
func (m *Manager) Append(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) (string, string, error) {
	h, _, err := ltx.Decode(segment)
	if err != nil {
		return "", "", fmt.Errorf("replica: invalid segment: %w", err)
	}
	if h.Epoch != epoch {
		return "", "", fmt.Errorf("replica: segment epoch %d != requested %d", h.Epoch, epoch)
	}
	key := Prefix(s, epoch) + ltx.SegmentName(h)
	etag, err := m.B.Put(ctx, key, segment)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// AppendBatch concatenates several segments into one object (group commit) and
// returns its key and etag.
func (m *Manager) AppendBatch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
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
	etag, err := m.B.Put(ctx, key, buf)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
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
		if strings.HasPrefix(k, l1) {
			continue
		}
		out = append(out, k)
	}
	return out, nil
}

// AppendL1 stores a compacted snapshot part under the L1 prefix.
func (m *Manager) AppendL1(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) (string, string, error) {
	h, _, err := ltx.Decode(segment)
	if err != nil {
		return "", "", fmt.Errorf("replica: invalid L1 segment: %w", err)
	}
	if h.Epoch != epoch {
		return "", "", fmt.Errorf("replica: L1 segment epoch %d != requested %d", h.Epoch, epoch)
	}
	key := L1Prefix(s, epoch) + ltx.SegmentName(h)
	etag, err := m.B.Put(ctx, key, segment)
	if err != nil {
		return "", "", err
	}
	return key, etag, nil
}

// PutManifest writes the L1 manifest for a scope/epoch.
func (m *Manager) PutManifest(ctx context.Context, s cell.Scope, epoch uint64, man Manifest) error {
	man.Epoch = epoch
	if man.CreatedMs == 0 {
		man.CreatedMs = time.Now().UnixMilli()
	}
	data, err := json.Marshal(man)
	if err != nil {
		return err
	}
	_, err = m.B.Put(ctx, ManifestName(s, epoch), data)
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

// SegmentKind extracts the LTX kind for a raw segment (used by restore).
func SegmentKind(segment []byte) (ltx.Kind, error) {
	h, _, err := ltx.Decode(segment)
	if err != nil {
		return 0, err
	}
	return h.Kind, nil
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
// path). When an L1 manifest exists it reads only the compacted snapshot objects
// plus L0 deltas newer than the manifest's MaxTxID; otherwise it reads the whole
// L0 chain. Ordering: snapshot(s) by EndTxID, then link markers, then deltas by
// StartTxID. This is the basis for takeover restore (docs/cell-protocol.md §6).
func (m *Manager) Restore(ctx context.Context, s cell.Scope, epoch uint64) ([]Segment, error) {
	man, ok, err := m.ReadManifest(ctx, s, epoch)
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
	l0, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	keys = append(keys, l0...)

	segs := make([]Segment, 0, len(keys))
	seen := map[string]bool{}
	for _, k := range keys {
		raw, err := m.ReadSegment(ctx, k)
		if err != nil {
			return nil, err
		}
		parts, err := ltx.Split(raw)
		if err != nil {
			return nil, fmt.Errorf("replica: corrupt object %s: %w", k, err)
		}
		for i, part := range parts {
			h, _, err := ltx.Decode(part)
			if err != nil {
				return nil, fmt.Errorf("replica: corrupt segment in %s: %w", k, err)
			}
			// Recovery re-uploads a follower's single segments, which may also be
			// present inside an owner-uploaded batch. Keep one logical copy.
			if id := h.ID(); seen[id] {
				continue
			} else {
				seen[id] = true
			}
			// L0 segments at or below the compaction floor are already folded
			// into the L1 snapshot.
			if !l1Set[k] && floor > 0 && h.StartTxID <= floor {
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
	default:
		return 2
	}
}
