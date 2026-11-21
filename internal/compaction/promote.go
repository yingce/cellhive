package compaction

import (
	"context"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

const (
	// Upload-path bars. L1 starts at 256 whole objects or 64MiB stored.
	// The byte bar only opens the batch: the batch still takes whole objects,
	// at most DefaultMaxInputs, and publishes one LTX.
	L1MinObjects = 256
	L1MinBytes   = 64 << 20
)

// Cue is the uncovered L0 tail for an L1 promotion.
// One native object counts as one segment.
type Cue struct {
	Objects  int
	Segments int
	Bytes    int64
}

// PromotePolicy is the count and byte bar for the upload-triggered path.
// A zero field does not participate. Promotion runs when any set bar is met.
type PromotePolicy struct {
	MinObjects  int
	MinSegments int
	MinBytes    int64
}

func L1PromotePolicy(minObjects int, minBytes int64) PromotePolicy {
	return PromotePolicy{MinObjects: minObjects, MinBytes: minBytes}
}

// ShouldPromote reports whether cue meets a count or byte bar.
func ShouldPromote(cue Cue, p PromotePolicy) bool {
	if cue.Objects <= 0 && cue.Segments <= 0 && cue.Bytes <= 0 {
		return false
	}
	if p.MinObjects > 0 && cue.Objects >= p.MinObjects {
		return true
	}
	if p.MinSegments > 0 && cue.Segments >= p.MinSegments {
		return true
	}
	if p.MinBytes > 0 && cue.Bytes >= p.MinBytes {
		return true
	}
	return false
}

// UploadCue is the owner's L0 file count and stored bytes for this epoch.
// The scheduler uses it to decide an L1 promotion. It does not list the bucket.
func UploadCue(m *replica.Manager, s cell.Scope, epoch uint64) Cue {
	if m == nil {
		return Cue{}
	}
	n, bytes := m.L0Pending(s, epoch)
	return Cue{Objects: n, Bytes: bytes}
}

// UncoveredCue measures the source tail CompactLevel would read for destLevel.
func UncoveredCue(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, destLevel uint16) (Cue, error) {
	selected, _, err := planLevel(ctx, m, s, epoch, destLevel)
	if err != nil || len(selected) == 0 {
		return Cue{}, err
	}
	infos, err := m.ListLevelInfos(ctx, s, epoch, destLevel-1)
	if err != nil {
		return Cue{}, err
	}
	byKey := make(map[string]int64, len(infos))
	for _, info := range infos {
		byKey[info.Key] = info.Size
	}
	cue := Cue{Objects: len(selected), Segments: len(selected)}
	for _, f := range selected {
		if sz := byKey[f.key]; sz > 0 {
			cue.Bytes += sz
		}
	}
	return cue, nil
}
