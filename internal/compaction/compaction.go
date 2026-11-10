// Package compaction promotes native LTX ranges between adjacent levels.
// Complete snapshots live at L9; incremental compaction does not rebuild them.
//
// Compaction is a cold-path operation: it Lists object keys. It must never run
// on a write-ack hot path.
package compaction

import (
	"context"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

// Options controls when and how a compaction runs.
type Options struct {
	// MinSegments and MinBytes gate the fold: compaction runs only when the
	// uncovered L0 tail has at least MinSegments segments, or its raw size is at least
	// MinBytes. The thresholds are an OR so either a long or a large chain
	// triggers a fold.
	MinSegments int
	MinBytes    int64
	// MinObjects is an OR threshold on uncovered source objects.
	// One native range object may span multiple TXIDs. Zero disables it.
	MinObjects int
	// MaxInputs and MaxInputBytes bound each promotion (defaults 256 and 64 MiB).
	// An indivisible source exceeding the byte limit is streamed alone.
	MaxInputs     int
	MaxInputBytes int64
	// SnapshotMaxBytes bounds logical database size before L9 payload reads.
	// Nonpositive values use the 80 MiB default; larger explicit budgets are allowed.
	SnapshotMaxBytes int64
	// Verify rechecks current ownership before reading inputs and publication.
	Verify func(context.Context) error
}

// Result reports what a compaction did.
type Result struct {
	Compacted  bool
	Skipped    string
	Inputs     int
	InputBytes int64
	NewL0      int
	Parts      int
	MinTxID    uint64
	MaxTxID    uint64
	Commit     uint32
	Epoch      uint64
}

// Compact promotes uncovered native ranges from L0 into L1. Source objects
// remain immutable and retained, as in celld additive compaction.
func Compact(ctx context.Context, m *replica.Manager, s cell.Scope, epoch uint64, opts Options) (Result, error) {
	return CompactLevel(ctx, m, s, epoch, 1, opts)
}

func canonicalHex(s string) bool {
	if len(s) != 16 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
