// Package objgc garbage-collects content-addressed platform objects (worker
// bundles and versioned assets) that are no longer referenced by any worker
// version (ADR-110/ADR-111).
//
// It is a cold path (bucket List, never on the write-ack path) and uses a
// two-phase mark: an unreferenced item is first *marked* with a timestamp and
// only deleted once it has stayed unreferenced past the grace period. That
// protects an item that was just uploaded (e.g. `cellhive bundle put` or an
// asset push) but not yet referenced by a `deploy` from being deleted
// underneath the caller.
package objgc

import (
	"context"
	"log/slog"
	"time"
)

// Items is one kind of GC-able objects, identified by an opaque id.
type Items interface {
	// List returns the id of every stored item.
	List(ctx context.Context) ([]string, error)
	// Delete removes one item by id.
	Delete(ctx context.Context, id string) error
}

// Refs supplies the referenced-id set and the durable GC marks for one kind.
type Refs interface {
	Referenced(ctx context.Context) (map[string]bool, error)
	Mark(ctx context.Context, id string) (int64, bool, error)
	SetMark(ctx context.Context, id string, ms int64) error
	ClearMark(ctx context.Context, id string) error
}

// GC runs garbage-collection passes over one kind of object.
type GC struct {
	Items Items
	Refs  Refs
	// Grace is how long an unreferenced item is kept after being marked.
	Grace time.Duration
	// Now is the clock (tests override it); nil means time.Now.
	Now func() time.Time
	Log *slog.Logger
}

// Result summarizes one pass.
type Result struct {
	Total      int `json:"total"`
	Referenced int `json:"referenced"`
	Marked     int `json:"marked"`
	Deleted    int `json:"deleted"`
}

// Pass runs one collection pass. Marks are cleared for items that became
// referenced again, so an abandoned upload does not accumulate state.
func (g *GC) Pass(ctx context.Context) (Result, error) {
	var res Result
	now := g.Now
	if now == nil {
		now = time.Now
	}
	refs, err := g.Refs.Referenced(ctx)
	if err != nil {
		return res, err
	}
	ids, err := g.Items.List(ctx)
	if err != nil {
		return res, err
	}
	nowMs := now().UnixMilli()
	graceMs := g.Grace.Milliseconds()
	res.Total = len(ids)
	for _, id := range ids {
		if refs[id] {
			res.Referenced++
			if err := g.Refs.ClearMark(ctx, id); err != nil {
				return res, err
			}
			continue
		}
		marked, ok, err := g.Refs.Mark(ctx, id)
		if err != nil {
			return res, err
		}
		if !ok {
			if err := g.Refs.SetMark(ctx, id, nowMs); err != nil {
				return res, err
			}
			res.Marked++
			continue
		}
		if nowMs-marked < graceMs {
			continue
		}
		if err := g.Items.Delete(ctx, id); err != nil {
			return res, err
		}
		if err := g.Refs.ClearMark(ctx, id); err != nil {
			return res, err
		}
		res.Deleted++
		if g.Log != nil {
			g.Log.Info("gc: deleted unreferenced object", "id", id)
		}
	}
	return res, nil
}
