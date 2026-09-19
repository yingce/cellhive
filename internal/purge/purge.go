// Package purge performs the data-side cleanup of a deleted namespace/worker:
// the local cell handles on this node plus the bucket prefixes that hold them.
// It is idempotent and resumable so the control-plane purge loop can retry it
// across nodes and across process restarts (ADR-142).
package purge

import (
	"context"
	"fmt"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

// LocalCells is the subset of cellstore.Store the purger needs.
type LocalCells interface {
	// DeleteNamespace drains and drops every local cell of a namespace.
	DeleteNamespace(ctx context.Context, ns string) error
	// ForgetPrefix drains and drops every local cell whose scope has the prefix.
	ForgetPrefix(ctx context.Context, prefix string) (int, error)
}

// Purger cleans the data side of one purge job.
type Purger struct {
	Bucket bucket.Bucket
	Cells  LocalCells
	// MaxDeletes bounds deletions per pass across all targets, so a huge
	// namespace cannot block a single tick; Run returns done=false and the loop
	// retries until it drains.
	MaxDeletes int
}

func (p *Purger) budget() int {
	if p.MaxDeletes <= 0 {
		return 500
	}
	return p.MaxDeletes
}

// target is one bucket directory plus an optional key filter. Filters exist
// because a DO scope is "<ns>/__do__/<worker>~<class>~shard<N>" (ADR-083/135):
// the worker boundary is inside a path segment, and not every bucket backend can
// list by a partial segment (the filesystem bucket needs a real directory).
type target struct {
	dir   string
	match func(string) bool
}

// Run cleans the data for (ns, worker). worker=="" is an app-level delete (every
// resource plus DO storage); otherwise only the worker's DO storage and its
// assets are removed — KV/D1/R2/Queue resources outlive a worker, like CF.
//
// done=false means at least one target still has objects; the caller keeps the
// purge job pending and retries. Errors are returned for the caller to record.
func (p *Purger) Run(ctx context.Context, ns, worker string) (bool, error) {
	if ns == "" || strings.ContainsAny(ns, "/\\") {
		return false, fmt.Errorf("purge: invalid namespace %q", ns)
	}
	if worker == "" {
		if p.Cells != nil {
			if err := p.Cells.DeleteNamespace(ctx, ns); err != nil {
				return false, err
			}
		}
		return p.delete(ctx, []target{
			{dir: "cells/" + ns + "/"},
			{dir: "assets/" + ns + "/"},
		})
	}
	if strings.ContainsAny(worker, "/\\") {
		return false, fmt.Errorf("purge: invalid worker %q", worker)
	}
	if p.Cells != nil {
		if _, err := p.Cells.ForgetPrefix(ctx, ns+"/__do__/"+worker+"~"); err != nil {
			return false, err
		}
	}
	return p.delete(ctx, []target{
		{
			dir: "cells/" + ns + "/__do__/",
			match: func(key string) bool {
				rel := strings.TrimPrefix(key, "cells/"+ns+"/__do__/")
				return strings.HasPrefix(rel, worker+"~")
			},
		},
		{dir: "assets/" + ns + "/" + worker + "/"},
	})
}

// delete removes matching objects from the targets, at most budget() per call.
// done=false means work remains.
func (p *Purger) delete(ctx context.Context, targets []target) (bool, error) {
	if p.Bucket == nil {
		return true, nil
	}
	budget := p.budget()
	for _, tg := range targets {
		st := p.store(tg.dir)
		if st == nil {
			return false, fmt.Errorf("purge: no owner for prefix %q", tg.dir)
		}
		// Walk the prefix in cursor pages so a huge namespace is not materialised
		// (ADR-169). Deletes are monotonic, so the next page's exclusive cursor
		// stays valid even as earlier keys disappear.
		after := ""
		for {
			items, next, err := st.ListPage(ctx, tg.dir, after, 1000)
			if err != nil {
				return false, err
			}
			if len(items) == 0 {
				break
			}
			for _, it := range items {
				if tg.match != nil && !tg.match(it.Key) {
					continue
				}
				if budget <= 0 {
					return false, nil
				}
				if err := st.Delete(ctx, it.Key); err != nil {
					return false, err
				}
				budget--
			}
			if next == "" {
				break
			}
			after = next
		}
	}
	// Every target was walked to the end within budget.
	return true, nil
}

// store opens the reserved prefix with its owner (objectstore enforces the
// registry, so only the matching owner can write/delete).
func (p *Purger) store(raw string) *objectstore.Objects {
	switch {
	case strings.HasPrefix(raw, objectstore.PrefixCells):
		return objectstore.NewOwned(p.Bucket, objectstore.PrefixCells, objectstore.OwnerReplica)
	case strings.HasPrefix(raw, objectstore.PrefixAssets):
		return objectstore.NewOwned(p.Bucket, objectstore.PrefixAssets, objectstore.OwnerArtifacts)
	default:
		return nil
	}
}
