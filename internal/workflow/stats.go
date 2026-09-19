package workflow

import (
	"context"

	"cellhive/internal/cellstore"
)

// StatsOptions tunes a workflow stats read (ADR-157).
type StatsOptions struct {
	// Exact counts instances (and their status breakdown) with COUNT(*), which
	// scans the instances table. Without it only the dbstat estimate is given.
	Exact bool
	// MaxEstimatePages guards the dbstat estimate; 0 uses the default.
	MaxEstimatePages int
}

// Stats is the operator-facing summary of a workflow definition's instance
// cell. It reads metadata and, at most, the dbstat page structure — never step
// results or payloads.
type Stats struct {
	cellstore.DiskStats
	// InstancesEstimate is the leaf-cell count from dbstat (approximate).
	InstancesEstimate int            `json:"instances_estimate"`
	ByStatus          map[string]int `json:"by_status,omitempty"`
	Instances         int            `json:"instances,omitempty"`
	Exact             bool           `json:"exact"`
	Note              string         `json:"note,omitempty"`
}

// Stats summarizes a workflow definition's cells (ADR-157).
func (s *Store) Stats(ctx context.Context, ns, name string, opts StatsOptions) (Stats, error) {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return Stats{}, err
	}
	ds, err := c.DiskStats(ctx)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{DiskStats: ds, Exact: opts.Exact}
	if opts.Exact {
		rows, err := c.DB.QueryContext(ctx, `SELECT status, COUNT(1) FROM instances GROUP BY status`)
		if err != nil {
			return st, err
		}
		defer rows.Close()
		st.ByStatus = map[string]int{}
		for rows.Next() {
			var status string
			var n int
			if err := rows.Scan(&status, &n); err != nil {
				return st, err
			}
			st.ByStatus[status] = n
			st.Instances += n
		}
		return st, rows.Err()
	}
	maxPages := opts.MaxEstimatePages
	if maxPages <= 0 {
		maxPages = cellstore.DefaultMaxEstimatePages
	}
	if ds.PageCount > maxPages {
		st.Note = "estimate_skipped_too_large"
		return st, nil
	}
	var est int
	if err := c.DB.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(ncell),0) FROM dbstat WHERE name='instances' AND pagetype='leaf'`).Scan(&est); err != nil {
		st.Note = "dbstat_unavailable"
		return st, nil
	}
	st.InstancesEstimate = est
	return st, nil
}
