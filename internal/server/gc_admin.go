package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/replica"
)

type gcScopeRequest struct {
	Scope        string `json:"scope"`
	Safe         bool   `json:"safe"`
	KeepPrevious int    `json:"keep_previous"`
	GraceSeconds int    `json:"grace_seconds"`
	DryRun       bool   `json:"dry_run"`
}

// handleControlGCScope runs explicit native maintenance on the live owner.
// Epoch deletion additionally requires a resident, fully hydrated activation.
func (s *Server) handleControlGCScope(w http.ResponseWriter, r *http.Request) {
	if s.Replica == nil || s.Owner == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_replica", "replica or owner manager not configured")
		return
	}
	var req gcScopeRequest
	if !decodeControl(w, r, &req) {
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_scope", err.Error())
		return
	}
	if !req.Safe && !req.DryRun {
		writeErr(w, http.StatusBadRequest, "safe_required", "Safe GC requires safe=true; use dry_run=true to inspect first")
		return
	}
	ctx := r.Context()
	o, _, err := s.Owner.Resolve(ctx, sc)
	if err != nil {
		writeErr(w, http.StatusConflict, "not_owner", err.Error())
		return
	}
	if o.Node != s.Cfg.NodeID || o.Session != s.Owner.Session || !s.ownerLive(ctx, o, time.Now()) {
		writeErr(w, http.StatusConflict, "not_owner", "Safe GC must run on the live owner for "+sc.String())
		return
	}
	current := o.Epoch
	grace := time.Duration(req.GraceSeconds) * time.Second
	if grace <= 0 {
		grace = time.Hour
	}
	if req.KeepPrevious < 0 {
		req.KeepPrevious = 0
	}
	opts := compaction.Options{MinSegments: 1, Verify: func(vctx context.Context) error {
		cur, _, err := s.Owner.Resolve(vctx, sc)
		if err != nil {
			return err
		}
		if cur.Node != s.Cfg.NodeID || cur.Session != s.Owner.Session || cur.Epoch != current || !s.ownerLive(vctx, cur, time.Now()) {
			return errors.New("owner changed during compaction")
		}
		return nil
	}}
	stats, statsErr := s.Replica.BacklogStats(ctx, sc, current)
	compaction.RecordStatsSnapshot(stats, statsErr)
	if req.DryRun {
		res, derr := compaction.DryRun(ctx, s.Replica, sc, current, opts)
		if derr != nil {
			writeErr(w, http.StatusInternalServerError, "dry_run_failed", derr.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"scope": sc.String(), "epoch": current, "dry_run": true,
			"compaction": res, "stats": stats,
		})
		return
	}
	res, cerr := compaction.Compact(ctx, s.Replica, sc, current, opts)
	if cerr != nil {
		writeErr(w, http.StatusInternalServerError, "gc_failed", cerr.Error())
		return
	}
	retired := 0
	retireSkipped := ""
	var residentReady func(context.Context) (bool, error)
	if s.Store != nil {
		c, release, rerr := s.Store.AcquireResident(ctx, sc)
		if rerr != nil {
			writeErr(w, http.StatusInternalServerError, "resident_failed", rerr.Error())
			return
		}
		if c != nil {
			defer release()
			residentReady = func(context.Context) (bool, error) { return c.PagedComplete(), nil }
		}
	}
	if residentReady == nil {
		retireSkipped = "no resident activation"
	} else {
		retired, err = s.Replica.RetireEpochsWithOptions(ctx, sc, current, req.KeepPrevious, replica.RetireOptions{
			Safe: true, Grace: grace, Ready: residentReady,
			Verify: func(vctx context.Context) error {
				cur, _, rerr := s.Owner.Resolve(vctx, sc)
				if rerr != nil {
					return rerr
				}
				if cur.Node != s.Cfg.NodeID || cur.Session != s.Owner.Session || cur.Epoch != current || !s.ownerLive(vctx, cur, time.Now()) {
					return errors.New("owner changed during Safe GC")
				}
				return nil
			},
		})
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "retire_failed", err.Error())
			return
		}
	}
	stats, statsErr = s.Replica.BacklogStats(ctx, sc, current)
	compaction.RecordStatsSnapshot(stats, statsErr)
	writeJSON(w, http.StatusOK, map[string]any{
		"scope": sc.String(), "epoch": current, "dry_run": false,
		"compaction": res, "retired": retired, "retire_skipped": retireSkipped,
		"stats": stats,
	})
}
