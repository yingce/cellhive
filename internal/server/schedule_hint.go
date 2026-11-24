package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/scheduleindex"
)

type scheduleRequest struct {
	Hint          scheduleindex.Hint
	Node, Session string
	Epoch         uint64
}

// DeliverScheduleHint routes to live owners, or requests the ordinary fenced
// takeover gate for an absent/dead owner. The hint itself never grants authority.
func (s *Server) DeliverScheduleHint(ctx context.Context, h scheduleindex.Hint) error {
	ctx = bucket.WithReadSource(ctx, bucket.SharedSchedule)
	sc, err := cell.ParseScope(h.Scope)
	if err != nil || !h.Valid() {
		return errors.New("invalid scheduling hint")
	}
	if s.Owner == nil || !s.ackAllowed(time.Now()) {
		return owner.ErrNotOwner
	}
	o, _, err := s.Owner.ResolveRoute(ctx, sc)
	missing := errors.Is(err, owner.ErrUnowned)
	if err != nil && !missing {
		return err
	}
	if !missing && o.Retired {
		return nil
	}
	live := false
	if !missing {
		live, err = s.Owner.Live(ctx, o, time.Now())
		if err != nil {
			return err
		}
	}
	if !live {
		if s.ScheduleAcquire == nil {
			return owner.ErrNotOwner
		}
		acquired, err := s.ScheduleAcquire(ctx, sc)
		if err != nil {
			return err
		}
		if !acquired {
			return owner.ErrNotOwner
		}
		// Re-read authority after the callback. Another claimant may have won;
		// never synthesize a request from the old tuple or the callback alone.
		o, _, err = s.Owner.Resolve(ctx, sc)
		if err != nil {
			return err
		}
		if o.Retired || o.Node != s.Owner.NodeID || o.Session != s.Owner.Session {
			return owner.ErrNotOwner
		}
		live, err = s.Owner.Live(ctx, o, time.Now())
		if err != nil {
			return err
		}
		if !live {
			return owner.ErrNotOwner
		}
	}
	req := scheduleRequest{Hint: h, Node: o.Node, Session: o.Session, Epoch: o.Epoch}
	if o.Node == s.Cfg.NodeID {
		return s.acceptScheduleHint(ctx, req)
	}
	err = s.cleanupRPC(ctx, o.Address, "/v1/internal/schedule/hint", req, nil)
	if err != nil {
		s.Owner.Invalidate(sc)
	}
	return err
}
func (s *Server) acceptScheduleHint(ctx context.Context, req scheduleRequest) error {
	h := req.Hint
	if !h.Valid() || s.Owner == nil || req.Node != s.Cfg.NodeID || req.Session != s.Owner.Session || !s.ackAllowed(time.Now()) {
		return owner.ErrNotOwner
	}
	sc, _ := cell.ParseScope(h.Scope)
	o, _, err := s.Owner.ResolveRoute(ctx, sc)
	if err != nil {
		return err
	}
	if o.Retired || o.Node != req.Node || o.Session != req.Session || o.Epoch != req.Epoch {
		return owner.ErrNotOwner
	}
	// Both runners acquire their normal resource and owner gates after the nudge.
	switch h.Kind {
	case "cron":
		if s.CronNudge == nil {
			return errors.New("cron scheduler unavailable")
		}
		s.CronNudge()
	case "purge":
		if s.Control == nil {
			return errors.New("control store unavailable")
		}
		s.Control.NudgePurge()
	case "resource-cleanup":
		s.NudgeResourceCleanup()
	case "queue":
		if s.QueueLocalNudge == nil {
			return errors.New("queue runner unavailable")
		}
		s.QueueLocalNudge(sc.Namespace, sc.ID)
	default:
		if s.Timers == nil {
			return errors.New("timer registry unavailable")
		}
		s.Timers.Add(sc.String())
	}
	return nil
}
func (s *Server) handleScheduleHint(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil || !req.Hint.Valid() {
		writeErr(w, 400, "bad_request", "invalid scheduling hint")
		return
	}
	if err := s.acceptScheduleHint(bucket.WithReadSource(r.Context(), bucket.SharedSchedule), req); err != nil {
		writeErr(w, 409, "schedule_hint_rejected", "owner or runner unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}
