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

// DeliverScheduleHint routes only to an address supplied by the existing owner
// manager. A stale or abandoned hint cannot appoint a new owner.
func (s *Server) DeliverScheduleHint(ctx context.Context, h scheduleindex.Hint) error {
	ctx = bucket.WithReadSource(ctx, bucket.SharedSchedule)
	sc, err := cell.ParseScope(h.Scope)
	if err != nil || !h.Valid() {
		return errors.New("invalid scheduling hint")
	}
	o, _, err := s.Owner.ResolveRoute(ctx, sc)
	// Durable orphan discovery handles takeover when there is no current owner.
	if errors.Is(err, owner.ErrUnowned) {
		return nil
	}
	if err != nil {
		return err
	}
	if o.Retired {
		return nil
	}
	req := scheduleRequest{Hint: h, Node: o.Node, Session: o.Session, Epoch: o.Epoch}
	if o.Node == s.Cfg.NodeID {
		return s.acceptScheduleHint(ctx, req)
	}
	live, err := s.Owner.Live(ctx, o, time.Now())
	if err != nil || !live {
		return err
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
	if h.Kind == "queue" {
		if s.QueueLocalNudge == nil {
			return errors.New("queue runner unavailable")
		}
		s.QueueLocalNudge(sc.Namespace, sc.ID)
	} else {
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
