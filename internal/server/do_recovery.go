package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
)

// handleDORecoverScope is a cold-start gate. The caller has claimed the DO
// shard, so the old runtime cannot acknowledge further writes. It brings one
// capture file up to the last follower-acknowledged fragment without sealing
// the cell-agent's process session, which may carry unrelated live cells.
func (s *Server) handleDORecoverScope(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Scope      string `json:"scope"`
		OwnerScope string `json:"owner_scope"`
		OwnerNode  string `json:"owner_node"`
		OwnerEpoch uint64 `json:"owner_epoch"`
	}
	if !decode(w, r, &req) {
		return
	}
	if s.Owner == nil || s.NodeLog == nil || s.Recovery == nil || s.Lease == nil {
		writeErr(w, http.StatusServiceUnavailable, "recovery_unavailable", "DO recovery dependencies are missing")
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_scope", err.Error())
		return
	}
	doScope, err := cell.ParseScope(req.OwnerScope)
	if err != nil || doScope.Class != DOClass || req.OwnerNode == "" || req.OwnerEpoch == 0 {
		writeErr(w, http.StatusBadRequest, "bad_owner", "DO owner identity is required")
		return
	}
	// A fresh bucket read avoids accepting an old runtime's cached owner after
	// the successor has claimed the shard.
	doOwner, _, err := s.Owner.Resolve(r.Context(), doScope)
	if err != nil || doOwner.Node != req.OwnerNode || doOwner.Epoch != req.OwnerEpoch || doOwner.Expired(time.Now()) {
		writeErr(w, http.StatusConflict, "owner_moved", "DO ownership changed before recovery")
		return
	}
	if pointer, _, perr := s.getDORecoveryPointer(r.Context(), sc); perr == nil {
		if pointer.DOOwnerScope != req.OwnerScope || pointer.DOOwnerEpoch > req.OwnerEpoch {
			writeErr(w, http.StatusConflict, "recovery_owner_mismatch", "DO recovery pointer belongs to another owner generation")
			return
		}
		n, rerr := s.recoverDOPointer(r.Context(), sc, pointer)
		if rerr != nil {
			writeErr(w, http.StatusServiceUnavailable, "recovery_failed", rerr.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"recovered": n})
		return
	} else if !errors.Is(perr, bucket.ErrNotFound) {
		writeErr(w, http.StatusServiceUnavailable, "recovery_pointer", perr.Error())
		return
	}
	captureOwner, _, err := s.Owner.Resolve(r.Context(), sc)
	if errors.Is(err, owner.ErrUnowned) {
		writeJSON(w, http.StatusOK, map[string]any{"recovered": 0})
		return
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "capture_owner", err.Error())
		return
	}
	rec, _, err := s.NodeLog.Get(r.Context(), captureOwner.Node, captureOwner.Session)
	if errors.Is(err, nodelog.ErrNotFound) {
		writeJSON(w, http.StatusOK, map[string]any{"recovered": 0})
		return
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "node_log", err.Error())
		return
	}
	if rec.Status == nodelog.StatusSealed {
		writeJSON(w, http.StatusOK, map[string]any{"recovered": 0})
		return
	}
	l, err := s.Lease.Get(r.Context(), captureOwner.Node)
	var n int
	switch {
	case err == nil && l.Live(time.Now()) && l.Session == captureOwner.Session:
		n, err = s.Recovery.RecoverLiveScope(r.Context(), rec, sc)
	case err == nil && l.Live(time.Now()):
		// A restarted node has a new session. The old session may still contain
		// acknowledged data and must be recovered before this file is read.
		n, err = s.Recovery.Recover(r.Context(), s.NodeLog, captureOwner.Node, captureOwner.Session)
	case errors.Is(err, bucket.ErrNotFound) || err == nil:
		n, err = s.Recovery.Recover(r.Context(), s.NodeLog, captureOwner.Node, captureOwner.Session)
	default:
		err = fmt.Errorf("capture node lease uncertain: %w", err)
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "recovery_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovered": n})
}
