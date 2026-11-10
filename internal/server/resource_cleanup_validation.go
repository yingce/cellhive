package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

// resourceCleanupValidation is internal-only: public cleanup status intentionally
// omits lease credentials. Never serialize this envelope in a public response.
type resourceCleanupValidation struct {
	Job          control.ResourceCleanup `json:"job"`
	LeaseToken   string                  `json:"lease_token"`
	LeaseUntilMs int64                   `json:"lease_until_ms"`
}

func cleanupValidationIdentity(j control.ResourceCleanup) error {
	if j.Namespace == "" || j.Name == "" || j.JobID == "" || j.LeaseToken == "" || j.LeaseUntilMs <= time.Now().UnixMilli() || (j.Kind != "kv" && j.Kind != "queue") {
		return control.ErrCleanupStale
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil || sc.String() != j.DataScope || sc.Namespace != j.Namespace || sc.Class != "__"+j.Kind+"__" || (j.Kind == "queue" && sc.ID != j.Name) {
		return control.ErrCleanupStale
	}
	return nil
}

// cleanupControlOwner always point-reads authority; a cached route or a local
// control SQLite file alone cannot authorize cleanup. Unowned/expired control
// fails closed: the normal control recovery path must establish authority first.
func (s *Server) cleanupControlOwner(ctx context.Context, ns string) (cell.Owner, error) {
	if s.Owner == nil {
		return cell.Owner{}, owner.ErrNotOwner
	}
	o, _, err := s.Owner.Resolve(ctx, control.ScopeFor(ns))
	if err != nil {
		return cell.Owner{}, err
	}
	if !s.ownerLive(ctx, o, time.Now()) {
		return cell.Owner{}, owner.ErrNotOwner
	}
	return o, nil
}

func sameCleanupControlOwner(a, b cell.Owner) bool {
	return a.Node == b.Node && a.Session == b.Session && a.Epoch == b.Epoch
}

// validateResourceCleanupAuthority is a point-in-time check, not permission for
// a later destructive operation. Call only AFTER the captured cleanup intent and
// lease writes finish, outside control resourceMu/capturedWrite callbacks. It
// neither claims resource data nor acquires a capture dependency.
func (s *Server) validateResourceCleanupAuthority(ctx context.Context, j control.ResourceCleanup) error {
	if err := cleanupValidationIdentity(j); err != nil {
		return err
	}
	o, err := s.cleanupControlOwner(ctx, j.Namespace)
	if err != nil {
		return err
	}
	if o.Node == s.Cfg.NodeID {
		return s.validateLocalResourceCleanupAuthority(ctx, j)
	}
	if o.Address == "" {
		return owner.ErrNotOwner
	}
	var result struct {
		Valid bool `json:"valid"`
	}
	err = s.cleanupRPC(ctx, o.Address, "/v1/internal/resource/cleanup-validate", resourceCleanupValidation{Job: j, LeaseToken: j.LeaseToken, LeaseUntilMs: j.LeaseUntilMs}, &result)
	if err != nil {
		return err
	}
	if !result.Valid {
		return control.ErrCleanupStale
	}
	after, err := s.cleanupControlOwner(ctx, j.Namespace)
	if err != nil {
		return err
	}
	if !sameCleanupControlOwner(o, after) {
		return owner.ErrNotOwner
	}
	return cleanupValidationIdentity(j)
}

func (s *Server) validateLocalResourceCleanupAuthority(ctx context.Context, j control.ResourceCleanup) error {
	if err := cleanupValidationIdentity(j); err != nil {
		return err
	}
	if s.Owner == nil || s.Control == nil {
		return owner.ErrNotOwner
	}
	// Track local release/invalidation across the SQLite read, without holding a
	// manager mutex or opening a resource admission/capture gate.
	check, ok := s.Owner.ReadFence(control.ScopeFor(j.Namespace))
	if !ok {
		return owner.ErrNotOwner
	}
	defer check()
	before, err := s.cleanupControlOwner(ctx, j.Namespace)
	if err != nil {
		return err
	}
	if before.Node != s.Cfg.NodeID || before.Session != s.Owner.Session || !s.ackAllowed(time.Now()) {
		return owner.ErrNotOwner
	}
	if err := s.Control.ValidateResourceCleanupLease(ctx, j); err != nil {
		return err
	}
	after, err := s.cleanupControlOwner(ctx, j.Namespace)
	if err != nil {
		return err
	}
	if !sameCleanupControlOwner(before, after) || !s.ackAllowed(time.Now()) || !check() {
		return owner.ErrNotOwner
	}
	return cleanupValidationIdentity(j)
}

func (s *Server) handleResourceCleanupValidate(w http.ResponseWriter, r *http.Request) {
	var in resourceCleanupValidation
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
	if err != nil || len(raw) > 64*1024 || json.Unmarshal(raw, &in) != nil {
		writeErr(w, 400, "bad_request", "invalid cleanup validation envelope")
		return
	}
	in.Job.LeaseToken, in.Job.LeaseUntilMs = in.LeaseToken, in.LeaseUntilMs
	if err := cleanupValidationIdentity(in.Job); err != nil {
		writeErr(w, 400, "bad_request", "invalid cleanup validation identity")
		return
	}
	// A wrong-owner receiver rejects instead of forwarding: stale routes cannot
	// recurse or cause a follower to consult its stale local control file.
	if err := s.validateLocalResourceCleanupAuthority(r.Context(), in.Job); err != nil {
		if errors.Is(err, control.ErrCleanupStale) || errors.Is(err, control.ErrNotFound) {
			writeErr(w, 409, "cleanup_stale", "cleanup lease is not current")
		} else {
			writeErr(w, 503, "cleanup_authority_unavailable", "current control owner could not validate cleanup")
		}
		return
	}
	writeJSON(w, 200, map[string]bool{"valid": true})
}
