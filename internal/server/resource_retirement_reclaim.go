package server

import (
	"context"
	"errors"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/owner"
)

// reclaimLostResourceGate discards only obsolete local state. It never seizes
// ownership or mutates the remote resource. Its caller is outside admission.
func (s *Server) reclaimLostResourceGate(ctx context.Context, sc cell.Scope) error {
	if s.Store == nil || s.Owner == nil || s.resourceReplica() == nil {
		return nil
	}
	identity, exists, err := s.Store.ResourceRetirementIdentity(sc)
	if err != nil || !exists {
		return err
	}
	// Avoid stopping a current owner's capture merely because deletion is pending.
	current, _, err := s.Owner.Resolve(ctx, sc)
	if err != nil && !errors.Is(err, owner.ErrUnowned) {
		return err
	}
	if err == nil && current.Node == s.Cfg.NodeID && current.Session == s.Owner.Session {
		return nil
	}
	if err := s.Store.BeginResourceRetirement(ctx, sc, identity); err != nil {
		return err
	}
	if s.Capture != nil {
		if err := s.Capture.Drop(ctx, sc); err != nil {
			return err
		}
	}
	return s.Owner.WithDiscardedResourceAuthority(ctx, sc, func(ctx context.Context) error {
		verify := func() error {
			now, _, err := s.Owner.Resolve(ctx, sc)
			if errors.Is(err, owner.ErrUnowned) {
				return nil
			}
			if err != nil {
				return err
			}
			if now.Node == s.Cfg.NodeID && now.Session == s.Owner.Session {
				return owner.ErrOwnerLive
			}
			return nil
		}
		ok, err := s.Store.TryInvalidate(ctx, sc, verify)
		if err != nil {
			return err
		}
		if !ok {
			return cellstore.ErrRetirementBusy
		}
		return s.Store.ReclaimResourceRetirement(ctx, sc, identity, func(ctx context.Context) error {
			if err := s.resourceReplica().RefreshResourceSnapshot(ctx, sc); err != nil {
				return err
			}
			return verify()
		})
	})
}

// All nodes sweep local gates, including followers of the control cell. Advance
// past failures/live owners so one unresolved gate cannot starve later entries.
func (s *Server) sweepLocalRetirementGates(ctx context.Context) error {
	if s.Store == nil || s.Owner == nil {
		return nil
	}
	scopes := s.Store.ResourceRetirementScopes(s.localRetirementAfter, 20)
	if len(scopes) == 0 {
		s.localRetirementAfter = ""
		return nil
	}
	work, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var first error
	for _, sc := range scopes {
		if work.Err() != nil {
			break
		}
		s.localRetirementAfter = sc.String()
		one, stop := context.WithTimeout(work, time.Second)
		err := s.reclaimLostResourceGate(one, sc)
		stop()
		if err != nil && !errors.Is(err, owner.ErrOwnerLive) && first == nil {
			first = err
		}
	}
	return first
}
