package server

import (
	"context"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

func retirementIdentity(j control.ResourceCleanup, o cell.Owner) cellstore.ResourceRetirementIdentity {
	return cellstore.ResourceRetirementIdentity{JobID: j.JobID, Generation: j.SourceGeneration, Node: o.Node, Session: o.Session, Epoch: o.Epoch}
}

func cleanupSourceMatches(j control.ResourceCleanup, o *cell.Owner) bool {
	if o == nil {
		return j.SourceGeneration == ""
	}
	if o.Retired {
		return o.RetirementJob == j.JobID
	}
	return o.RetirementJob != j.JobID && cleanupSourceGeneration(*o) == j.SourceGeneration
}

// acquireResourceRetirement only claims expired/unowned cleanup authority. All
// local requests are drained first; the owner implementation rechecks liveness,
// recovers the complete predecessor session, and publishes Retired atomically.
func (s *Server) acquireResourceRetirement(ctx context.Context, j control.ResourceCleanup, expected *cell.Owner) (cell.Owner, error) {
	if s.Owner == nil || s.Store == nil || !s.ackAllowed(time.Now()) {
		return cell.Owner{}, owner.ErrNotOwner
	}
	if err := s.validateResourceCleanupAuthority(ctx, j); err != nil {
		return cell.Owner{}, err
	}
	if !cleanupSourceMatches(j, expected) {
		return cell.Owner{}, control.ErrCleanupStale
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return cell.Owner{}, err
	}
	gateOwner := cell.Owner{Node: s.Cfg.NodeID, Session: s.Owner.Session, Epoch: 1}
	if expected != nil {
		gateOwner.Epoch = max(expected.Epoch, 1)
		if expected.Node != "" {
			gateOwner.Node, gateOwner.Session = expected.Node, expected.Session
		}
	}
	identity := retirementIdentity(j, gateOwner)
	if previous, exists, err := s.Store.ResourceRetirementIdentity(sc); err != nil {
		return cell.Owner{}, err
	} else if exists {
		if previous.JobID != j.JobID || previous.Generation != j.SourceGeneration {
			return cell.Owner{}, cellstore.ErrRetirementIdentityMismatch
		}
		identity = previous
	}
	if err := s.Store.BeginResourceRetirement(ctx, sc, identity); err != nil {
		return cell.Owner{}, err
	}
	if s.Capture != nil {
		if err := s.Capture.Drop(ctx, sc); err != nil {
			return cell.Owner{}, err
		}
	}
	validate := func(ctx context.Context) error {
		if !s.ackAllowed(time.Now()) {
			return owner.ErrNotOwner
		}
		return s.validateResourceCleanupAuthority(ctx, j)
	}
	acquired, err := s.Owner.ClaimForResourceCleanup(ctx, sc, time.Now(), j.JobID, expected, validate, func(ctx context.Context, sc cell.Scope) error {
		ok, err := s.Store.TryInvalidate(ctx, sc, func() error { return validate(ctx) })
		if err != nil {
			return err
		}
		if !ok {
			return control.ErrCleanupPending
		}
		return nil
	})
	if err != nil {
		return cell.Owner{}, err
	}
	if err := s.alignRetiredResourceGate(ctx, j, acquired); err != nil {
		return cell.Owner{}, err
	}
	if err := validate(ctx); err != nil {
		return cell.Owner{}, err
	}
	return acquired, nil
}

// A successful cleanup CAS can lose its acknowledgment before the gate is
// rebound to the new administrative tuple. Only a fresh, already-retired local
// owner for this exact job may reconcile that gate; never an ordinary owner.
func (s *Server) alignRetiredResourceGate(ctx context.Context, j control.ResourceCleanup, current cell.Owner) error {
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return err
	}
	previous, exists, err := s.Store.ResourceRetirementIdentity(sc)
	if err != nil {
		return err
	}
	next := retirementIdentity(j, current)
	if !exists || previous == next {
		return nil
	}
	if previous.JobID != j.JobID || previous.Generation != j.SourceGeneration || !current.Retired || current.RetirementJob != j.JobID || current.RetiredEpoch < current.Epoch || current.Node != s.Cfg.NodeID || current.Session != s.Owner.Session {
		return cellstore.ErrRetirementIdentityMismatch
	}
	if err := s.Store.BeginResourceRetirement(ctx, sc, previous); err != nil {
		return err
	}
	if s.Capture != nil {
		if err := s.Capture.Drop(ctx, sc); err != nil {
			return err
		}
	}
	validate := func(ctx context.Context) error {
		if err := s.validateResourceCleanupAuthority(ctx, j); err != nil {
			return err
		}
		now, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil {
			return err
		}
		if !sameRetirementSource(now, current, j.JobID) || !now.Retired || now.RetirementJob != j.JobID || !s.ackAllowed(time.Now()) {
			return owner.ErrEpochMismatch
		}
		return nil
	}
	ok, err := s.Store.TryInvalidate(ctx, sc, func() error { return validate(ctx) })
	if err != nil {
		return err
	}
	if !ok {
		return control.ErrCleanupPending
	}
	return s.Owner.WithResourceRetirement(ctx, sc, j.JobID, current, func(ctx context.Context, _ cell.Owner) error {
		return s.Store.RebindResourceRetirement(ctx, sc, previous, next, validate)
	})
}
