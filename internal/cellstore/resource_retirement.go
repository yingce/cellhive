package cellstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"cellhive/internal/cell"
)

// ErrRetirementIdentityMismatch rejects retries from a different job or owner
// incarnation. Callers must validate current authority before installing a gate.
var ErrRetirementIdentityMismatch = errors.New("cellstore: retirement identity mismatch")

// ResourceRetirementIdentity binds a gate to a validated cleanup intent and the
// exact owner incarnation it drains. Generation may be empty for legacy cells.
// These values are identity, not proof of authority: coordinator validation is
// required before calling BeginResourceRetirement.
type ResourceRetirementIdentity struct {
	JobID         string
	Generation    string
	Node          string
	Session       string
	Epoch         uint64
	OwnerRevision uint64
}

type resourceRetirement struct {
	identity ResourceRetirementIdentity
	since    time.Time
}

// BeginResourceRetirement closes request admission atomically, then drains
// busy requests and cold opens. Already admitted requests may still call Cell;
// eviction is deliberately not set here. Cancellation leaves admission closed
// and the identical job/owner can retry. Pinned cells are not exempt.
//
// This staged primitive does not publish retirement, stop capture, authenticate
// owner authority, or permit reopening. Ordinary Forget preserves the gate.
// Rebind/reclaim require explicit caller authority proofs; ordinary removal
// cannot clear this gate or authorize a fresh activation.
func (s *Store) BeginResourceRetirement(ctx context.Context, sc cell.Scope, identity ResourceRetirementIdentity) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return fmt.Errorf("cellstore: unsupported retirement class")
	}
	if identity.JobID == "" || identity.Node == "" || identity.Session == "" || identity.Epoch == 0 || len(identity.Generation) > 160 {
		return fmt.Errorf("cellstore: invalid retirement identity")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sc.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.retiring[key]; exists {
		if current.identity != identity {
			return ErrRetirementIdentityMismatch
		}
	} else {
		if s.retiring == nil {
			s.retiring = make(map[string]resourceRetirement)
		}
		s.retiring[key] = resourceRetirement{identity: identity, since: time.Now()}
		s.cond.Broadcast()
	}
	if err := s.waitLocked(ctx, func() bool { return s.busy[key] > 0 || s.opening[key] || s.evicting[key] }); err != nil {
		return err
	}
	// A guarded rebind/reclaim may have finished while this retry waited.
	if current, ok := s.retiring[key]; !ok || current.identity != identity {
		return ErrRetirementIdentityMismatch
	}
	return nil
}

// ResourceRetirementStats exposes retained barriers, including cancelled drains.
// OldestAge reports time since installation, not time since successful drain.
func (s *Store) ResourceRetirementStats() (count int, oldestAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, entry := range s.retiring {
		count++
		if age := now.Sub(entry.since); age > oldestAge {
			oldestAge = age
		}
	}
	return
}

// ErrRetirementBusy means a lifecycle transition cannot start without waiting
// for admitted work, a resident handle, or another open/removal to finish.
var ErrRetirementBusy = errors.New("cellstore: retirement lifecycle busy")

// ResourceRetirementIdentity returns a point-in-time retry hint, never authority.
func (s *Store) ResourceRetirementIdentity(sc cell.Scope) (ResourceRetirementIdentity, bool, error) {
	if err := sc.Validate(); err != nil {
		return ResourceRetirementIdentity{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	gate, ok := s.retiring[sc.String()]
	return gate.identity, ok, nil
}

// RebindResourceRetirement changes only the administrative owner identity of a
// drained gate. verify must freshly prove the exact predecessor -> next cleanup
// owner transition, the same durable job/source, and stopped capture while the
// caller serializes owner transitions. It runs outside Store.mu and must not
// reenter same-scope Store open/removal/lifecycle methods. No admission opens.
func (s *Store) RebindResourceRetirement(ctx context.Context, sc cell.Scope, expected, next ResourceRetirementIdentity, verify func(context.Context) error) error {
	if next.JobID != expected.JobID || next.Generation != expected.Generation || next.Node == "" || next.Session == "" || next.Epoch < expected.Epoch || next.Epoch == 0 {
		return ErrRetirementIdentityMismatch
	}
	return s.transitionResourceRetirement(ctx, sc, expected, &next, verify)
}

// ReclaimResourceRetirement removes exactly one drained gate after all local
// files/handles are gone. verify MUST prove capture stopped, local ownership and
// read authority discarded, and fresh authoritative safe loss/retirement or a
// distinct recreated generation, under caller-owned owner-transition exclusion.
// It must also arrange that later requests require a fresh authorized activation.
// This method cannot establish those remote proofs itself. A nil proof is rejected.
// The callback runs outside Store.mu; it must not reenter same-scope Store
// open/removal/lifecycle methods. Ordinary Forget/Invalidate never invoke this.
func (s *Store) ReclaimResourceRetirement(ctx context.Context, sc cell.Scope, expected ResourceRetirementIdentity, verify func(context.Context) error) error {
	return s.transitionResourceRetirement(ctx, sc, expected, nil, verify)
}

func (s *Store) transitionResourceRetirement(ctx context.Context, sc cell.Scope, expected ResourceRetirementIdentity, next *ResourceRetirementIdentity, verify func(context.Context) error) error {
	p, err := s.Path(sc)
	if err != nil {
		return err
	}
	if verify == nil {
		return errors.New("cellstore: retirement proof required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sc.String()
	s.mu.Lock()
	gate, ok := s.retiring[key]
	if !ok || gate.identity != expected {
		s.mu.Unlock()
		return ErrRetirementIdentityMismatch
	}
	if s.busy[key] > 0 || s.opening[key] || s.evicting[key] || s.cells[p] != nil {
		s.mu.Unlock()
		return ErrRetirementBusy
	}
	// Existing open/removal paths honor evicting. No operation can replace this
	// reservation; concurrent lifecycle calls reject it, and Begin only waits.
	s.evicting[key] = true
	s.mu.Unlock()
	defer s.finishEviction(key)
	if next == nil {
		for _, path := range []string{p, p + "-wal", p + "-shm", pagedMarkerPath(p)} {
			if _, err := os.Lstat(path); err == nil {
				return ErrRetirementBusy
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err := verify(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current, ok := s.retiring[key]
	if !ok || current.identity != expected {
		return ErrRetirementIdentityMismatch
	}
	if s.busy[key] > 0 || s.opening[key] || s.cells[p] != nil {
		return ErrRetirementBusy
	}
	if next == nil {
		delete(s.retiring, key)
	} else {
		current.identity = *next
		s.retiring[key] = current
	}
	s.cond.Broadcast()
	return nil
}

// ResourceRetirementScopes pages the process-local gates in scope order. It is
// diagnostic/maintenance state, never an authorization proof or bucket listing.
func (s *Store) ResourceRetirementScopes(after string, limit int) []cell.Scope {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, min(limit, len(s.retiring)))
	for key := range s.retiring {
		if key <= after {
			continue
		}
		i := sort.SearchStrings(keys, key)
		if i >= limit {
			continue
		}
		keys = append(keys, "")
		copy(keys[i+1:], keys[i:])
		keys[i] = key
		if len(keys) > limit {
			keys = keys[:limit]
		}
	}
	out := make([]cell.Scope, 0, len(keys))
	for _, key := range keys {
		sc, err := cell.ParseScope(key)
		if err == nil {
			out = append(out, sc)
		}
	}
	return out
}
