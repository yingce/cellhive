package owner

import "cellhive/internal/cell"

// ResourceProof retains local ownership evidence until Release. Validate does
// not release it, so admission and response publication can check the same cut.
// Callers must additionally hold the store's resource admission gate.
type ResourceProof struct {
	Owner    cell.Owner
	Validate func() bool
	Release  func()
}

func (m *Manager) LocalResourceProof(sc cell.Scope) (*ResourceProof, bool) {
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return nil, false
	}
	key := sc.String()
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.routes[key]
	lm := m.localLease(c.owner)
	if !ok || !c.local || lm == nil || m.releasing[key] != 0 || c.owner.Retired || c.owner.Epoch <= c.owner.RetiredEpoch {
		return nil, false
	}
	health := lm.HealthProof()
	if !health.Healthy || health.Revision != c.leaseRevision {
		return nil, false
	}
	rev, epoch := m.cacheRev[key], m.cacheEpoch
	if m.guards == nil {
		m.guards = make(map[string]int)
	}
	m.guards[key]++
	released := false
	proof := &ResourceProof{Owner: c.owner}
	proof.Validate = func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		now, exists := m.routes[key]
		h := lm.HealthProof()
		return !released && exists && now.owner == c.owner && m.cacheEpoch == epoch && m.cacheRev[key] == rev && m.releasing[key] == 0 && m.localLease(c.owner) == lm && h.Healthy && h.Revision == c.leaseRevision
	}
	proof.Release = func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !released {
			released = true
			m.releaseGuardLocked(key)
		}
	}
	return proof, true
}
