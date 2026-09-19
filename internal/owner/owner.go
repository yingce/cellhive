// Package owner implements the cell owner claim / resolve protocol.
package owner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

var (
	// ErrUnowned means no owner record exists (cold cell).
	ErrUnowned = errors.New("owner: unowned")
	// ErrOwnerLive means another node holds a live lease.
	ErrOwnerLive = errors.New("owner: live owner exists")
	// ErrEpochMismatch means the expected epoch no longer matches.
	ErrEpochMismatch = errors.New("owner: epoch mismatch")
	// ErrNotOwner means this node is not the scope's owner (a non-owner replica
	// must forward to the owner, ADR-003).
	ErrNotOwner = errors.New("owner: caller is not the owner")
)

type cachedOwner struct {
	owner cell.Owner
	etag  string
	at    time.Time
}

// Manager performs owner operations for a node.
type Manager struct {
	B         bucket.Bucket
	NodeID    string
	Session   string
	Advertise string
	Role      cell.Role
	OwnerTTL  time.Duration

	mu    sync.Mutex
	cache map[string]cachedOwner

	ownedMu sync.Mutex
	owned   map[string]OwnedScope

	// Metrics (ADR-165): epoch bumps and takeover outcomes, read by
	// cell-agent's /metrics.
	epochBumps      atomic.Int64
	takeoverSuccess atomic.Int64
	takeoverFailed  atomic.Int64
	takeoverBlocked atomic.Int64
}

// ClaimStats reports owner lifecycle counters for /metrics.
func (m *Manager) ClaimStats() map[string]int64 {
	return map[string]int64{
		"epoch_bumps":      m.epochBumps.Load(),
		"takeover_success": m.takeoverSuccess.Load(),
		"takeover_failed":  m.takeoverFailed.Load(),
		"takeover_blocked": m.takeoverBlocked.Load(),
	}
}

// OwnedScope is a scope this node currently holds, with the epoch it holds it
// under. It is advisory process-local state; callers must still re-check
// ownership before acting on it.
type OwnedScope struct {
	Scope cell.Scope
	Epoch uint64
}

// Key returns the owner record key for a scope.
func Key(s cell.Scope) string { return s.Key() + "/owner.json" }

// ResolveCached returns the owner record with a short TTL cache so the hot path
// does not read object storage on every request (see docs/cell-protocol.md
// §4.1). A stale hit can lag an ownership change by at most ttl; writers still
// verify the epoch they were handed and self-fence when renewal fails.
func (m *Manager) ResolveCached(ctx context.Context, s cell.Scope, ttl time.Duration) (cell.Owner, string, error) {
	key := s.String()
	m.mu.Lock()
	if c, ok := m.cache[key]; ok && time.Since(c.at) < ttl {
		m.mu.Unlock()
		return c.owner, c.etag, nil
	}
	m.mu.Unlock()

	o, e, err := m.Resolve(ctx, s)
	if err != nil {
		return o, e, err
	}
	m.mu.Lock()
	if m.cache == nil {
		m.cache = make(map[string]cachedOwner)
	}
	m.cache[key] = cachedOwner{owner: o, etag: e, at: time.Now()}
	m.mu.Unlock()
	return o, e, nil
}

// Invalidate drops a cached owner record (after claim/release).
func (m *Manager) Invalidate(s cell.Scope) {
	m.mu.Lock()
	delete(m.cache, s.String())
	m.mu.Unlock()
}

// Resolve returns the current owner record and its etag, or ErrUnowned.
func (m *Manager) Resolve(ctx context.Context, s cell.Scope) (cell.Owner, string, error) {
	data, e, err := m.B.Get(ctx, Key(s))
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return cell.Owner{}, "", ErrUnowned
		}
		return cell.Owner{}, "", err
	}
	o, err := cell.UnmarshalOwner(data)
	if err != nil {
		return cell.Owner{}, "", fmt.Errorf("owner: corrupt record: %w", err)
	}
	return o, e, nil
}

// Claim attempts to acquire ownership of a scope (ADR-037: cell-agent driven).
//
// On success it returns the new owner record. If another node holds a live
// lease it returns ErrOwnerLive with that record. Races return
// bucket.ErrPrecondition.
func (m *Manager) Claim(ctx context.Context, s cell.Scope, now time.Time) (cell.Owner, error) {
	cur, e, err := m.Resolve(ctx, s)
	switch {
	case errors.Is(err, ErrUnowned):
		epoch, gerr := m.nextEpoch(ctx, s)
		if gerr != nil {
			return cell.Owner{}, gerr
		}
		o := m.newOwner(epoch, now)
		data, _ := cell.MarshalOwner(o)
		if _, cerr := m.B.ConditionalCreate(ctx, Key(s), data); cerr != nil {
			return cell.Owner{}, cerr // lost the race
		}
		m.Invalidate(s)
		m.rememberOwned(s, o.Epoch)
		return o, nil
	case err != nil:
		return cell.Owner{}, err
	}

	// A live lease blocks a claim regardless of who holds it: re-claiming our
	// own live lease must not bump the epoch (that would advance the replication
	// lineage without a restart). A restarted process with the same node id also
	// waits out the old session's lease (ADR-037).
	if !cur.Expired(now) {
		m.takeoverBlocked.Add(1)
		return cur, ErrOwnerLive
	}
	takeover := cur.Node != m.NodeID
	epoch, gerr := m.nextEpoch(ctx, s)
	if gerr != nil {
		return cell.Owner{}, gerr
	}
	o := m.newOwner(epoch, now)
	data, _ := cell.MarshalOwner(o)
	if _, cerr := m.B.CAS(ctx, Key(s), data, e); cerr != nil {
		if takeover {
			m.takeoverFailed.Add(1)
		}
		return cell.Owner{}, cerr // lost the race; caller re-resolves
	}
	if takeover {
		m.takeoverSuccess.Add(1)
	}
	m.Invalidate(s)
	m.rememberOwned(s, o.Epoch)
	return o, nil
}

// Renew refreshes the lease for a scope this node owns.
func (m *Manager) Renew(ctx context.Context, s cell.Scope, epoch uint64, now time.Time) (cell.Owner, error) {
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return cell.Owner{}, err
	}
	if cur.Node != m.NodeID || cur.Epoch != epoch {
		return cell.Owner{}, ErrEpochMismatch
	}
	cur.Expiry = now.Add(m.OwnerTTL).UnixMilli()
	data, _ := cell.MarshalOwner(cur)
	if _, err := m.B.CAS(ctx, Key(s), data, e); err != nil {
		return cell.Owner{}, err
	}
	m.rememberOwned(s, cur.Epoch)
	return cur, nil
}

// Release removes the owner record if this node still owns the given epoch.
func (m *Manager) Release(ctx context.Context, s cell.Scope, epoch uint64) error {
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return err
	}
	if cur.Node != m.NodeID || cur.Epoch != epoch {
		return ErrEpochMismatch
	}
	// Conditional delete: if the lease expired and a peer re-claimed between the
	// resolve above and this delete, the CAS fails and the peer's claim survives
	// (never break the single-writer fence).
	if err := m.B.ConditionalDelete(ctx, Key(s), e); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return ErrEpochMismatch
		}
		return err
	}
	m.Invalidate(s)
	m.forgetOwned(s)
	return nil
}

func (m *Manager) rememberOwned(s cell.Scope, epoch uint64) {
	m.ownedMu.Lock()
	if m.owned == nil {
		m.owned = map[string]OwnedScope{}
	}
	m.owned[s.String()] = OwnedScope{Scope: s, Epoch: epoch}
	m.ownedMu.Unlock()
}

func (m *Manager) forgetOwned(s cell.Scope) {
	m.ownedMu.Lock()
	delete(m.owned, s.String())
	m.ownedMu.Unlock()
}

// OwnedScopes returns a snapshot of the scopes this node currently claims.
func (m *Manager) OwnedScopes() []OwnedScope {
	m.ownedMu.Lock()
	defer m.ownedMu.Unlock()
	out := make([]OwnedScope, 0, len(m.owned))
	for _, v := range m.owned {
		out = append(out, v)
	}
	return out
}

func (m *Manager) newOwner(epoch uint64, now time.Time) cell.Owner {
	return cell.Owner{
		Node:         m.NodeID,
		Role:         m.Role,
		Session:      m.Session,
		Epoch:        epoch,
		Expiry:       now.Add(m.OwnerTTL).UnixMilli(),
		Address:      m.Advertise,
		ProtoVersion: cell.ProtoVersion,
	}
}
