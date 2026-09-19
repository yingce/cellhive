package owner

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

// ErrGenContention means the generation counter could not be bumped after
// several CAS attempts (a lossy store under contention). It is retryable: the
// caller should re-resolve and try again. Epochs are only skipped, never reused.
var ErrGenContention = errors.New("owner: generation bump contention")

// genKey is the monotonic generation counter for a scope. Unlike the owner
// record it is never deleted, so epochs are never reused even after release
// (WDL: "the generation key is a fence, not a cache", ADR-078).
func genKey(s cell.Scope) string { return s.Key() + "/owner-gen" }

// nextEpoch bumps and returns the scope's monotonic generation. Bumping happens
// before the owner write, so a lost race only skips epochs (never reuses one).
func (m *Manager) nextEpoch(ctx context.Context, s cell.Scope) (uint64, error) {
	key := genKey(s)
	for attempt := 0; attempt < 8; attempt++ {
		data, e, err := m.B.Get(ctx, key)
		var g uint64
		switch {
		case err == nil:
			g, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		case errors.Is(err, bucket.ErrNotFound):
			g = 0
		default:
			return 0, err
		}
		next := g + 1
		nb := []byte(strconv.FormatUint(next, 10))
		if errors.Is(err, bucket.ErrNotFound) {
			if _, cerr := m.B.ConditionalCreate(ctx, key, nb); cerr == nil {
				m.epochBumps.Add(1)
				return next, nil
			}
			continue // lost the race; re-read
		}
		if _, cerr := m.B.CAS(ctx, key, nb, e); cerr == nil {
			m.epochBumps.Add(1)
			return next, nil
		}
	}
	return 0, ErrGenContention
}

// ClaimAs acquires ownership on behalf of a specific node/advertise (e.g. a
// remote do-runtime task) rather than this manager's own node. This is the
// cell-agent-driven claim used by do-runtime (ADR-037/ADR-078). The epoch
// (generation) is monotonic per scope; a live lease held by another node returns
// ErrOwnerLive with that record.
func (m *Manager) ClaimAs(ctx context.Context, s cell.Scope, node, advertise string, role cell.Role, ttl time.Duration, now time.Time) (cell.Owner, error) {
	if ttl <= 0 {
		ttl = m.OwnerTTL
	}
	cur, e, err := m.Resolve(ctx, s)
	switch {
	case errors.Is(err, ErrUnowned):
		epoch, gerr := m.nextEpoch(ctx, s)
		if gerr != nil {
			return cell.Owner{}, gerr
		}
		o := m.ownerFor(epoch, now, node, advertise, role, ttl)
		data, _ := cell.MarshalOwner(o)
		if _, cerr := m.B.ConditionalCreate(ctx, Key(s), data); cerr != nil {
			return cell.Owner{}, cerr // lost the race
		}
		m.Invalidate(s)
		return o, nil
	case err != nil:
		return cell.Owner{}, err
	}
	if cur.Node != node && !cur.Expired(now) {
		m.takeoverBlocked.Add(1)
		return cur, ErrOwnerLive
	}
	takeover := cur.Node != node
	epoch, gerr := m.nextEpoch(ctx, s)
	if gerr != nil {
		return cell.Owner{}, gerr
	}
	o := m.ownerFor(epoch, now, node, advertise, role, ttl)
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
	return o, nil
}

// RenewAs refreshes the lease for a scope held by an explicit node/epoch.
func (m *Manager) RenewAs(ctx context.Context, s cell.Scope, node string, epoch uint64, ttl time.Duration, now time.Time) (cell.Owner, error) {
	if ttl <= 0 {
		ttl = m.OwnerTTL
	}
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return cell.Owner{}, err
	}
	if cur.Node != node || cur.Epoch != epoch {
		return cell.Owner{}, ErrEpochMismatch
	}
	cur.Expiry = now.Add(ttl).UnixMilli()
	data, _ := cell.MarshalOwner(cur)
	if _, err := m.B.CAS(ctx, Key(s), data, e); err != nil {
		return cell.Owner{}, err
	}
	return cur, nil
}

// ReleaseAs removes the owner record for an explicit node/epoch (used on drain).
func (m *Manager) ReleaseAs(ctx context.Context, s cell.Scope, node string, epoch uint64) error {
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return err
	}
	if cur.Node != node || cur.Epoch != epoch {
		return ErrEpochMismatch
	}
	if err := m.B.ConditionalDelete(ctx, Key(s), e); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return ErrEpochMismatch
		}
		return err
	}
	m.Invalidate(s)
	return nil
}

func (m *Manager) ownerFor(epoch uint64, now time.Time, node, advertise string, role cell.Role, ttl time.Duration) cell.Owner {
	return cell.Owner{
		Node:         node,
		Role:         role,
		Session:      m.Session,
		Epoch:        epoch,
		Expiry:       now.Add(ttl).UnixMilli(),
		Address:      advertise,
		ProtoVersion: cell.ProtoVersion,
	}
}

// DOScope is the owner scope for a (worker, class, shard) Durable Object host.
func DOScope(ns, worker, class string, shard int) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__do__", ID: worker + "~" + class + "~shard" + strconv.Itoa(shard)}
}
