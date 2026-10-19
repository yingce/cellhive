// Package recovery collects acknowledged writes from surviving followers after
// a node loss (docs/cell-protocol.md §6). It runs before a new owner restores.
package recovery

import (
	"context"
	"errors"
	"fmt"

	"cellhive/internal/cell"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/replica"
)

// Manager performs follower-seal recovery.
type Manager struct {
	Replica *replica.Manager
	Peer    peer.Transport
	Sealed  peer.RecoveryTransport
}

// New creates a recovery manager.
func New(r *replica.Manager, p peer.Transport) *Manager {
	m := &Manager{Replica: r, Peer: p}
	if sealed, ok := p.(peer.RecoveryTransport); ok {
		m.Sealed = sealed
	}
	return m
}

// Collect fetches every segment held by the given followers and uploads them to
// the object store under their scope/epoch prefix. It returns the number of
// segments collected. Followers that are unreachable are skipped (best effort);
// an error is returned only when nothing could be collected and a follower
// failed.
func (m *Manager) Collect(ctx context.Context, followers []string) (int, error) {
	total := 0
	var lastErr error
	for _, f := range followers {
		held, err := m.Peer.Held(ctx, f)
		if err != nil {
			// An unreachable follower means segments it holds may be missing;
			// the caller must not seal the session (it will retry later).
			lastErr = fmt.Errorf("recovery: follower %s unreachable: %w", f, err)
			continue
		}
		for _, h := range held {
			sc, err := cell.ParseScope(h.Scope)
			if err != nil {
				lastErr = err
				continue
			}
			if _, _, err := m.Replica.Append(ctx, sc, h.Epoch, h.Segment); err != nil {
				if errors.Is(err, replica.ErrResourceDeleted) {
					continue
				}
				lastErr = fmt.Errorf("recovery: append %s: %w", h.Scope, err)
				continue
			}
			total++
		}
	}
	if lastErr != nil {
		// Partial collection is not a successful recovery: report it (and never
		// seal) so the acked-but-unuploaded segments are retried.
		return total, fmt.Errorf("recovery: incomplete collect: %w", lastErr)
	}
	return total, nil
}

// collectSealed freezes each follower session and uploads its contiguous tail.
// Recovery fails closed unless every recorded follower can be sealed and read
// through the final sequence it reported.
func (m *Manager) collectSealed(ctx context.Context, followers []string, session string) (int, map[string]uint64, error) {
	if m.Sealed == nil {
		return 0, nil, fmt.Errorf("recovery: peer transport lacks seal/tail support")
	}
	total := 0
	covered := make(map[string]uint64, len(followers))
	for _, follower := range followers {
		final, err := m.Sealed.Seal(ctx, follower, session)
		if err != nil {
			return total, covered, fmt.Errorf("recovery: seal follower %s: %w", follower, err)
		}
		var after uint64
		for after < final {
			fragments, err := m.Sealed.Tail(ctx, follower, session, after)
			if err != nil {
				return total, covered, fmt.Errorf("recovery: tail follower %s after %d: %w", follower, after, err)
			}
			if len(fragments) == 0 {
				return total, covered, fmt.Errorf("recovery: follower %s tail ended at %d want %d", follower, after, final)
			}
			for _, fragment := range fragments {
				want := after + 1
				if fragment.Sequence != want {
					return total, covered, fmt.Errorf("recovery: follower %s tail gap: got %d want %d", follower, fragment.Sequence, want)
				}
				sc, err := cell.ParseScope(fragment.Scope)
				if err != nil {
					return total, covered, err
				}
				for _, segment := range fragment.Segments {
					if _, _, err := m.Replica.Append(ctx, sc, fragment.Epoch, segment); err != nil {
						// A durable resource-delete fence intentionally supersedes
						// these old payloads. Advance the session sequence without
						// resurrecting them or blocking unrelated live cells.
						if errors.Is(err, replica.ErrResourceDeleted) {
							continue
						}
						return total, covered, fmt.Errorf("recovery: append %s: %w", fragment.Scope, err)
					}
					total++
				}
				after = fragment.Sequence
			}
		}
		covered[follower] = final
	}
	return total, covered, nil
}

// RecoverLiveScope copies the acknowledged tail of one fenced DO capture scope
// out of an active cell-agent session. Other cells keep using that session.
// The DO owner must already have changed before this function is called.
func (m *Manager) RecoverLiveScope(ctx context.Context, rec nodelog.Record, scope cell.Scope) (int, error) {
	live, ok := m.Peer.(peer.LiveRecoveryTransport)
	if !ok {
		return 0, fmt.Errorf("recovery: peer transport lacks live tail support")
	}
	if rec.ProtoVersion != nodelog.ProtoVersionSession || rec.Status != nodelog.StatusOpen {
		return 0, fmt.Errorf("recovery: active session has unsupported state %s/%s", rec.ProtoVersion, rec.Status)
	}
	if len(rec.Followers) == 0 {
		return 0, fmt.Errorf("recovery: open session has no followers")
	}
	total := 0
	for _, follower := range rec.Followers {
		final, err := live.Head(ctx, follower, rec.Session)
		if err != nil {
			return total, fmt.Errorf("recovery: follower head %s: %w", follower, err)
		}
		var after uint64
		var offset int64
		for after < final {
			fragments, next, nextOffset, err := live.TailOpen(ctx, follower, rec.Session, scope.String(), after, final, offset)
			if err != nil {
				return total, fmt.Errorf("recovery: follower tail %s: %w", follower, err)
			}
			if next <= after || next > final || nextOffset <= offset {
				return total, fmt.Errorf("recovery: follower %s tail did not advance correctly at %d/%d", follower, after, final)
			}
			previous := after
			for _, fragment := range fragments {
				if fragment.Sequence <= previous || fragment.Sequence > next || fragment.Scope != scope.String() {
					return total, fmt.Errorf("recovery: follower %s invalid scoped tail at %d", follower, fragment.Sequence)
				}
				for _, segment := range fragment.Segments {
					if _, _, err := m.Replica.Append(ctx, scope, fragment.Epoch, segment); err != nil {
						return total, fmt.Errorf("recovery: append %s: %w", scope.String(), err)
					}
					total++
				}
				previous = fragment.Sequence
			}
			after, offset = next, nextOffset
		}
	}
	return total, nil
}

// RecoverNode runs recovery for every unsealed session of a dead node. Sealed
// sessions take the fast path (graceful handoff already uploaded everything). It
// returns the number of segments collected and the number of sessions recovered.
func (m *Manager) RecoverNode(ctx context.Context, nl *nodelog.Manager, deadNode string) (segments, sessions int, err error) {
	recs, err := nl.ListNode(ctx, deadNode)
	if err != nil {
		return 0, 0, err
	}
	for _, rec := range recs {
		if rec.Status == nodelog.StatusSealed {
			continue
		}
		n, rerr := m.Recover(ctx, nl, deadNode, rec.Session)
		if rerr != nil {
			return segments, sessions, fmt.Errorf("recovery: node %s session %s: %w", deadNode, rec.Session, rerr)
		}
		segments += n
		sessions++
	}
	return segments, sessions, nil
}

// Recover runs the node-log recovery gate for a dead node/session:
// absent or sealed -> no-op; open/recovering -> fence, collect followers, seal.
func (m *Manager) Recover(ctx context.Context, nl *nodelog.Manager, deadNode, session string) (int, error) {
	rec, _, err := nl.Get(ctx, deadNode, session)
	if err != nil {
		if errors.Is(err, nodelog.ErrNotFound) {
			return 0, nil // session never acked past the bucket
		}
		return 0, err
	}
	if rec.Status == nodelog.StatusSealed {
		return 0, nil
	}
	if _, err := nl.SetStatus(ctx, deadNode, session, nodelog.StatusRecovering); err != nil {
		return 0, err
	}
	var n int
	covered := map[string]uint64{}
	if len(rec.Followers) > 0 {
		if rec.ProtoVersion == nodelog.ProtoVersionSession {
			n, covered, err = m.collectSealed(ctx, rec.Followers, rec.Session)
		} else {
			n, err = m.Collect(ctx, rec.Followers)
		}
	}
	if err != nil {
		return n, err
	}
	if _, err := nl.SealCovered(ctx, deadNode, session, covered); err != nil {
		return n, err
	}
	for follower := range covered {
		// Coverage is already durable in the sealed node-log. Reclamation is a
		// space optimization and must not turn a completed recovery into failure.
		_ = m.Sealed.Reclaim(ctx, follower, session)
	}
	return n, nil
}
