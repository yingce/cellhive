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
}

// New creates a recovery manager.
func New(r *replica.Manager, p peer.Transport) *Manager { return &Manager{Replica: r, Peer: p} }

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
	n, err := m.Collect(ctx, rec.Followers)
	if err != nil {
		return n, err
	}
	if _, err := nl.Seal(ctx, deadNode, session); err != nil {
		return n, err
	}
	return n, nil
}
