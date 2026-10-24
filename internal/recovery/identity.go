package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
)

func (m *Manager) follower(ctx context.Context, rec nodelog.Record, addr string) (context.Context, string, error) {
	if rec.ProtoVersion != nodelog.ProtoVersionIdentity {
		return ctx, addr, nil
	}
	id, ok := rec.FollowerRefs[addr]
	if !ok || id.Node == "" || id.Instance == "" || m.Lease == nil {
		return ctx, "", fmt.Errorf("recovery: missing follower identity or resolver for %s", addr)
	}
	resolved, err := m.Lease.ResolveRecoveryAddress(ctx, id)
	return peer.WithIdentity(ctx, id), resolved, err
}

// A locally locked data directory proves that our previous process exited.
// A foreign live process with the SAME session must never be sealed.
func (m *Manager) checkSession(ctx context.Context, node, session string) error {
	if m.Lease == nil {
		return nil
	}
	if node == m.Lease.NodeID && session == m.Lease.Session {
		return fmt.Errorf("recovery: refusing current local session")
	}
	l, err := m.Lease.Get(ctx, node)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if l.Node != node {
		return fmt.Errorf("recovery: lease node mismatch")
	}
	if l.Live(time.Now()) && l.Session == session {
		if m.LocalInstanceLocked && node == m.Lease.NodeID && l.Instance != "" && l.Instance == m.Lease.Instance {
			return nil
		}
		return fmt.Errorf("recovery: session %s/%s is live", node, session)
	}
	return nil
}
