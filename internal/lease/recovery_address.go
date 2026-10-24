package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/objectstore"
)

type RecoveryAddress struct {
	cell.PeerIdentity
	Address string `json:"address"`
}

func recoveryAddressKey(id cell.PeerIdentity) string {
	return "nodes/" + id.Node + "/recovery/" + id.Instance
}

// PublishRecoveryAddress is called only while holding the data-directory lock
// and after binding the recovery listener. It grants no ownership or liveness.
func (m *Manager) PublishRecoveryAddress(ctx context.Context) error {
	if m.NodeID == "" || m.Instance == "" || m.PeerURL == "" {
		return fmt.Errorf("recovery address requires node, instance and URL")
	}
	a := RecoveryAddress{PeerIdentity: cell.PeerIdentity{Node: m.NodeID, Instance: m.Instance}, Address: m.PeerURL}
	b, _ := json.Marshal(a)
	_, err := objectstore.NewOwned(m.B, objectstore.PrefixNodes, objectstore.OwnerNodeLog).Put(ctx, recoveryAddressKey(a.PeerIdentity), b)
	return err
}
func (m *Manager) ResolveRecoveryAddress(ctx context.Context, id cell.PeerIdentity) (string, error) {
	if id.Node == "" || id.Instance == "" {
		return "", fmt.Errorf("missing follower identity")
	}
	// This address exists before the new lease, so simultaneous restarts can
	// recover from each other without making an unfinished boot a live owner.
	b, err := objectstore.NewOwned(m.B, objectstore.PrefixNodes, objectstore.OwnerNodeLog).Get(ctx, recoveryAddressKey(id))
	if err == nil {
		var a RecoveryAddress
		if err = json.Unmarshal(b, &a); err != nil {
			return "", err
		}
		if a.PeerIdentity != id || a.Address == "" {
			return "", fmt.Errorf("invalid recovery address identity")
		}
		return a.Address, nil
	}
	// No blind fallback on read uncertainty. Old installations may have only a lease.
	if !errors.Is(err, bucket.ErrNotFound) {
		return "", err
	}
	l, err := m.Get(ctx, id.Node)
	if err != nil {
		return "", err
	}
	if l.Node != id.Node || l.Instance != id.Instance || l.PeerURL == "" {
		return "", fmt.Errorf("follower persistent identity mismatch for %s", id.Node)
	}
	return l.PeerURL, nil
}
