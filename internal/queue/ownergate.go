package queue

import (
	"context"
	"errors"
	"time"

	"cellhive/internal/owner"
)

// OwnerGate returns a Runner.Filter that keeps only queues whose cell this node
// owns, claiming unowned/expired cells (ADR-119). A queue held by a live peer is
// skipped, so exactly one node consumes it; when that node dies, another claims
// the cell after its lease expires and resumes consumption.
func OwnerGate(om *owner.Manager) func(ctx context.Context, ref Ref) (bool, error) {
	return func(ctx context.Context, ref Ref) (bool, error) {
		if om == nil || ref.Namespace == "" || ref.Name == "" {
			return true, nil
		}
		sc := Scope(ref.Namespace, ref.Name)
		now := time.Now()
		o, _, err := om.ResolveCached(ctx, sc, time.Second)
		if err == nil && !o.Expired(now) {
			return o.Node == om.NodeID, nil
		}
		if err != nil && !errors.Is(err, owner.ErrUnowned) {
			return false, err
		}
		if _, cerr := om.Claim(ctx, sc, now); cerr == nil {
			return true, nil
		}
		// A peer won the claim (or the race): let it consume.
		return false, nil
	}
}
