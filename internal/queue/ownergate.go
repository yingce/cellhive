package queue

import (
	"context"
	"errors"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
)

// OwnerGate returns a Runner.Filter that keeps only queues whose cell this node
// owns, claiming unowned/expired cells (ADR-119). A queue held by a live peer is
// skipped, so exactly one node consumes it; when that node dies, another claims
// the cell after its lease expires and resumes consumption.
func OwnerGate(om *owner.Manager) func(ctx context.Context, ref Ref) (bool, error) {
	return OwnerGateWithReset(om, nil)
}

// OwnerGateWithReset is OwnerGate with a post-claim reset hook. When the gate
// claims a cell that was unowned or held by a different node/session, the
// local SQLite file may be a stale copy from an earlier ownership period.
// Consuming it would replay acked messages, hide messages produced by the
// previous owner, and publish the stale state as the new epoch baseline. The
// caller passes reset (normally store.Forget) so the next open hydrates the
// authoritative chain. A same-session expired lease keeps its local file.
func OwnerGateWithReset(om *owner.Manager, reset func(context.Context, cell.Scope) error) func(ctx context.Context, ref Ref) (bool, error) {
	return func(ctx context.Context, ref Ref) (bool, error) {
		if om == nil || ref.Namespace == "" || ref.Name == "" {
			return true, nil
		}
		sc := Scope(ref.Namespace, ref.Name)
		now := time.Now()
		o, _, err := om.ResolveCached(ctx, sc, time.Second)
		if err == nil && !o.Expired(now) {
			// Another session on the same node is still a different writer; do
			// not consume under it (its cell-agent may be restarting).
			return o.Node == om.NodeID && o.Session == om.Session, nil
		}
		if err != nil && !errors.Is(err, owner.ErrUnowned) {
			return false, err
		}
		staleLocal := errors.Is(err, owner.ErrUnowned)
		if err == nil && (o.Node != om.NodeID || o.Session != om.Session) {
			staleLocal = true
		}
		if _, cerr := om.Claim(ctx, sc, now); cerr == nil {
			if reset != nil && staleLocal {
				if rerr := reset(ctx, sc); rerr != nil {
					return false, rerr
				}
			}
			return true, nil
		}
		// A peer won the claim (or the race): let it consume.
		return false, nil
	}
}
