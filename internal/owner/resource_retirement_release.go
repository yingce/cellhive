package owner

import (
	"context"
	"fmt"

	"cellhive/internal/cell"
)

// WithReleasedResourceRetirement proves this process's retired owner was
// released before reclaiming its local retirement marker. Admission and captures
// must already be drained and cell files removed. The callback must validate its
// durable cleanup lease, and must not claim/release this scope or open/remove a
// store cell. It grants no ordinary access or cached ownership evidence.
//
// The local claim lock excludes reactivation during the callback. Remote owner
// changes cannot be locked here: a fresh read after the callback must still match
// the released tuple and retirement lineage, otherwise the result fails closed.
func (m *Manager) WithReleasedResourceRetirement(ctx context.Context, sc cell.Scope, jobID string, previous cell.Owner, fn func(context.Context) error) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	if (sc.Class != "__kv__" && sc.Class != "__queue__") || jobID == "" || fn == nil || previous.Node == "" || previous.Session == "" || previous.Epoch == 0 || previous.Node != m.NodeID || previous.Session != m.Session {
		return fmt.Errorf("owner: invalid released resource retirement proof")
	}
	unlock, err := m.lockClaim(ctx, sc.String())
	if err != nil {
		return err
	}
	defer unlock()
	finish := m.beginOwnershipTransition(sc)
	defer finish()
	current, _, err := m.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if current.Node != "" || current.Session != "" || current.Epoch < previous.Epoch {
		return ErrEpochMismatch
	}
	if !current.Retired || current.RetirementJob != jobID || current.RetiredEpoch < previous.Epoch {
		return ErrRetired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	after, _, err := m.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if after.Node != current.Node || after.Session != current.Session || after.Epoch != current.Epoch {
		return ErrEpochMismatch
	}
	if after.Retired != current.Retired || after.RetirementJob != current.RetirementJob || after.RetiredEpoch != current.RetiredEpoch {
		return ErrRetired
	}
	return nil
}
