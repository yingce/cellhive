package owner

import (
	"context"
	"errors"
	"fmt"

	"cellhive/internal/cell"
)

// WithDiscardedResourceAuthority serializes removal of obsolete local resource
// state. The caller first drains admission and stops capture. fn must not claim
// or release this scope. Same-process authority is rejected at any epoch; lease
// expiry alone is not permission to discard it. No remote state is mutated.
func (m *Manager) WithDiscardedResourceAuthority(ctx context.Context, sc cell.Scope, fn func(context.Context) error) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	if (sc.Class != "__kv__" && sc.Class != "__queue__") || fn == nil {
		return fmt.Errorf("owner: invalid authority discard")
	}
	unlock, err := m.lockClaim(ctx, sc.String())
	if err != nil {
		return err
	}
	defer unlock()
	finish := m.beginOwnershipTransition(sc)
	defer finish()
	read := func() (cell.Owner, bool, error) {
		o, _, err := m.Resolve(ctx, sc)
		if errors.Is(err, ErrUnowned) {
			return cell.Owner{}, false, nil
		}
		if err != nil {
			return cell.Owner{}, false, err
		}
		if o.Node == m.NodeID && o.Session == m.Session {
			return cell.Owner{}, false, ErrOwnerLive
		}
		return o, true, nil
	}
	before, exists, err := read()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.forgetOwned(sc)
	if err := fn(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	after, present, err := read()
	if err != nil {
		return err
	}
	if exists != present || before.Node != after.Node || before.Session != after.Session || before.Epoch != after.Epoch || before.Retired != after.Retired || before.RetirementJob != after.RetirementJob || before.RetiredEpoch != after.RetiredEpoch {
		return ErrEpochMismatch
	}
	return nil
}
