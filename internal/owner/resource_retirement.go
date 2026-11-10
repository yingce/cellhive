package owner

import (
	"context"
	"fmt"
	"time"

	"cellhive/internal/cell"
)

// WithResourceRetirement serializes a drained resource's retirement callback
// with this Manager's ordinary Claim, cleanup Claim and Release operations.
// The caller MUST first close admission and drain admitted/opening work. Neither
// callback nor any callback it invokes may reenter same-scope Claim/Release or
// wait for admitted requests. Other scopes and Renew remain independent.
//
// expected binds the exact local Node/Session/Epoch and source retirement
// lineage. The callback must validate durable cleanup authority and conditionally
// publish retirement (e.g. replica.RetireOwnedResource), checking that identity
// on every CAS retry: this local lock cannot exclude remote takeovers or Renew.
// A nil result confirms callback success and current ownership, not publication
// by itself. Admission stays the caller's responsibility, including on error.
// No ordinary access or cached ownership proof is granted by this method.
func (m *Manager) WithResourceRetirement(ctx context.Context, sc cell.Scope, jobID string, expected cell.Owner, fn func(context.Context, cell.Owner) error) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	if (sc.Class != "__kv__" && sc.Class != "__queue__") || jobID == "" || fn == nil || expected.Node == "" || expected.Session == "" || expected.Epoch == 0 {
		return fmt.Errorf("owner: invalid resource retirement transition")
	}
	unlock, err := m.lockClaim(ctx, sc.String())
	if err != nil {
		return err
	}
	defer unlock()
	finish := m.beginOwnershipTransition(sc)
	defer finish()
	cur, _, err := m.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if cur.Node != m.NodeID || cur.Session != m.Session || cur.Node != expected.Node || cur.Session != expected.Session || cur.Epoch != expected.Epoch {
		return ErrEpochMismatch
	}
	// The original request can be replayed after publication succeeded but its
	// response was lost. Only the same job on the same owner tuple qualifies;
	// AllowResource clears Retired, so recreation cannot take this exception.
	replay := cur.Retired && cur.RetirementJob == jobID && cur.RetiredEpoch >= expected.Epoch
	if !replay && (cur.Retired != expected.Retired || cur.RetirementJob != expected.RetirementJob || cur.RetiredEpoch != expected.RetiredEpoch) {
		return ErrEpochMismatch
	}
	if (cur.Retired && cur.RetirementJob != jobID) || (!cur.Retired && cur.RetirementJob == jobID) {
		return ErrRetired
	}
	live, err := m.liveFresh(ctx, cur, time.Now())
	if err != nil {
		return err
	}
	if !live {
		return ErrNotOwner
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(ctx, cur); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	after, _, err := m.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if after.Node != cur.Node || after.Session != cur.Session || after.Epoch != cur.Epoch {
		return ErrEpochMismatch
	}
	// Only the supplied source or this job's published retirement may survive
	// the callback. A cleared/recreated lineage is not an idempotent success.
	sameSource := after.Retired == cur.Retired && after.RetirementJob == cur.RetirementJob && after.RetiredEpoch == cur.RetiredEpoch
	published := after.Retired && after.RetirementJob == jobID && after.RetiredEpoch >= cur.Epoch
	if !sameSource && !published {
		return ErrRetired
	}
	live, err = m.liveFresh(ctx, after, time.Now())
	if err != nil {
		return err
	}
	if !live {
		return ErrNotOwner
	}
	return nil
}
