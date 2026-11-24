package owner

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"context"
)

// publishClaim folds generation allocation into publication on backends that
// can do both atomically. Object backends retain their existing generation CAS.
func (m *Manager) publishClaim(ctx context.Context, s cell.Scope, o cell.Owner, expected string, floor uint64, atomicClaim bool) (cell.Owner, error) {
	if c, ok := m.B.(bucket.OwnerClaimer); ok && atomicClaim {
		o.Epoch = 0
		body, err := cell.MarshalOwner(o)
		if err != nil {
			return o, err
		}
		raw, _, err := c.ClaimOwner(ctx, Key(s), body, expected, floor)
		if err != nil {
			return o, err
		}
		o, err = cell.UnmarshalOwner(raw)
		if err == nil {
			m.epochBumps.Add(1)
		}
		return o, err
	}
	epoch, err := m.nextEpoch(ctx, s, floor)
	if err != nil {
		return o, err
	}
	o.Epoch = epoch
	body, err := cell.MarshalOwner(o)
	if err != nil {
		return o, err
	}
	if expected == "" {
		_, err = m.B.ConditionalCreate(ctx, Key(s), body)
	} else {
		_, err = m.B.CAS(ctx, Key(s), body, expected)
	}
	return o, err
}
