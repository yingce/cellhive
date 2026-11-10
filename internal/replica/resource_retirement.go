package replica

import (
	"context"
	"errors"
	"fmt"

	"cellhive/internal/cell"
)

var ErrResourceOwnerChanged = errors.New("replica: resource retirement owner changed")

// RetireOwnedResource publishes a previously drained owner's retirement in one
// CAS. The caller must first validate the durable cleanup intent, close local
// admission and drain it, and serialize ownership transitions. This method
// checks identity; it does not establish those authority or drain proofs.
// A conflicting renewal may retry, but a successor must be gated separately.
func (m *Manager) RetireOwnedResource(ctx context.Context, sc cell.Scope, jobID string, expected cell.Owner, validate func(context.Context) error) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	if validate == nil {
		return fmt.Errorf("replica: missing cleanup authority validator")
	}
	if (sc.Class != "__kv__" && sc.Class != "__queue__") || jobID == "" || expected.Node == "" || expected.Session == "" || expected.Epoch == 0 {
		return fmt.Errorf("replica: invalid owned retirement identity")
	}
	return m.updateResourceFence(ctx, sc, func(f *resourceFence) error {
		if (!f.Deleted && f.JobID == jobID) || (f.Deleted && f.JobID != jobID) {
			return ErrResourceDeleted
		}
		f.Deleted, f.JobID = true, jobID
		if f.Epoch < expected.Epoch {
			f.Epoch = expected.Epoch
		}
		return nil
	}, func(current cell.Owner) error {
		if err := validate(ctx); err != nil {
			return err
		}
		if current.Node != expected.Node || current.Session != expected.Session || current.Epoch != expected.Epoch {
			return ErrResourceOwnerChanged
		}
		// A lost response may leave this exact job published. Source-generation
		// matching otherwise rejects recreation even if the owner tuple is unchanged.
		if current.Retired && current.RetirementJob == jobID {
			return nil
		}
		if current.Retired != expected.Retired || current.RetirementJob != expected.RetirementJob || current.RetiredEpoch != expected.RetiredEpoch {
			return ErrResourceDeleted
		}
		return nil
	})
}
