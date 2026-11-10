package owner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cellhive/internal/cell"
)

// ClaimForResourceCleanup acquires only expired/unowned cleanup authority. The
// caller must close local admission and drain admitted/opening work before
// calling: this method holds the per-scope claim lock during recovery/reset.
// Supply a durable cleanup-lease validator. expected is the exact source
// incarnation; nil means absent.
// No ordinary requests may use this acquisition. Claim still rejects Retired.
// Validation/reset callbacks must not reenter claims for this scope or wait
// for requests that may themselves be waiting to claim it.
func (m *Manager) ClaimForResourceCleanup(ctx context.Context, sc cell.Scope, now time.Time, jobID string, expected *cell.Owner, validate func(context.Context) error, reset func(context.Context, cell.Scope) error) (cell.Owner, error) {
	if err := sc.Validate(); err != nil {
		return cell.Owner{}, err
	}
	if (sc.Class != "__kv__" && sc.Class != "__queue__") || jobID == "" || validate == nil {
		return cell.Owner{}, fmt.Errorf("owner: invalid cleanup claim")
	}
	release, err := m.lockClaim(ctx, sc.String())
	if err != nil {
		return cell.Owner{}, err
	}
	defer release()
	if err := validate(ctx); err != nil {
		return cell.Owner{}, err
	}
	cur, token, err := m.Resolve(ctx, sc)
	missing := errors.Is(err, ErrUnowned)
	if err != nil && !missing {
		return cell.Owner{}, err
	}
	if missing != (expected == nil) {
		return cur, ErrEpochMismatch
	}
	if !missing {
		if cur.Node != expected.Node || cur.Session != expected.Session || cur.Epoch != expected.Epoch || cur.Retired != expected.Retired || cur.RetirementJob != expected.RetirementJob || cur.RetiredEpoch != expected.RetiredEpoch {
			return cur, ErrEpochMismatch
		}
		if (cur.Retired && cur.RetirementJob != jobID) || (!cur.Retired && cur.RetirementJob == jobID) {
			return cur, ErrRetired
		}
		if cur.Node != "" {
			if cur.Session == "" {
				return cur, fmt.Errorf("owner: cleanup predecessor lacks session")
			}
			live, err := m.liveFresh(ctx, cur, now)
			if err != nil {
				return cur, err
			}
			if live {
				return cur, ErrOwnerLive
			}
			if m.BeforeTakeover != nil {
				if err := m.BeforeTakeover(ctx, cur); err != nil {
					return cur, err
				}
			}
		} else if cur.Session != "" {
			return cur, fmt.Errorf("owner: unowned cleanup lineage has session")
		}
	}
	if reset != nil {
		if err := reset(ctx, sc); err != nil {
			return cur, err
		}
	}
	epoch, err := m.nextEpoch(ctx, sc, cur.Epoch)
	if err != nil {
		return cell.Owner{}, err
	}
	if err := validate(ctx); err != nil {
		return cur, err
	}
	if err := ctx.Err(); err != nil {
		return cur, err
	}
	acquired := m.newOwner(epoch, now)
	acquired.Retired, acquired.RetirementJob = true, jobID
	// This administrative epoch never admits writes. Cover it in the same
	// acquisition CAS so lost-ACK retries have a complete retirement proof.
	acquired.RetiredEpoch = max(cur.RetiredEpoch, acquired.Epoch)
	raw, err := cell.MarshalOwner(acquired)
	if err != nil {
		return cur, err
	}
	if missing {
		_, err = m.B.ConditionalCreate(ctx, Key(sc), raw)
	} else {
		_, err = m.B.CAS(ctx, Key(sc), raw, token)
	}
	if err != nil {
		return cur, err
	}
	m.Invalidate(sc)
	m.rememberOwned(sc, acquired.Epoch)
	return acquired, nil
}
