package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

// Internal only: the lease credentials never belong in public cleanup status.
type resourceRetirementRequest struct {
	resourceCleanupValidation
	Expected cell.Owner `json:"expected_owner"`
}

func cleanupSourceGeneration(o cell.Owner) string {
	if o.RetirementJob == "" {
		return ""
	}
	return fmt.Sprintf("%s/%d", o.RetirementJob, o.RetiredEpoch)
}

func sameRetirementSource(current, expected cell.Owner, job string) bool {
	if current.Node != expected.Node || current.Session != expected.Session || current.Epoch != expected.Epoch {
		return false
	}
	if current.Retired && current.RetirementJob == job && current.RetiredEpoch >= expected.Epoch {
		return true // publication succeeded but its acknowledgment may have been lost
	}
	return current.Retired == expected.Retired && current.RetirementJob == expected.RetirementJob && current.RetiredEpoch == expected.RetiredEpoch
}

// prepareOwnedResourceRetirement is staged separately from physical cleanup.
// The caller must have durably captured the cleanup intent and lease. The
// returned owner is a retirement proof for this job, not authority to purge
// later without revalidating the lease. Every ambiguous failure keeps the local
// gate closed; this method never reopens admission or claims another owner.
func (s *Server) prepareOwnedResourceRetirement(ctx context.Context, j control.ResourceCleanup, expected cell.Owner) (cell.Owner, error) {
	if err := cleanupValidationIdentity(j); err != nil {
		return cell.Owner{}, err
	}
	if s.Owner == nil || s.Store == nil || s.resourceReplica() == nil {
		return cell.Owner{}, owner.ErrNotOwner
	}
	if expected.Node != s.Cfg.NodeID || expected.Session != s.Owner.Session || expected.Epoch == 0 {
		return cell.Owner{}, owner.ErrNotOwner
	}
	replay := expected.Retired && expected.RetirementJob == j.JobID && expected.RetiredEpoch >= expected.Epoch
	if !replay && (expected.Retired || cleanupSourceGeneration(expected) != j.SourceGeneration) {
		return cell.Owner{}, control.ErrCleanupStale
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return cell.Owner{}, err
	}
	work, cancel := context.WithDeadline(ctx, time.UnixMilli(j.LeaseUntilMs))
	defer cancel()
	if err := s.validateResourceCleanupAuthority(work, j); err != nil {
		return cell.Owner{}, err
	}
	current, _, err := s.Owner.Resolve(work, sc)
	if err != nil {
		return cell.Owner{}, err
	}
	if !sameRetirementSource(current, expected, j.JobID) || !s.ownerLive(work, current, time.Now()) || !s.ackAllowed(time.Now()) {
		return cell.Owner{}, owner.ErrEpochMismatch
	}
	if err := s.alignRetiredResourceGate(work, j, current); err != nil {
		return cell.Owner{}, err
	}
	identity := retirementIdentity(j, expected)
	if err := s.Store.BeginResourceRetirement(work, sc, identity); err != nil {
		return cell.Owner{}, err
	}
	// Drain before the owner transition lock: admitted requests and capture
	// completion can themselves need ownership resolution or claim work.
	if s.Capture != nil {
		if err := s.Capture.Drop(work, sc); err != nil {
			return cell.Owner{}, err
		}
	}
	err = s.Owner.WithResourceRetirement(work, sc, j.JobID, expected, func(ctx context.Context, _ cell.Owner) error {
		if err := s.validateResourceCleanupAuthority(ctx, j); err != nil {
			return err
		}
		if !s.ackAllowed(time.Now()) {
			return owner.ErrNotOwner
		}
		if err := s.resourceReplica().RetireOwnedResource(ctx, sc, j.JobID, expected, func(ctx context.Context) error {
			if !s.ackAllowed(time.Now()) {
				return owner.ErrNotOwner
			}
			return s.validateResourceCleanupAuthority(ctx, j)
		}); err != nil {
			return err
		}
		return s.validateResourceCleanupAuthority(ctx, j)
	})
	if err != nil {
		return cell.Owner{}, err
	}
	retired, _, err := s.Owner.Resolve(work, sc)
	if err != nil {
		return cell.Owner{}, err
	}
	if !sameRetirementSource(retired, expected, j.JobID) || !retired.Retired || retired.RetirementJob != j.JobID || retired.RetiredEpoch < expected.Epoch || !s.ackAllowed(time.Now()) {
		return cell.Owner{}, owner.ErrEpochMismatch
	}
	if err := s.validateResourceCleanupAuthority(work, j); err != nil {
		return cell.Owner{}, err
	}
	return retired, work.Err()
}

func (s *Server) handleResourceRetirementPrepare(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
	var in resourceRetirementRequest
	if err != nil || len(raw) > 64*1024 || json.Unmarshal(raw, &in) != nil {
		writeErr(w, 400, "bad_request", "invalid retirement envelope")
		return
	}
	in.Job.LeaseToken, in.Job.LeaseUntilMs = in.LeaseToken, in.LeaseUntilMs
	retired, err := s.prepareOwnedResourceRetirement(r.Context(), in.Job, in.Expected)
	if err != nil {
		writeErr(w, 503, "retirement_pending", "resource retirement could not be proven")
		return
	}
	writeJSON(w, 200, map[string]any{"owner": retired})
}

// prepareResourceRetirement resolves data authority independently of control
// authority. It never uses ordinary forwarding/claim admission, which rejects
// an already retired scope and cannot carry the cleanup lease credentials.
func (s *Server) prepareResourceRetirement(ctx context.Context, j control.ResourceCleanup) (cell.Owner, error) {
	if err := cleanupValidationIdentity(j); err != nil {
		return cell.Owner{}, err
	}
	if s.Owner == nil {
		return cell.Owner{}, owner.ErrNotOwner
	}
	work, cancel := context.WithDeadline(ctx, time.UnixMilli(j.LeaseUntilMs))
	defer cancel()
	if err := s.validateResourceCleanupAuthority(work, j); err != nil {
		return cell.Owner{}, err
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return cell.Owner{}, err
	}
	current, _, err := s.Owner.Resolve(work, sc)
	if errors.Is(err, owner.ErrUnowned) {
		return s.acquireResourceRetirement(work, j, nil)
	}
	if err != nil {
		return cell.Owner{}, err
	}
	live, err := s.Owner.Live(work, current, time.Now())
	if err != nil {
		return cell.Owner{}, err
	}
	if !live {
		return s.acquireResourceRetirement(work, j, &current)
	}
	if current.Node == s.Cfg.NodeID && current.Session == s.Owner.Session {
		return s.prepareOwnedResourceRetirement(work, j, current)
	}
	if current.Address == "" {
		return cell.Owner{}, owner.ErrNotOwner
	}
	var out struct {
		Owner cell.Owner `json:"owner"`
	}
	in := resourceRetirementRequest{resourceCleanupValidation: resourceCleanupValidation{Job: j, LeaseToken: j.LeaseToken, LeaseUntilMs: j.LeaseUntilMs}, Expected: current}
	if err := s.cleanupRPC(work, current.Address, "/v1/internal/resource/retirement-prepare", in, &out); err != nil {
		return cell.Owner{}, err
	}
	// A remote response is not an enduring permission to clean up. Re-read the
	// exact owner and lease so stale/malformed acknowledgments fail closed.
	after, _, err := s.Owner.Resolve(work, sc)
	if err != nil {
		return cell.Owner{}, err
	}
	if !sameRetirementSource(after, current, j.JobID) || !after.Retired || after.RetirementJob != j.JobID || after.RetiredEpoch < after.Epoch || !sameRetirementSource(out.Owner, after, j.JobID) || !out.Owner.Retired || out.Owner.RetirementJob != j.JobID || out.Owner.RetiredEpoch < after.Epoch {
		return cell.Owner{}, owner.ErrEpochMismatch
	}
	if err := s.validateResourceCleanupAuthority(work, j); err != nil {
		return cell.Owner{}, err
	}
	return after, work.Err()
}
