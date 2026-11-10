package server

import (
	"bytes"
	"cellhive/internal/telemetry"
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
	"cellhive/internal/ownerclient"
	queuepkg "cellhive/internal/queue"
	"cellhive/internal/replica"
	"cellhive/internal/wake"
)

func (s *Server) resourceReplica() *replica.Manager { return s.resourceRep }

// prepareResourceAccessSnapshot takes one fence snapshot before the request
// gate. Admission below rechecks it after acquiring the gate; a stale snapshot
// can therefore neither serve a deleted resource nor roll back its marker.
func (s *Server) prepareResourceAccessSnapshot(ctx context.Context, sc cell.Scope) (string, error) {
	if err := s.reclaimLostResourceGate(ctx, sc); err != nil {
		return "", err
	}
	ctx, span := telemetry.Tracer().Start(ctx, "cell.resource_prepare")
	defer span.End()
	var generation string
	var blocked bool
	var floor uint64
	var err error
	if p := resourceProof(ctx, sc); p != nil {
		generation, err = resourceProofGeneration(p)
		floor = p.Owner.RetiredEpoch
	} else {
		generation, blocked, floor, err = s.resourceReplica().ResourceSnapshot(ctx, sc)
	}
	if err != nil {
		return "", err
	}
	if blocked {
		return "", replica.ErrResourceDeleted
	}
	if s.Store != nil {
		if err := s.Store.EnsureResourceGenerationFromSnapshot(ctx, sc, generation, func() (string, error) {
			return s.preparedResourceGeneration(ctx, sc)
		}); err != nil {
			return "", err
		}
	}
	if floor > 0 && s.Owner != nil && s.Store != nil {
		o, _, err := s.Owner.ResolveRoute(ctx, sc)
		if err == nil && o.Epoch <= floor {
			s.Owner.Invalidate(sc)
			// Never block a request behind a full Forget: an in-flight request
			// may still need to open the cell. TryForget returns false if the
			// cell is busy; the caller can retry instead of deadlocking.
			ok, ferr := s.Store.TryForget(ctx, sc, nil)
			if ferr != nil {
				return "", ferr
			}
			if !ok {
				return "", fmt.Errorf("cell busy while retiring a stale generation")
			}
		}
	}
	return generation, nil
}

func (s *Server) beginPreparedResourceRequest(ctx context.Context, sc cell.Scope, generation string) (func(), error) {
	ctx, span := telemetry.Tracer().Start(ctx, "cell.resource_gate")
	defer span.End()
	return s.Store.BeginPreparedResourceRequest(ctx, sc, generation, func() (string, error) {
		return s.preparedResourceGeneration(ctx, sc)
	})
}

// PrepareResourceAccess runs outside a cell's BeginRequest gate. The epoch
// floor also invalidates an old self-owner hint and local cache on a node that
// was offline during deletion, so recreation cannot expose its old SQLite data.
func (s *Server) PrepareResourceAccess(ctx context.Context, sc cell.Scope) error {
	if err := s.reclaimLostResourceGate(ctx, sc); err != nil {
		return err
	}
	if s.resourceReplica() == nil {
		return nil
	}
	blocked, floor, err := s.resourceReplica().ResourceState(ctx, sc)
	if err != nil {
		return err
	}
	if blocked {
		return replica.ErrResourceDeleted
	}
	if s.Store != nil {
		generation, err := s.resourceReplica().ResourceGeneration(ctx, sc)
		if err != nil {
			return err
		}
		if err := s.Store.EnsureResourceGeneration(ctx, sc, generation, func() (string, error) { return s.resourceReplica().ResourceGeneration(ctx, sc) }); err != nil {
			return err
		}
	}
	if floor > 0 && s.Owner != nil && s.Store != nil {
		o, _, err := s.Owner.ResolveRoute(ctx, sc)
		if err == nil && o.Epoch <= floor {
			s.Owner.Invalidate(sc)
			if err := s.Store.Forget(ctx, sc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) BeginResourceRequest(ctx context.Context, sc cell.Scope) (func(), error) {
	if s.Store == nil {
		return func() {}, nil
	}
	if s.resourceReplica() == nil {
		return s.Store.BeginRequestContext(ctx, sc.String())
	}
	return s.Store.BeginResourceRequest(ctx, sc, func() (string, error) { return s.resourceReplica().ResourceGeneration(ctx, sc) })
}
func (s *Server) activateResource(ctx context.Context, ns, kind, name string) error {
	if kind != "kv" && kind != "queue" {
		return nil
	}
	resource, err := s.Control.GetResource(ctx, ns, kind, name)
	if err != nil {
		return err
	}
	activeScope, err := control.ResourceDataScope(ns, kind, name, resource.Scope)
	if err != nil {
		// Existing registry validation permits inert cross-scope declarations;
		// data-plane resolution still rejects them. They cannot be reactivated.
		return nil
	}
	r, err := s.Control.ResourceRetirement(ctx, activeScope.String())
	if errors.Is(err, control.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	sc, err := cell.ParseScope(r.DataScope)
	if err != nil {
		return err
	}
	return s.resourceReplica().AllowResource(ctx, sc, r.JobID)
}
func (s *Server) revokeResource(ctx context.Context, ns, kind, name string, j control.ResourceCleanup) error {
	if kind != "kv" && kind != "queue" {
		return nil
	}
	// This exact intent was sampled under resourceMu before the control proof.
	// Looking it up again by name could substitute an unproven later incarnation.
	if j.JobID == "" {
		s.invalidateBindings(ns)
		return nil
	}
	if j.State == "complete" {
		return nil
	}
	s.invalidateBindings(ns)
	var leased control.ResourceCleanup
	var claimed bool
	if err := s.capturedWrite(ctx, control.ScopeFor(ns), func() error {
		var err error
		leased, claimed, err = s.Control.ClaimResourceCleanup(ctx, j)
		return err
	}); err != nil {
		return err
	}
	if !claimed {
		return control.ErrCleanupPending
	}
	_, prepareErr := s.prepareResourceRetirement(ctx, leased)
	// Publish pending state only after preparation has returned; no recursive
	// cleanup callback runs inside the captured control write.
	if err := s.capturedWrite(ctx, control.ScopeFor(ns), func() error {
		return s.Control.ContinueResourceCleanup(ctx, leased)
	}); err != nil {
		return err
	}
	return prepareErr
}

func (s *Server) RunResourceCleanupLoop(ctx context.Context) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := s.ResourceCleanupPass(ctx); err != nil {
				s.Log.Warn("resource cleanup failed", "err", err)
			}
		}
	}
}
func (s *Server) ResourceCleanupPass(ctx context.Context) error {
	// A pass can wait on control/cleanup I/O; do not queue another one behind it.
	// The periodic caller simply skips this tick and retries later.
	if !s.resourceCleanupMu.TryLock() {
		return nil
	}
	defer s.resourceCleanupMu.Unlock()
	if err := s.sweepLocalRetirementGates(ctx); err != nil && s.Log != nil {
		s.Log.Warn("local retirement gate reclamation pending", "err", err)
	}
	if s.Control == nil || s.Store == nil || s.resourceReplica() == nil {
		return nil
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, control.Scope())
		if err != nil || !s.ownerLive(ctx, o, time.Now()) {
			if err := s.invalidateStaleLocal(ctx, control.Scope()); err != nil {
				return err
			}
			o, err = s.Owner.Claim(ctx, control.Scope(), time.Now())
			if err != nil {
				return err
			}
		}
		if o.Node != s.Cfg.NodeID {
			return nil
		}
	}
	// Completed jobs are kept for idempotent retries, but not forever: prune
	// them hourly so the due-job scan stays bounded.
	if s.resourceCleanupPrunedAt.IsZero() || time.Since(s.resourceCleanupPrunedAt) >= time.Hour {
		if n, perr := s.Control.PruneCompletedResourceCleanups(ctx, time.Now().Add(-7*24*time.Hour).UnixMilli(), 100); perr == nil {
			s.resourceCleanupPrunedAt = time.Now()
			if n > 0 {
				s.Log.Info("resource cleanups pruned", "removed", n)
			}
		}
	}
	jobs, err := s.Control.PendingResourceCleanups(ctx, 20)
	if err != nil {
		return err
	}
	for _, candidate := range jobs {
		var j control.ResourceCleanup
		var claimed bool
		if err := s.capturedWrite(ctx, control.Scope(), func() error {
			var err error
			j, claimed, err = s.Control.ClaimResourceCleanup(ctx, candidate)
			return err
		}); err != nil {
			return err
		}
		if !claimed {
			continue
		}
		work, cancel := context.WithTimeout(ctx, 20*time.Second)
		done, err := s.cleanupResourceData(work, j)
		if err == nil && done && j.Kind == "queue" {
			for _, dlq := range j.DeadLetters {
				if err = s.removeQueueSource(work, j.Namespace, dlq, j.Name, j.SourceGeneration); err != nil {
					break
				}
			}
		}
		if err == nil && done {
			sc, parseErr := cell.ParseScope(j.DataScope)
			err = parseErr
			if err == nil {
				// Preparation can run at a remote owner. Its CAS cannot invalidate
				// this node's old fence cache, so persist a fresh retirement floor.
				var retired cell.Owner
				retired, _, err = s.Owner.Resolve(work, sc)
				if err == nil && (!retired.Retired || retired.RetirementJob != j.JobID) {
					err = control.ErrCleanupStale
				}
				j.RetiredEpoch = retired.RetiredEpoch
			}
		}
		cancel()
		if err == nil && !done {
			if err := s.capturedWrite(ctx, control.Scope(), func() error { return s.Control.ContinueResourceCleanup(ctx, j) }); err != nil {
				return err
			}
			continue
		}
		if finishErr := s.capturedWrite(ctx, control.Scope(), func() error { return s.Control.FinishResourceCleanup(ctx, j, err) }); finishErr != nil {
			return finishErr
		}
	}
	return s.sweepResourceRetirements(ctx)
}

func (s *Server) sweepResourceRetirements(ctx context.Context) error {
	rows, err := s.Control.ResourceRetirements(ctx, s.retirementAfter, 20)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		s.retirementAfter = ""
		return nil
	}
	for _, r := range rows {
		s.retirementAfter = r.DataScope
		sc, err := cell.ParseScope(r.DataScope)
		if err != nil {
			return err
		}
		next, sweepErr := s.resourceReplica().SweepRetired(ctx, sc, r.JobID, r.Epoch, r.Cursor)
		if next != r.Cursor || sweepErr != nil || r.LastError != "" {
			if err := s.capturedWrite(ctx, control.Scope(), func() error { return s.Control.UpdateResourceRetirement(ctx, r, next, sweepErr) }); err != nil {
				return err
			}
		}
		if sweepErr != nil {
			return sweepErr
		}
	}
	return nil
}

func (s *Server) cleanupResourceData(ctx context.Context, j control.ResourceCleanup) (bool, error) {
	prepared, err := s.prepareResourceRetirement(ctx, j)
	if err != nil {
		return false, err
	}
	if prepared.Node == s.Cfg.NodeID && prepared.Session == s.Owner.Session {
		return s.cleanupPreparedResource(ctx, j, prepared)
	}
	var out struct {
		Done bool `json:"done"`
	}
	in := resourceRetirementRequest{resourceCleanupValidation: resourceCleanupValidation{Job: j, LeaseToken: j.LeaseToken, LeaseUntilMs: j.LeaseUntilMs}, Expected: prepared}
	if err := s.cleanupRPC(ctx, prepared.Address, "/v1/internal/resource/cleanup-data", in, &out); err != nil {
		return false, err
	}
	if err := s.validateResourceCleanupAuthority(ctx, j); err != nil {
		return false, err
	}
	return out.Done, nil
}

func (s *Server) cleanupPreparedResource(ctx context.Context, j control.ResourceCleanup, expected cell.Owner) (bool, error) {
	work, cancel := context.WithDeadline(ctx, time.UnixMilli(j.LeaseUntilMs))
	defer cancel()
	prepared, err := s.prepareOwnedResourceRetirement(work, j, expected)
	if err != nil {
		return false, err
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return false, err
	}
	validate := func() error {
		if err := s.validateResourceCleanupAuthority(work, j); err != nil {
			return err
		}
		now, _, err := s.Owner.Resolve(work, sc)
		if err != nil {
			return err
		}
		if !sameRetirementSource(now, prepared, j.JobID) || !now.Retired || now.RetirementJob != j.JobID || now.RetiredEpoch < prepared.Epoch || !s.ackAllowed(time.Now()) {
			return owner.ErrEpochMismatch
		}
		return nil
	}
	var done bool
	err = s.Owner.WithResourceRetirement(work, sc, j.JobID, prepared, func(ctx context.Context, _ cell.Owner) error {
		// Gate is drained and capture has stopped. Invalidation deliberately
		// bypasses residency Pin, which cannot protect deleted user data.
		ok, err := s.Store.TryInvalidate(ctx, sc, validate)
		if err != nil {
			return err
		}
		if !ok {
			return control.ErrCleanupPending
		}
		if err := validate(); err != nil {
			return err
		}
		if s.Timers != nil {
			s.Timers.Remove(sc.String())
		}
		if s.Bucket != nil {
			if err := wake.New(s.Bucket).DeleteValidated(ctx, sc.String(), validate); err != nil {
				return err
			}
		}
		if err := validate(); err != nil {
			return err
		}
		done, err = s.resourceReplica().PurgeResource(ctx, sc, j.JobID, 500, func(context.Context) error { return validate() })
		if err != nil {
			return err
		}
		return validate()
	})
	if err != nil || !done {
		return done, err
	}
	if err := s.Owner.ReleaseForPurge(work, sc, prepared.Epoch); err != nil {
		return false, err
	}
	identity := retirementIdentity(j, prepared)
	err = s.Owner.WithReleasedResourceRetirement(work, sc, j.JobID, prepared, func(ctx context.Context) error {
		return s.Store.ReclaimResourceRetirement(ctx, sc, identity, func(ctx context.Context) error { return s.validateResourceCleanupAuthority(ctx, j) })
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) handleResourceCleanupData(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
	var in resourceRetirementRequest
	if err != nil || len(raw) > 64*1024 || json.Unmarshal(raw, &in) != nil {
		writeErr(w, 400, "bad_request", "invalid cleanup envelope")
		return
	}
	in.Job.LeaseToken, in.Job.LeaseUntilMs = in.LeaseToken, in.LeaseUntilMs
	done, err := s.cleanupPreparedResource(r.Context(), in.Job, in.Expected)
	if err != nil {
		writeErr(w, 503, "cleanup_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"done": done})
}
func (s *Server) cleanupRPC(ctx context.Context, address, path string, payload, out any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ownerclient.OwnerURL(ownerclient.Hint{Address: address})+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	if err := s.setInternalAuthorization(req, "internal"); err != nil {
		return err
	}
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("cleanup owner returned %d: %s", res.StatusCode, body)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}

func (s *Server) handleResourceCleanupStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, kind, name := q.Get("namespace"), q.Get("kind"), q.Get("name")
	if ns == "" || kind == "" || name == "" {
		writeErr(w, 400, "bad_request", "namespace, kind and name required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	j, err := s.Control.ResourceCleanup(r.Context(), ns, kind, name)
	if err != nil {
		writeErr(w, 404, "cleanup_not_found", err.Error())
		return
	}
	writeJSON(w, 200, j)
}
func (s *Server) handleResourceCleanupRetry(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, kind, name := q.Get("namespace"), q.Get("kind"), q.Get("name")
	if ns == "" || kind == "" || name == "" {
		writeErr(w, 400, "bad_request", "namespace, kind and name required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.Scope(), func() error { return s.Control.RetryResourceCleanup(r.Context(), ns, kind, name) }); err != nil {
		writeErr(w, 409, "cleanup_retry_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) removeQueueSource(ctx context.Context, ns, queue, source, generation string) error {
	sc := queuepkg.Scope(ns, queue)
	if blocked, err := s.resourceReplica().ResourceBlocked(ctx, sc); err != nil {
		return err
	} else if blocked {
		return nil
	}
	if err := s.PrepareResourceAccess(ctx, sc); err != nil {
		return err
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil || !s.ownerLive(ctx, o, time.Now()) {
			if err := s.invalidateStaleLocal(ctx, sc); err != nil {
				return err
			}
			o, err = s.Owner.Claim(ctx, sc, time.Now())
			if err != nil {
				return err
			}
		}
		if o.Node != s.Cfg.NodeID {
			return s.cleanupRPC(ctx, o.Address, "/v1/internal/queue/remove-source", map[string]string{"namespace": ns, "queue": queue, "source_queue": source, "source_generation": generation}, nil)
		}
	}
	end, err := s.BeginResourceRequest(ctx, sc)
	if err != nil {
		return err
	}
	defer end()
	return s.capturedWrite(ctx, sc, func() error { return s.Queue.RemoveSource(ctx, ns, queue, source, generation) })
}
func (s *Server) handleQueueRemoveSource(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16*1024))
	var body struct {
		Namespace  string `json:"namespace"`
		Queue      string `json:"queue"`
		Source     string `json:"source_queue"`
		Generation string `json:"source_generation"`
	}
	if err != nil || json.Unmarshal(raw, &body) != nil || body.Namespace == "" || body.Queue == "" || body.Source == "" {
		writeErr(w, 400, "bad_request", "invalid queue cleanup")
		return
	}
	if s.forwardOrClaim(w, r, queuepkg.Scope(body.Namespace, body.Queue), raw) {
		return
	}
	if err := s.removeQueueSource(r.Context(), body.Namespace, body.Queue, body.Source, body.Generation); err != nil {
		writeErr(w, 503, "cleanup_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
