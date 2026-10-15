package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/ownerclient"
	queuepkg "cellhive/internal/queue"
	"cellhive/internal/replica"
	"cellhive/internal/wake"
)

func (s *Server) resourceReplica() *replica.Manager { return s.resourceRep }

// PrepareResourceAccess runs outside a cell's BeginRequest gate. The epoch
// floor also invalidates an old self-owner hint and local cache on a node that
// was offline during deletion, so recreation cannot expose its old SQLite data.
func (s *Server) PrepareResourceAccess(ctx context.Context, sc cell.Scope) error {
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
		o, _, err := s.Owner.ResolveCached(ctx, sc, ownerCacheTTL)
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
		return s.Store.BeginRequest(sc.String()), nil
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
func (s *Server) revokeResource(ctx context.Context, ns, kind, name string) error {
	if kind != "kv" && kind != "queue" {
		return nil
	}
	j, err := s.Control.ResourceCleanup(ctx, ns, kind, name)
	if err != nil {
		return err
	}
	if j.State == "complete" {
		return nil
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return err
	}
	s.invalidateBindings(ns)
	return s.resourceReplica().BlockResource(ctx, sc, j.JobID)
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
	s.resourceCleanupMu.Lock()
	defer s.resourceCleanupMu.Unlock()
	if s.Control == nil || s.Store == nil || s.resourceReplica() == nil {
		return nil
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, control.Scope())
		if err != nil || o.Expired(time.Now()) {
			o, err = s.Owner.Claim(ctx, control.Scope(), time.Now())
			if err != nil {
				return err
			}
		}
		if o.Node != s.Cfg.NodeID {
			return nil
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
		err := s.Control.ValidateResourceCleanupLease(work, j)
		if err == nil {
			sc, parseErr := cell.ParseScope(j.DataScope)
			err = parseErr
			if err == nil {
				err = s.resourceReplica().BlockResource(work, sc, j.JobID)
			}
		}
		done := false
		if err == nil {
			done, err = s.cleanupResourceData(work, j)
		}
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
				_, j.RetiredEpoch, err = s.resourceReplica().ResourceState(work, sc)
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
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil {
		return false, err
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil || o.Expired(time.Now()) {
			o, err = s.Owner.Claim(ctx, sc, time.Now())
			if err != nil {
				return false, err
			}
		}
		if o.Node != s.Cfg.NodeID {
			var out struct {
				Done bool `json:"done"`
			}
			err := s.cleanupRPC(ctx, o.Address, "/v1/internal/resource/cleanup-data", j, &out)
			return out.Done, err
		}
	}
	return s.cleanupOwnedResource(ctx, j)
}
func (s *Server) cleanupOwnedResource(ctx context.Context, j control.ResourceCleanup) (bool, error) {
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil || sc.Namespace != j.Namespace || (j.Kind != "kv" && j.Kind != "queue") || sc.Class != "__"+j.Kind+"__" {
		return false, fmt.Errorf("invalid resource cleanup identity")
	}
	var epoch uint64
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil {
			return false, err
		}
		if o.Node != s.Cfg.NodeID || o.Expired(time.Now()) {
			return false, fmt.Errorf("cleanup scope owner changed")
		}
		epoch = o.Epoch
	} else {
		latest, _, err := s.resourceReplica().LatestEpoch(ctx, sc, 0)
		if err != nil {
			return false, err
		}
		epoch = latest
	}
	if err := s.resourceReplica().FenceResource(ctx, sc, j.JobID, epoch); err != nil {
		return false, err
	}
	// Stop capture/drain requests before removing files. The persistent fence
	// rejects queued uploads and old-epoch compaction that outlive the handle.
	if err := s.Store.ForgetWithVerify(ctx, sc, func() error { return s.resourceReplica().ValidateResourceDelete(ctx, sc, j.JobID) }); err != nil {
		return false, err
	}
	if s.Timers != nil {
		s.Timers.Remove(sc.String())
	}
	if s.Bucket != nil {
		if err := wake.New(s.Bucket).Delete(ctx, sc.String()); err != nil {
			return false, err
		}
	}
	done, err := s.resourceReplica().PurgeResource(ctx, sc, j.JobID, 500)
	if err != nil || !done {
		return done, err
	}
	if s.Owner != nil {
		if err := s.Owner.Release(ctx, sc, epoch); err != nil {
			return false, err
		}
	}
	return true, nil
}
func (s *Server) handleResourceCleanupData(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	var j control.ResourceCleanup
	if err != nil || json.Unmarshal(raw, &j) != nil {
		writeErr(w, 400, "bad_request", "invalid cleanup job")
		return
	}
	sc, err := cell.ParseScope(j.DataScope)
	if err != nil || sc.Namespace != j.Namespace {
		writeErr(w, 400, "bad_request", "invalid cleanup scope")
		return
	}
	if s.forwardOrClaim(w, r, sc, raw) {
		return
	}
	done, err := s.cleanupOwnedResource(r.Context(), j)
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
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
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
		if err != nil || o.Expired(time.Now()) {
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
