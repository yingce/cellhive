package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/replica"
)

// Control and capture must refer to exactly the same SQLite store.
func newResourceDeleteProofServer(t *testing.T) *Server {
	t.Helper()
	base := newControlServer(t)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return New(Deps{Cfg: base.Cfg, Store: cs, Control: control.New(cs, nil), Bucket: b, Log: base.Log})
}

func TestResourceDeleteProvesIntentBeforeCallback(t *testing.T) {
	for _, kind := range []string{"kv", "queue"} {
		t.Run(kind, func(t *testing.T) {
			s := newResourceDeleteProofServer(t)
			if _, err := s.Control.CreateResource(t.Context(), "acme", kind, "DATA", "physical", "ops"); err != nil {
				t.Fatal(err)
			}
			failed := errors.New("proof unavailable")
			s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error { return failed })}
			defer s.Capture.Stop()
			calls := 0
			s.Control.OnResourceRevoked = func(context.Context, string, string, string, control.ResourceCleanup) error {
				calls++
				return errors.New("remote unavailable")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			if err := s.Control.DeleteResource(ctx, "acme", kind, "DATA", "ops"); err == nil {
				t.Fatal("delete acknowledged failed proof")
			}
			if _, err := s.Control.ResourceCleanup(t.Context(), "acme", kind, "DATA"); err != nil {
				t.Fatalf("intent not committed: %v", err)
			}
			if calls != 0 {
				t.Fatalf("remote retirement ran before proof: %d", calls)
			}
		})
	}
}

func TestResourceDeleteRemoteFailureKeepsProvenJobAndUnlocks(t *testing.T) {
	for _, kind := range []string{"kv", "queue"} {
		t.Run(kind, func(t *testing.T) {
			s := newResourceDeleteProofServer(t)
			if _, err := s.Control.CreateResource(t.Context(), "acme", kind, "DATA", "physical", "ops"); err != nil {
				t.Fatal(err)
			}
			var proofs atomic.Int32
			s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error { proofs.Add(1); return nil })}
			defer s.Capture.Stop()
			remoteErr := errors.New("remote unavailable")
			var jobID string
			calls := 0
			s.Control.OnResourceRevoked = func(ctx context.Context, ns, k, name string, intent control.ResourceCleanup) error {
				if proofs.Load() == 0 {
					t.Error("callback before proof")
				}
				j, err := s.Control.ResourceCleanup(ctx, ns, k, name)
				if err != nil {
					return err
				}
				if jobID != "" && j.JobID != jobID {
					t.Error("retry changed job")
				}
				jobID = j.JobID
				calls++
				// Reenter an API acquiring resourceMu; formerly deadlocked under DeleteResource.
				if _, err := s.Control.CreateResource(ctx, ns, "d1", "OTHER", "other", "ops"); err != nil {
					return err
				}
				return remoteErr
			}
			done := make(chan error, 1)
			go func() {
				for i := 0; i < 2; i++ {
					if err := s.Control.DeleteResource(t.Context(), "acme", kind, "DATA", "ops"); !errors.Is(err, remoteErr) {
						done <- err
						return
					}
				}
				done <- remoteErr
			}()
			select {
			case err := <-done:
				if !errors.Is(err, remoteErr) {
					t.Fatalf("delete=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("callback retained resource lock")
			}
			if calls != 2 || jobID == "" {
				t.Fatalf("calls=%d job=%q", calls, jobID)
			}
		})
	}
}

func TestResourceDeleteEndpointRemoteFailureIsPendingOnEveryAttempt(t *testing.T) {
	for _, kind := range []string{"kv", "queue"} {
		t.Run(kind, func(t *testing.T) {
			s := newControlServer(t)
			if _, err := s.Control.CreateResource(t.Context(), "acme", kind, "DATA", "physical", "ops"); err != nil {
				t.Fatal(err)
			}
			s.Control.OnResourceRevoked = func(context.Context, string, string, string, control.ResourceCleanup) error {
				return errors.New("remote unavailable")
			}
			var id string
			for i := 0; i < 2; i++ {
				rr := adminDo(t, s.AdminHandler(), http.MethodDelete, "/v1/control/resource?namespace=acme&kind="+kind+"&name=DATA", "admin-tok", "")
				var body struct {
					Error   string `json:"error"`
					Cleanup struct {
						JobID string `json:"job_id"`
						State string `json:"state"`
					} `json:"cleanup"`
				}
				if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if rr.Code != 503 || body.Error != "resource_revocation_failed" || body.Cleanup.JobID == "" || body.Cleanup.State != "pending" {
					t.Fatalf("attempt %d: %d %s", i, rr.Code, rr.Body.String())
				}
				if id != "" && id != body.Cleanup.JobID {
					t.Fatal("retry changed job")
				}
				id = body.Cleanup.JobID
			}
		})
	}
}

func TestResourceDeleteIntentIndependentlyRestorableBeforeRetirement(t *testing.T) {
	s := newResourceDeleteProofServer(t)
	if _, err := s.Control.CreateResource(t.Context(), "acme", "kv", "DATA", "physical", "ops"); err != nil {
		t.Fatal(err)
	}
	rep := replica.New(s.Bucket)
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: replicaCommitter{rep: rep}}
	defer s.Capture.Stop()
	remoteErr := errors.New("retirement RPC unavailable")
	var jobID string
	s.Control.OnResourceRevoked = func(ctx context.Context, ns, kind, name string, intent control.ResourceCleanup) error {
		local, err := s.Control.ResourceCleanup(ctx, ns, kind, name)
		if err != nil {
			return err
		}
		restored := restoreDB(t, rep, control.Scope(), 1)
		var recovered string
		if err := restored.QueryRow("SELECT job_id FROM resource_cleanups WHERE ns=? AND kind=? AND name=?", ns, kind, name).Scan(&recovered); err != nil {
			t.Fatal(err)
		}
		if recovered != local.JobID || recovered == "" {
			t.Fatalf("restored job=%q local=%q", recovered, local.JobID)
		}
		var registered int
		if err := restored.QueryRow("SELECT count(*) FROM resources WHERE ns=? AND kind=? AND name=?", ns, kind, name).Scan(&registered); err != nil {
			t.Fatal(err)
		}
		if registered != 0 {
			t.Fatal("registry revocation not durable before retirement")
		}
		jobID = recovered
		return remoteErr
	}
	if err := s.Control.DeleteResource(t.Context(), "acme", "kv", "DATA", "ops"); !errors.Is(err, remoteErr) {
		t.Fatalf("delete=%v", err)
	}
	if jobID == "" {
		t.Fatal("retirement callback not reached")
	}
}

// A proof covers the intent sampled before it; a later control incarnation may
// not be substituted by looking up the resource name again after that proof.
func TestResourceDeleteBindsCallbackToProvenCleanupIdentity(t *testing.T) {
	s := newResourceDeleteProofServer(t)
	if _, err := s.Control.CreateResource(t.Context(), "acme", "kv", "DATA", "physical", "ops"); err != nil {
		t.Fatal(err)
	}
	var proven string
	s.Control.BeforeResourceRevoked = func(ctx context.Context, ns string) error {
		j, err := s.Control.ResourceCleanup(ctx, ns, "kv", "DATA")
		if err != nil {
			return err
		}
		proven = j.JobID
		c, err := s.Store.Cell(ctx, control.Scope())
		if err != nil {
			return err
		}
		_, err = c.DB.ExecContext(ctx, "UPDATE resource_cleanups SET job_id=? WHERE ns=? AND kind=? AND name=?", "later-unproven", ns, "kv", "DATA")
		return err
	}
	if err := s.Control.DeleteResource(t.Context(), "acme", "kv", "DATA", "ops"); !errors.Is(err, control.ErrCleanupPending) {
		t.Fatalf("changed intent must not acquire cleanup lease: %v", err)
	}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "physical"}
	// A blocked fence still exposes its exact generation through ResourceSnapshot.
	generation, blocked, _, err := s.resourceReplica().ResourceSnapshot(t.Context(), sc)
	if err != nil || blocked || generation != "" || proven == "" {
		t.Fatalf("retired unproven intent: generation=%s proven=%s blocked=%t err=%v", generation, proven, blocked, err)
	}
}
