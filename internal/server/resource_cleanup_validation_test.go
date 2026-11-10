package server

import (
	"bytes"
	"cellhive/internal/bucket"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

func leasedCleanupForValidation(t *testing.T, s *Server) control.ResourceCleanup {
	t.Helper()
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "test"); err != nil {
		t.Fatal(err)
	}
	// These tests stage the transport only; do not invoke legacy revocation.
	s.Control.OnResourceRevoked = nil
	if err := s.Control.DeleteResource(ctx, "acme", "kv", "CACHE", "test"); err != nil {
		t.Fatal(err)
	}
	j, err := s.Control.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if err != nil {
		t.Fatal(err)
	}
	j, ok, err := s.Control.ClaimResourceCleanup(ctx, j)
	if err != nil || !ok {
		t.Fatalf("claim cleanup: %v %v", ok, err)
	}
	return j
}

func validationRequest(t *testing.T, s *Server, in any, authenticated bool) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/internal/resource/cleanup-validate", bytes.NewReader(raw))
	if authenticated {
		r.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestResourceCleanupValidationLocalAndRemoteAuthority(t *testing.T) {
	authoritative, follower, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authoritative)
	if _, err := follower.Control.ResourceCleanup(t.Context(), j.Namespace, j.Kind, j.Name); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("follower must lack job: %v", err)
	}
	for _, s := range []*Server{authoritative, follower} {
		if err := s.validateResourceCleanupAuthority(t.Context(), j); err != nil {
			t.Fatalf("%s validation: %v", s.Cfg.NodeID, err)
		}
	}
	dataScope, _ := cell.ParseScope(j.DataScope)
	if _, _, err := authoritative.Owner.Resolve(t.Context(), dataScope); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("validation claimed data scope: %v", err)
	}
	// Public status cannot accidentally be reused as a credential-bearing RPC.
	if got := validationRequest(t, authoritative, j, true); got.Code != 400 {
		t.Fatalf("public job accepted: %d %s", got.Code, got.Body.String())
	}
	in := resourceCleanupValidation{Job: j, LeaseToken: j.LeaseToken, LeaseUntilMs: j.LeaseUntilMs}
	if got := validationRequest(t, follower, in, true); got.Code != 503 {
		t.Fatalf("wrong owner accepted: %d %s", got.Code, got.Body.String())
	}
	if got := validationRequest(t, authoritative, in, false); got.Code == 200 {
		t.Fatal("unauthenticated validation accepted")
	}
	// Reclaiming the same NodeID with a different process session is not local authority.
	authoritative.Owner.Session = "successor-session"
	if err := authoritative.validateResourceCleanupAuthority(t.Context(), j); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("wrong session accepted: %v", err)
	}
}

func TestResourceCleanupValidationRejectsIdentityAndLease(t *testing.T) {
	s, follower, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, s)
	tests := map[string]func(*control.ResourceCleanup){
		"missing token":    func(j *control.ResourceCleanup) { j.LeaseToken = "" },
		"wrong token":      func(j *control.ResourceCleanup) { j.LeaseToken = "wrong" },
		"missing deadline": func(j *control.ResourceCleanup) { j.LeaseUntilMs = 0 },
		"expired":          func(j *control.ResourceCleanup) { j.LeaseUntilMs = time.Now().Add(-time.Second).UnixMilli() },
		"wrong deadline":   func(j *control.ResourceCleanup) { j.LeaseUntilMs++ },
		"missing job":      func(j *control.ResourceCleanup) { j.JobID = "" },
		"wrong job":        func(j *control.ResourceCleanup) { j.JobID = "wrong" },
		"wrong namespace":  func(j *control.ResourceCleanup) { j.Namespace = "other" },
		"wrong kind":       func(j *control.ResourceCleanup) { j.Kind = "queue" },
		"wrong data scope": func(j *control.ResourceCleanup) { j.DataScope = "acme/__kv__/other" },
		"wrong generation": func(j *control.ResourceCleanup) { j.SourceGeneration = "wrong" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			bad := j
			mutate(&bad)
			if err := follower.validateResourceCleanupAuthority(t.Context(), bad); err == nil {
				t.Fatal("invalid job accepted remotely")
			}
			in := resourceCleanupValidation{Job: bad, LeaseToken: bad.LeaseToken, LeaseUntilMs: bad.LeaseUntilMs}
			if got := validationRequest(t, s, in, true); got.Code == 200 {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
	if err := s.Control.FinishResourceCleanup(t.Context(), j, nil); err != nil {
		t.Fatal(err)
	}
	if err := follower.validateResourceCleanupAuthority(t.Context(), j); err == nil {
		t.Fatal("completed job accepted")
	}
}

func TestResourceCleanupValidationUnownedControlFailsClosed(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "unowned")
	j := leasedCleanupForValidation(t, s)
	if err := s.validateResourceCleanupAuthority(t.Context(), j); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("unowned control accepted: %v", err)
	}
	if _, _, err := s.Owner.Resolve(t.Context(), control.Scope()); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("validation claimed control: %v", err)
	}
	s.Owner = nil
	if err := s.validateResourceCleanupAuthority(t.Context(), j); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("ownerless local SQLite accepted: %v", err)
	}
}

// Force an ownership transition observation precisely after the SQLite check.
type cleanupValidationOwnerChangeBucket struct {
	bucket.Bucket
	reads atomic.Int32
}

func (b *cleanupValidationOwnerChangeBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	raw, etag, err := b.Bucket.Get(ctx, key)
	if err == nil && key == owner.Key(control.Scope()) && b.reads.Add(1) == 2 {
		o, parseErr := cell.UnmarshalOwner(raw)
		if parseErr != nil {
			return nil, "", parseErr
		}
		o.Epoch++
		raw, err = json.Marshal(o)
	}
	return raw, etag, err
}
func TestResourceCleanupValidationRejectsOwnerChangeAfterRead(t *testing.T) {
	s, _, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, s)
	s.Owner.B = &cleanupValidationOwnerChangeBucket{Bucket: s.Owner.B}
	if err := s.validateLocalResourceCleanupAuthority(t.Context(), j); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("owner change accepted: %v", err)
	}
}
func TestResourceCleanupValidationUnreachableOwnerFailsClosed(t *testing.T) {
	s, follower, hs := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, s)
	hs.Close()
	if err := follower.validateResourceCleanupAuthority(t.Context(), j); err == nil {
		t.Fatal("unreachable control owner accepted")
	}
}
