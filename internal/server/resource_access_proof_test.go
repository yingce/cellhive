package server

import (
	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"cellhive/internal/scopedtoken"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newResourceProofServer(t *testing.T) (*Server, cell.Scope, *lease.Manager, string) {
	t.Helper()
	s := newDomainServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "KV"}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "kv", "KV", sc.String(), "test"); err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(s.Bucket, s.Owner.NodeID, s.Owner.Session, "", "", time.Minute)
	s.Owner.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Owner.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	token, err := testBindingJWT(s, scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "KV"})
	if err != nil {
		t.Fatal(err)
	}
	return s, sc, lm, token
}
func proofRequest(token string) *http.Request {
	r := httptest.NewRequest("GET", "/v1/kv/get?ns=acme&key=k", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}
func TestResourceProofAdmissionSkipsFenceReads(t *testing.T) {
	s, sc, _, token := newResourceProofServer(t)
	b := &fenceCountingBucket{Bucket: s.resourceReplica().B}
	s.resourceReplica().B = b
	handler := s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) {
		if p := resourceProof(r.Context(), sc); p == nil || !p.Validate() {
			t.Error("missing retained request proof")
		}
		w.Write([]byte("current"))
	})
	for i := 0; i < 3; i++ {
		rr := httptest.NewRecorder()
		handler(rr, proofRequest(token))
		if rr.Code != 200 || rr.Body.String() != "current" {
			t.Fatalf("%d %s", rr.Code, rr.Body.String())
		}
	}
	if n := b.gets.Load(); n != 0 {
		t.Fatalf("local proof used %d fence GETs", n)
	}
}
func TestResourceProofRejectsResponseAfterAuthorityChange(t *testing.T) {
	for _, mode := range []string{"invalidate", "health-gap"} {
		t.Run(mode, func(t *testing.T) {
			s, _, lm, token := newResourceProofServer(t)
			handler := s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) {
				if mode == "invalidate" {
					s.Owner.Invalidate(cell.Scope{Namespace: "acme", Class: "__kv__", ID: "KV"})
				} else {
					lm.Healthy(lm.HealthyUntil())
					if err := lm.Publish(r.Context(), lease.Load{}); err != nil {
						t.Error(err)
					}
				}
				w.Header().Set("Content-Length", "9")
				w.Write([]byte("old-value"))
			})
			rr := httptest.NewRecorder()
			handler(rr, proofRequest(token))
			if rr.Code != 503 || strings.Contains(rr.Body.String(), "old-value") || rr.Header().Get("Content-Length") == "9" {
				t.Fatalf("stale response: %d %s", rr.Code, rr.Body.String())
			}
		})
	}
}
func TestResourceProofRejectsBetweenPreparationAndAdmission(t *testing.T) {
	s, sc, _, _ := newResourceProofServer(t)
	p, ok := s.Owner.LocalResourceProof(sc)
	if !ok {
		t.Fatal("missing proof")
	}
	defer p.Release()
	ctx := context.WithValue(t.Context(), resourceProofKey{}, &resourceAccessProof{scope: sc, proof: p})
	g, err := s.prepareResourceAccessSnapshot(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	s.Owner.Invalidate(sc)
	end, err := s.beginPreparedResourceRequest(ctx, sc, g)
	if end != nil {
		end()
	}
	if err == nil {
		t.Fatal("stale proof admitted")
	}
}

func TestResourceProofRetirementDrainsResponse(t *testing.T) {
	s, sc, _, token := newResourceProofServer(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handler := s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-proceed:
		case <-ctx.Done():
			return
		}
		w.Write([]byte("before-delete"))
	})
	rr := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() { defer close(requestDone); handler(rr, proofRequest(token).WithContext(ctx)) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	deleted := make(chan error, 1)
	go func() { deleted <- s.Control.DeleteResource(ctx, "acme", "kv", "KV", "test") }()
	for {
		_, exists, err := s.Store.ResourceRetirementIdentity(sc)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			break
		}
		select {
		case err := <-deleted:
			t.Fatalf("delete completed before drain: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	current, _, err := s.Owner.Resolve(ctx, sc)
	if err != nil || current.Retired {
		t.Fatalf("published retirement with active request: %+v %v", current, err)
	}
	close(proceed)
	<-requestDone
	if rr.Code != 200 || rr.Body.String() != "before-delete" {
		t.Fatalf("admitted response failed: %d %s", rr.Code, rr.Body.String())
	}
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Owner.LocalResourceProof(sc); ok {
		t.Fatal("retired scope granted proof")
	}
	denied := httptest.NewRecorder()
	s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) { t.Error("deleted handler entered") })(denied, proofRequest(token))
	if denied.Code < 400 {
		t.Fatalf("deleted request accepted: %d", denied.Code)
	}
}
