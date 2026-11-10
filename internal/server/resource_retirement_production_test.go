package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestProductionRetirementReclaimsGatesAcrossRecreation(t *testing.T) {
	s := newDomainServer(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "physical"}
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	var old resourceRetirementRequest
	for cycle := 0; cycle < 5; cycle++ {
		if _, err := s.Control.CreateResource(t.Context(), "acme", "kv", "CACHE", sc.String(), "test"); err != nil {
			t.Fatal(err)
		}
		if err := s.PrepareResourceAccess(t.Context(), sc); err != nil {
			t.Fatal(err)
		}
		o, err := s.Owner.Claim(t.Context(), sc, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.Store.Cell(t.Context(), sc)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Put(t.Context(), "current", []byte("value"), nil); err != nil {
			t.Fatal(err)
		}
		if cycle > 0 {
			raw, _ := json.Marshal(old)
			req := httptest.NewRequest(http.MethodPost, "/v1/internal/resource/cleanup-data", bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code < 400 {
				t.Fatal("stale cleanup accepted after recreation")
			}
			if n, _ := s.Store.ResourceRetirementStats(); n != 0 {
				t.Fatal("stale cleanup gated recreated resource")
			}
			value, _, err := c.Get(t.Context(), "current")
			if err != nil || string(value) != "value" {
				t.Fatalf("stale cleanup removed new data: %q %v", value, err)
			}
		}
		if err := s.Control.DeleteResource(t.Context(), "acme", "kv", "CACHE", "test"); err != nil {
			t.Fatal(err)
		}
		retired, _, err := s.Owner.Resolve(t.Context(), sc)
		if err != nil || !retired.Retired || retired.Epoch != o.Epoch {
			t.Fatalf("delete did not retire: %+v %v", retired, err)
		}
		if n, _ := s.Store.ResourceRetirementStats(); n != 1 {
			t.Fatalf("prepared gates=%d", n)
		}
		job, err := s.Control.ResourceCleanup(t.Context(), "acme", "kv", "CACHE")
		if err != nil {
			t.Fatal(err)
		}
		if job.State != "pending" {
			t.Fatalf("DELETE must not report physical completion: %+v", job)
		}
		// Capture a real lease envelope to replay after this cycle completes.
		job, ok, err := s.Control.ClaimResourceCleanup(t.Context(), job)
		if err != nil || !ok {
			t.Fatalf("claim %v %v", ok, err)
		}
		old = resourceRetirementRequest{resourceCleanupValidation: resourceCleanupValidation{Job: job, LeaseToken: job.LeaseToken, LeaseUntilMs: job.LeaseUntilMs}, Expected: retired}
		if err := s.Control.ContinueResourceCleanup(t.Context(), job); err != nil {
			t.Fatal(err)
		}
		if err := s.ResourceCleanupPass(t.Context()); err != nil {
			t.Fatal(err)
		}
		job, err = s.Control.ResourceCleanup(t.Context(), "acme", "kv", "CACHE")
		if err != nil || job.State != "complete" {
			t.Fatalf("cleanup %+v %v", job, err)
		}
		if n, _ := s.Store.ResourceRetirementStats(); n != 0 {
			t.Fatalf("completed cleanup leaked %d gates", n)
		}
		after, _, err := s.Owner.Resolve(t.Context(), sc)
		if err != nil || after.Node != "" || !after.Retired || after.RetiredEpoch < o.Epoch {
			t.Fatalf("release %+v %v", after, err)
		}
		lineage, err := s.Control.ResourceRetirement(t.Context(), sc.String())
		if err != nil || lineage.Epoch < o.Epoch {
			t.Fatalf("retirement floor %+v %v", lineage, err)
		}
	}
}
