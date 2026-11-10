package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

func TestResourceMaintenanceRejectsClosedAdmission(t *testing.T) {
	for _, operation := range []string{"expiry", "kv-stats", "queue-stats", "queue-purge"} {
		t.Run(operation, func(t *testing.T) {
			s := newDomainServer(t)
			t.Cleanup(func() { _ = s.Store.Close() })
			kind := "kv"
			if operation == "queue-stats" || operation == "queue-purge" {
				kind = "queue"
			}
			sc := cell.Scope{Namespace: "acme", Class: "__" + kind + "__", ID: "data"}
			if _, err := s.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Control.CreateResource(t.Context(), "acme", kind, "data", sc.String(), "test"); err != nil {
				t.Fatal(err)
			}
			if kind == "queue" {
				if _, err := s.Queue.Send(t.Context(), "acme", "data", []byte("message"), "text", 0, ""); err != nil {
					t.Fatal(err)
				}
			}
			id := cellstore.ResourceRetirementIdentity{JobID: "pending", Node: "node", Session: "session", Epoch: 1}
			if err := s.Store.BeginResourceRetirement(t.Context(), sc, id); err != nil {
				t.Fatal(err)
			}
			var rr *httptest.ResponseRecorder
			switch operation {
			case "expiry":
				rr = httptest.NewRecorder()
				s.handleKVExpire(rr, httptest.NewRequest(http.MethodPost, "/expire?scope="+sc.String(), nil))
			case "kv-stats", "queue-stats":
				rr = adminDo(t, s.AdminHandler(), http.MethodGet, "/v1/"+kind+"/stats?namespace=acme&name=data", "admin-tok", "")
			case "queue-purge":
				req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/queues/data/purge", nil)
				req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "admin"))
				rr = httptest.NewRecorder()
				s.AdminHandler().ServeHTTP(rr, req)
			}
			if rr.Code < 400 {
				t.Fatalf("retiring operation admitted: %d %s", rr.Code, rr.Body.String())
			}
			if kind == "queue" {
				if depth, err := s.Queue.Depth(t.Context(), "acme", "data"); err != nil || depth != 1 {
					t.Fatalf("closed admission mutated queue: %d %v", depth, err)
				}
			}
		})
	}
}
