package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/control"
)

func TestWorkerExecutionViewUsesVersionSecretSnapshot(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.Control.PutSecret(ctx, "acme", "api", "TOKEN", []byte("first"), "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.Control.PutSecret(ctx, "acme", "api", "TOKEN", []byte("second"), "ops"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		version string
		want    string
	}{
		{"1", ""}, {"2", "first"}, {"3", "second"},
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/control/worker?ns=acme&worker=api&version="+tc.version, nil)
		req.Header.Set("x-cellhive-internal-token", "tok")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("version %s: HTTP %d: %s", tc.version, rec.Code, rec.Body.String())
		}
		var view control.WorkerView
		if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		if got := view.Vars["TOKEN"]; got != tc.want {
			t.Fatalf("version %s TOKEN=%q, want %q", tc.version, got, tc.want)
		}
		bindingReq := httptest.NewRequest(http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=api&version="+tc.version, nil)
		bindingReq.Header.Set("x-cellhive-internal-token", "tok")
		bindingRec := httptest.NewRecorder()
		s.Handler().ServeHTTP(bindingRec, bindingReq)
		if bindingRec.Code != http.StatusOK {
			t.Fatalf("version %s bindings: HTTP %d: %s", tc.version, bindingRec.Code, bindingRec.Body.String())
		}
		var bindingView struct {
			Vars map[string]string `json:"vars"`
		}
		if err := json.Unmarshal(bindingRec.Body.Bytes(), &bindingView); err != nil {
			t.Fatal(err)
		}
		if got := bindingView.Vars["TOKEN"]; got != tc.want {
			t.Fatalf("version %s binding TOKEN=%q, want %q", tc.version, got, tc.want)
		}
	}
}

func TestCloudflareSecretPutDeploysNewWorkerVersion(t *testing.T) {
	s := newControlServer(t)
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/api/secrets", bytes.NewBufferString(`{"name":"TOKEN","text":"new","type":"secret_text"}`))
	req.Header.Set("Authorization", "Bearer admin-tok")
	rec := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("CF secret put: HTTP %d: %s", rec.Code, rec.Body.String())
	}
	view, ok, err := s.Control.WorkerEnv(ctx, "acme", "api")
	if err != nil || !ok || view.Version != 2 || view.BundleSHA != "sha" {
		t.Fatalf("secret publication: %+v ok=%v err=%v", view, ok, err)
	}
	deleteReq := httptest.NewRequest(http.MethodDelete, "/client/v4/accounts/acme/workers/scripts/api/secrets/TOKEN", nil)
	deleteReq.Header.Set("Authorization", "Bearer admin-tok")
	deleteRec := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("CF secret delete: HTTP %d: %s", deleteRec.Code, deleteRec.Body.String())
	}
	view, ok, err = s.Control.WorkerEnv(ctx, "acme", "api")
	if err != nil || !ok || view.Version != 3 {
		t.Fatalf("secret deletion publication: %+v ok=%v err=%v", view, ok, err)
	}
}
