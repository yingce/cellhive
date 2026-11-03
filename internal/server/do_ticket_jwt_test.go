package server

import (
	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/doticket"
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDOTicketPinsExecutionIdentity(t *testing.T) {
	s := New(Deps{Cfg: testAuthConfig(config.Config{})})
	req := doInvokeReq{Namespace: "acme", Worker: "web", Class: "RoomV2", BundleSHA: "bundle", StorageID: "incarnation", Version: 2}
	token, _ := s.doTicket(req, "Room", 3)
	key, err := s.jwtKey("token/do-ticket")
	if err != nil {
		t.Fatal(err)
	}
	got, err := doticket.Verify(key, token, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Class != req.Class || got.BundleSHA != req.BundleSHA || got.StorageID != req.StorageID || got.Version != req.Version || got.Limits != "null" {
		t.Fatal(got)
	}
}

func TestDOTicketClampsParentExpiry(t *testing.T) {
	s := New(Deps{Cfg: testAuthConfig(config.Config{})})
	now := time.Now().Unix()
	key, err := s.jwtKey("token/scope")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := bearertoken.Mint(key, "token/scope", bearertoken.Claims{Ver: 1, Type: "binding", Subject: "parent", Audience: "cellhive", IssuedAt: now, ExpiresAt: now + 5, Namespace: "acme", Kind: "do", Name: "ROOM"})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/do/invoke", nil)
	r.Header.Set("Authorization", "Bearer "+parent)
	token, exp := s.doTicket(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", BundleSHA: "bundle"}, "Room", 0, r)
	if token == "" || exp > (now+5)*1000 {
		t.Fatalf("ticket expiry exceeded parent: %d", exp)
	}
	r.Header.Set("Authorization", "Bearer invalid")
	if token, _ := s.doTicket(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", BundleSHA: "bundle"}, "Room", 0, r); token != "" {
		t.Fatal("invalid parent received ticket")
	}
}

func TestDOTicketAuthorizesStoredExecutionForInvokeAndConnect(t *testing.T) {
	s, _ := newTestServer(t)
	s.Owner = nil
	s.Control = control.New(s.Store, nil)
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "bundle", Bindings: []control.Binding{{Type: "do", Name: "ROOM", ID: "Room"}}}, "test"); err != nil {
		t.Fatal(err)
	}
	view, _, err := s.Control.WorkerEnv(ctx, "acme", "web")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*doInvokeReq){func(r *doInvokeReq) { r.BundleSHA = "other" }, func(r *doInvokeReq) { r.Class = "Other" }, func(r *doInvokeReq) { r.StorageID = "other" }, func(r *doInvokeReq) { r.StorageClass = "other" }, func(r *doInvokeReq) { r.Kind = "alarm" }} {
		r := doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", BundleSHA: view.BundleSHA, Version: view.Version, StorageID: view.StorageID}
		change(&r)
		request := httptest.NewRequest("POST", "/v1/do/invoke", nil)
		request = request.WithContext(context.WithValue(context.WithValue(request.Context(), ctxScopeNS, "acme"), ctxScopeName, "ROOM"))
		w := httptest.NewRecorder()
		if s.authorizeDOExecution(w, request, &r) || w.Code != 403 {
			t.Fatalf("allowed %+v status=%d", r, w.Code)
		}
	}
	r := doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", BundleSHA: view.BundleSHA, Version: view.Version}
	request := httptest.NewRequest("GET", "/v1/do/connect", nil)
	request = request.WithContext(context.WithValue(context.WithValue(request.Context(), ctxScopeNS, "acme"), ctxScopeName, "ROOM"))
	w := httptest.NewRecorder()
	if !s.authorizeDOExecution(w, request, &r) || r.StorageID != view.StorageID {
		t.Fatalf("stored identity not resolved: %+v status=%d body=%s", r, w.Code, w.Body.String())
	}
}

func TestDOExecutionFailsClosedWithoutControlOrForAlarm(t *testing.T) {
	s := New(Deps{Cfg: testAuthConfig(config.Config{})})
	for _, kind := range []string{"alarm", ""} {
		request := httptest.NewRequest("POST", "/v1/do/invoke", nil)
		request = request.WithContext(context.WithValue(request.Context(), ctxScopeNS, "acme"))
		w := httptest.NewRecorder()
		if s.authorizeDOExecution(w, request, &doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", Kind: kind}) {
			t.Fatal("allowed untrusted execution", kind)
		}
		if w.Code != 403 && w.Code != 503 {
			t.Fatal(w.Code)
		}
	}
}
