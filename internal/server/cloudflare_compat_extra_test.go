package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCloudflareTokenVerifyAndBetaWorker(t *testing.T) {
	s := newControlServer(t)
	do := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, req)
		return rr
	}
	rr := do(http.MethodGet, "/client/v4/user/tokens/verify")
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"status":"active"`)) {
		t.Fatalf("verify: %d %s", rr.Code, rr.Body.String())
	}
	rr = do(http.MethodGet, "/client/v4/accounts/acme/workers/workers/web")
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"subdomain"`)) {
		t.Fatalf("beta worker get: %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareHyperdriveCRUD(t *testing.T) {
	s := newControlServer(t)
	doJSON := func(method, path string, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, req)
		return rr
	}
	create := doJSON(http.MethodPost, "/client/v4/accounts/acme/hyperdrive/configs",
		`{"name":"hd1","origin":{"scheme":"postgres","host":"db.internal","port":5432,"database":"app","user":"app","password":"secret"}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create: %d %s", create.Code, create.Body.String())
	}
	if bytes.Contains(create.Body.Bytes(), []byte("secret")) {
		t.Fatalf("create leaked password: %s", create.Body.String())
	}
	rr := doJSON(http.MethodGet, "/client/v4/accounts/acme/hyperdrive/configs", "")
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"hd1"`)) {
		t.Fatalf("list: %d %s", rr.Code, rr.Body.String())
	}
	rr = doJSON(http.MethodGet, "/client/v4/accounts/acme/hyperdrive/configs/hd1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rr.Code, rr.Body.String())
	}
	rr = doJSON(http.MethodDelete, "/client/v4/accounts/acme/hyperdrive/configs/hd1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rr.Code, rr.Body.String())
	}
	rr = doJSON(http.MethodGet, "/client/v4/accounts/acme/hyperdrive/configs/hd1", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d %s", rr.Code, rr.Body.String())
	}
	var v any
	if err := json.Unmarshal(create.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
}
