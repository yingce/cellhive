package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
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

func TestCloudflareLegacyScriptUploadAndPreviewMetadata(t *testing.T) {
	s := newControlServer(t)
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, _ := w.CreateFormField("metadata")
	_, _ = part.Write([]byte(`{"main_module":"worker.js","compatibility_date":"2026-09-23","migrations":{"new_tag":"v1","steps":[{"new_classes":["LegacyDO"]}]}}`))
	part, _ = w.CreateFormFile("worker.js", "worker.js")
	_, _ = part.Write([]byte("export default { fetch() { return new Response('legacy-ok') } }"))
	_ = w.Close()
	req := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/legacy", &body)
	req.Header.Set("Authorization", "Bearer admin-tok")
	req.Header.Set("Content-Type", w.FormDataContentType())
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("legacy put: %d %s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"has_preview":false`)) {
		t.Fatalf("legacy response missing has_preview: %s", rr.Body.String())
	}
	if _, ok, err := s.Control.WorkerEnv(t.Context(), "acme", "legacy"); err != nil || !ok {
		t.Fatalf("legacy upload not deployed: ok=%v err=%v", ok, err)
	}
}

func TestDecodeRunWorkerFirstShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		flag bool
		path string
	}{
		{"bool", `true`, true, ""},
		{"array", `["/api/*","/health"]`, false, "/api/*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out cloudflareWorkerMetadata
			if err := decodeCloudflareMetadata([]byte(`{"assets":{"jwt":"x","config":{"run_worker_first":`+tc.raw+`}}}`), &out); err != nil {
				t.Fatal(err)
			}
			if out.Assets == nil || out.Assets.RunWorkerFirst != tc.flag {
				t.Fatalf("assets=%+v", out.Assets)
			}
			if tc.path != "" && (len(out.Assets.RunWorkerFirstPaths) != 2 || out.Assets.RunWorkerFirstPaths[0] != tc.path) {
				t.Fatalf("paths=%v", out.Assets.RunWorkerFirstPaths)
			}
		})
	}
}
