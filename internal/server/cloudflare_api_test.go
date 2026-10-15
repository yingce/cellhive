package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"cellhive/internal/control"
	"cellhive/internal/logbuf"
	"github.com/gorilla/websocket"
)

func TestCloudflareWorkerVersionsUploadDeploys(t *testing.T) {
	s := newControlServer(t)
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, _ := w.CreateFormField("metadata")
	_, _ = part.Write([]byte(`{"main_module":"worker.js","compatibility_date":"2026-09-23"}`))
	part, _ = w.CreateFormFile("worker.js", "worker.js")
	_, _ = part.Write([]byte("export default { fetch() { return new Response('ok') } }"))
	_ = w.Close()
	req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/versions", &body)
	req.Header.Set("Authorization", "Bearer admin-tok")
	req.Header.Set("Content-Type", w.FormDataContentType())
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"success":true`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"version_id"`)) {
		t.Fatalf("response=%s", rr.Body.String())
	}
	releases, err := s.Control.Releases(t.Context(), "acme", "web")
	if err != nil || len(releases) != 1 {
		t.Fatalf("worker was not deployed: releases=%d err=%v", len(releases), err)
	}
	if rr := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/web/versions", nil)
		req.Header.Set("Authorization", "Bearer admin-tok")
		out := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(out, req)
		return out
	}(); rr.Code != 200 {
		t.Fatalf("versions list %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareResourcesSecretsAndRoutes(t *testing.T) {
	s := newControlServer(t)
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/storage/kv/namespaces", `{"title":"sessions"}`); rr.Code != 200 {
		t.Fatalf("kv create %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/storage/kv/namespaces", ""); rr.Code != 200 {
		t.Fatalf("kv list %d", rr.Code)
	}
	if rr := call(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/web/secrets", `{"name":"TOKEN","text":"secret","type":"secret_text"}`); rr.Code != 200 {
		t.Fatalf("secret %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/web/secrets", ""); rr.Code != 200 {
		t.Fatalf("secret list %d", rr.Code)
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/workers/routes", `{"pattern":"https://example.com/*","script":"web"}`); rr.Code != 200 {
		t.Fatalf("route %d %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(call(http.MethodGet, "/client/v4/accounts/acme/workers/routes", "").Body.Bytes(), &got)
	if got["success"] != true {
		t.Fatalf("routes response=%v", got)
	}
}

func TestCloudflareAccountsDeleteAndVersionDeploy(t *testing.T) {
	s := newControlServer(t)
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := call(http.MethodGet, "/client/v4/accounts", ""); rr.Code != 200 {
		t.Fatalf("accounts %d %s", rr.Code, rr.Body.String())
	}
	// Upload two versions through the native control endpoint, then publish the
	// first one via the Cloudflare versions API.
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	for _, sha := range []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"} {
		if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: sha}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, "test"); err != nil {
		t.Fatal(err)
	}
	versions := call(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/web/versions", "")
	if versions.Code != 200 || !bytes.Contains(versions.Body.Bytes(), []byte(`"id":"1"`)) || !bytes.Contains(versions.Body.Bytes(), []byte(`"id":"3"`)) {
		t.Fatalf("versions with reused bundle SHA = %d %s", versions.Code, versions.Body.String())
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/versions/1/deploy", "{}"); rr.Code != 200 {
		t.Fatalf("version deploy %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/web/versions/2", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"number":2`)) {
		t.Fatalf("inactive version view %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodDelete, "/client/v4/accounts/acme/workers/scripts/web", ""); rr.Code != 200 {
		t.Fatalf("worker delete %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareSecretBulkPatch(t *testing.T) {
	s := newControlServer(t)
	h := s.AdminHandler()
	call := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPatch, "/client/v4/accounts/acme/workers/scripts/web/secrets-bulk", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := call(`{"secrets":{"A":{"name":"A","type":"secret_text","text":"one"},"B":{"name":"B","type":"secret_text","text":"two"}}}`); rr.Code != 200 {
		t.Fatalf("bulk put %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(`{"secrets":{"A":null,"B":{"name":"wrong","type":"secret_text","text":"changed"}}}`); rr.Code == 200 {
		t.Fatalf("invalid bulk accepted: %s", rr.Body.String())
	}
	if value, err := s.Control.GetSecret(t.Context(), "acme", "web", "A"); err != nil || string(value) != "one" {
		t.Fatalf("invalid bulk changed A: %s, %v", value, err)
	}
	if rr := call(`{"secrets":{"A":null,"B":{"name":"B","type":"secret_text","text":"changed"}}}`); rr.Code != 200 {
		t.Fatalf("bulk patch %d %s", rr.Code, rr.Body.String())
	}
	if value, err := s.Control.GetSecret(t.Context(), "acme", "web", "B"); err != nil || string(value) != "changed" {
		t.Fatalf("bulk changed B: %s, %v", value, err)
	}
}

func TestCloudflareQueueConsumerLifecycle(t *testing.T) {
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "worker", control.DeploySpec{BundleSHA: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, "test"); err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/queues", `{"name":"jobs"}`); rr.Code != 200 {
		t.Fatalf("queue %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/queues/jobs/consumers", `{"type":"worker","script_name":"worker","settings":{"batch_size":10,"max_retries":2}}`); rr.Code != 200 {
		t.Fatalf("consumer create %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/queues/jobs/consumers", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"consumer_id":"worker"`)) {
		t.Fatalf("consumer list %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodDelete, "/client/v4/accounts/acme/queues/jobs/consumers/worker", ""); rr.Code != 200 {
		t.Fatalf("consumer delete %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareTailSessionLifecycle(t *testing.T) {
	s := newControlServer(t)
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	created := call(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/tails", `{"search":"hello"}`)
	if created.Code != 200 || !bytes.Contains(created.Body.Bytes(), []byte(`"url"`)) {
		t.Fatalf("tail create %d %s", created.Code, created.Body.String())
	}
	var envelope struct {
		Result struct {
			ID string `json:"id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &envelope); err != nil || envelope.Result.ID == "" {
		t.Fatalf("tail response %s", created.Body.String())
	}
	if rr := call(http.MethodDelete, "/client/v4/accounts/acme/workers/scripts/web/tails/"+envelope.Result.ID, ""); rr.Code != 200 {
		t.Fatalf("tail delete %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareTailWebSocket(t *testing.T) {
	s := newControlServer(t)
	s.Logs = logbuf.New(16, 4)
	s.Logs.Add(logbuf.Entry{Namespace: "acme", Worker: "web", Message: "hello"})
	h := httptest.NewServer(s.AdminHandler())
	defer h.Close()
	base, _ := url.Parse(h.URL)
	req, _ := http.NewRequest(http.MethodPost, h.URL+"/client/v4/accounts/acme/workers/scripts/web/tails", bytes.NewBufferString("{}"))
	req.Header.Set("Authorization", "Bearer admin-tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env struct {
		Result struct {
			URL string `json:"url"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(env.Result.URL)
	u.Scheme = "ws"
	u.Host = base.Host
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
}

func TestCloudflareAssetsAndWorkflows(t *testing.T) {
	s := newControlServer(t)
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	session := call(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/assets-upload-session", `{"manifest":{"/index.html":{"hash":"hash1","size":5}}}`)
	if session.Code != 200 {
		t.Fatalf("assets session %d %s", session.Code, session.Body.String())
	}
	var envelope struct {
		Result struct {
			JWT string `json:"jwt"`
		} `json:"result"`
	}
	if err := json.Unmarshal(session.Body.Bytes(), &envelope); err != nil || envelope.Result.JWT == "" {
		t.Fatalf("assets session response=%s", session.Body.String())
	}
	req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/workers/assets/upload/hash1", bytes.NewBufferString("hello"))
	req.Header.Set("Authorization", "Bearer "+envelope.Result.JWT)
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("asset upload %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPut, "/client/v4/accounts/acme/workflows/daily", `{"script_name":"web","class_name":"Daily","concurrency":{"limit":2},"schedules":[{"cron":"*/5 * * * *"}],"default_retention":{"success_retention":"3 days","error_retention":86400000}}`); rr.Code != 200 {
		t.Fatalf("workflow put %d %s", rr.Code, rr.Body.String())
	}
	if raw, err := s.Control.ResourceConfig(t.Context(), "acme", "workflow", "daily"); err != nil || !bytes.Contains(raw, []byte(`"success_retention_ms"`)) {
		t.Fatalf("workflow policy was not persisted: %v %s", err, raw)
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/workflows/daily", ""); rr.Code != 200 {
		t.Fatalf("workflow get %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/workflows/new-workflow", ""); rr.Code != 200 {
		t.Fatalf("workflow probe %d %s", rr.Code, rr.Body.String())
	}
}

func TestParseCloudflareWorkerMultipart(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	meta := `{"main_module":"worker.js","compatibility_date":"2026-09-23","bindings":[{"name":"DB","type":"d1","id":"db1"},{"name":"TOKEN","type":"plain_text","text":"ok"},{"name":"BUCKET","type":"r2_bucket","bucket_name":"files"}],"triggers":{"crons":["*/5 * * * *"]}}`
	part, err := w.CreateFormField("metadata")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte(meta))
	part, err = w.CreateFormFile("worker.js", "worker.js")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("export default { fetch() { return new Response('ok') } }"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/", &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	got, module, err := parseCloudflareWorkerMultipart(r)
	if err != nil {
		t.Fatal(err)
	}
	if got.MainModule != "worker.js" || string(module) == "" || got.CompatibilityDate != "2026-09-23" {
		t.Fatalf("metadata/module = %+v %q", got, module)
	}
	if got.Vars["TOKEN"] != "ok" || len(got.Bindings) != 2 || got.Crons[0] != "*/5 * * * *" {
		t.Fatalf("mapped metadata = %+v", got)
	}
}
