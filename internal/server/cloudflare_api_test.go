package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/lease"
	"cellhive/internal/logbuf"
	"cellhive/internal/queue"
	"cellhive/internal/scopedtoken"
	"github.com/gorilla/websocket"
)

func TestCloudflareWorkerVersionsUploadDeploys(t *testing.T) {
	s := newControlServer(t)
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, _ := w.CreateFormField("metadata")
	_, _ = part.Write([]byte(`{"main_module":"worker.js","compatibility_date":"2026-09-23","limits":{"cpu_ms":50}}`))
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
	view, ok, err := s.Control.VersionEnv(t.Context(), "acme", "web", releases[0].Version)
	if err != nil || !ok || view.Limits == nil || view.Limits.CpuMs == nil || *view.Limits.CpuMs != 50 {
		t.Fatalf("uploaded cpu_ms not persisted: limits=%+v ok=%v err=%v", view.Limits, ok, err)
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
	get := httptest.NewRequest(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/web/versions/1", nil)
	get.Header.Set("Authorization", "Bearer admin-tok")
	getResult := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(getResult, get)
	if getResult.Code != 200 || !bytes.Contains(getResult.Body.Bytes(), []byte(`"cpu_ms":50`)) {
		t.Fatalf("version readback = %d %s", getResult.Code, getResult.Body.String())
	}
}

func TestCloudflareRepeatUploadPreservesSeparateTriggers(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "JOBS", "acme/__queue__/JOBS", "test"); err != nil {
		t.Fatal(err)
	}
	upload := func(meta string) {
		t.Helper()
		var body bytes.Buffer
		w := multipart.NewWriter(&body)
		part, _ := w.CreateFormField("metadata")
		_, _ = part.Write([]byte(meta))
		part, _ = w.CreateFormFile("worker.js", "worker.js")
		_, _ = part.Write([]byte("export default { fetch() { return new Response('ok') } }"))
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/versions", &body)
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", w.FormDataContentType())
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("upload status=%d body=%s", rr.Code, rr.Body.String())
		}
	}
	read := func() control.Version {
		t.Helper()
		v, err := s.activeCloudflareVersionForWorker(ctx, "acme", "web")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	upload(`{"main_module":"worker.js","triggers":{"crons":["* * * * *"]},"bindings":[{"name":"JOBS","type":"queue","queue_name":"JOBS"}]}`)
	consumer := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/queues/JOBS/consumers", strings.NewReader(`{"type":"worker","script_name":"web","settings":{"batch_size":1}}`))
	consumer.Header.Set("Authorization", "Bearer admin-tok")
	consumer.Header.Set("Content-Type", "application/json")
	cr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(cr, consumer)
	if cr.Code != 200 {
		t.Fatalf("consumer status=%d body=%s", cr.Code, cr.Body.String())
	}
	upload(`{"main_module":"worker.js","bindings":[{"name":"JOBS","type":"queue","queue_name":"JOBS"}]}`)
	v := read()
	if len(v.Crons) != 1 || v.Crons[0] != "* * * * *" || len(v.Consumers) != 1 || v.Consumers[0].Queue != "JOBS" {
		t.Fatalf("repeat upload lost triggers: crons=%v consumers=%v", v.Crons, v.Consumers)
	}
	upload(`{"main_module":"worker.js","triggers":{"crons":[]},"bindings":[{"name":"JOBS","type":"queue","queue_name":"JOBS"}]}`)
	v = read()
	if len(v.Crons) != 0 || len(v.Consumers) != 1 {
		t.Fatalf("explicit cron clear failed: crons=%v consumers=%v", v.Crons, v.Consumers)
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

func TestCloudflareDeploymentsReflectCommittedActivations(t *testing.T) {
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	path := "/client/v4/accounts/acme/workers/scripts/web/deployments"
	if rr := call(http.MethodGet, path, ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"deployments":[]`)) {
		t.Fatalf("initial deployments = %d %s", rr.Code, rr.Body.String())
	}
	for _, sha := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: sha}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if rr := call(http.MethodPost, path, `{"strategy":"percentage","versions":[{"version_id":"1","percentage":50},{"version_id":"2","percentage":50}]}`); rr.Code != 400 {
		t.Fatalf("traffic split should fail: %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, path, `{"strategy":"percentage","versions":[{"version_id":"1","percentage":100}]}`); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"version_id":"1"`)) {
		t.Fatalf("promote = %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, path, ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"version_id":"1"`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"version_id":"2"`)) {
		t.Fatalf("deployment history = %d %s", rr.Code, rr.Body.String())
	}
	releases, err := s.Control.Releases(t.Context(), "acme", "web")
	if err != nil || len(releases) != 2 || !releases[1].Active {
		t.Fatalf("persisted active version = %+v, %v", releases, err)
	}
}

func TestCloudflareWorkflowInstancesReadPersistedStepsAndPages(t *testing.T) {
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "workflow", "flow", "acme/__workflow__/flow", "test"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := s.Workflows.Create(t.Context(), "acme", "flow", id, []byte(`{"value":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Workflows.PutStep(t.Context(), "acme", "flow", "one", "saved", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	base := "/client/v4/accounts/acme/workflows/flow/instances"
	if rr := call(http.MethodGet, base+"/one", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"name":"saved"`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"ok":true`)) {
		t.Fatalf("describe=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"?page=2&per_page=2", ""); rr.Code != 200 || bytes.Count(rr.Body.Bytes(), []byte(`"created_on"`)) != 1 {
		t.Fatalf("page=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"?page=2&per_page=2&status=complete", ""); rr.Code != 200 || bytes.Contains(rr.Body.Bytes(), []byte(`"created_on"`)) {
		t.Fatalf("filtered page=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"?page=0", ""); rr.Code != 400 {
		t.Fatalf("invalid page=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPatch, base+"/one/status", `{"status":"pause","ignored":true}`); rr.Code != 400 {
		t.Fatalf("unknown status option=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, base+"/batch/delete", `{"instances":["one"]}`); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"deleted":[{"id":"one"}]`)) {
		t.Fatalf("delete=%d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareVectorizeManagement(t *testing.T) {
	s := newVectorizeServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	base := "/client/v4/accounts/acme/vectorize/v2/indexes"
	if rr := call(http.MethodPost, base, `{"name":"docs","config":{"dimensions":3,"metric":"cosine"}}`); rr.Code != 200 {
		t.Fatalf("create=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base, ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"name":"docs"`)) {
		t.Fatalf("list=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"/docs", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"dimensions":3`)) {
		t.Fatalf("get=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"/docs/info", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"vectorCount":0`)) {
		t.Fatalf("info=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, base, `{"name":"bad","config":{"dimensions":3,"metric":"dot-product"}}`); rr.Code != 400 {
		t.Fatalf("unsupported metric=%d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodDelete, base+"/docs", ""); rr.Code != 200 {
		t.Fatalf("delete=%d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareQueuePurgeClearsPersistedMessages(t *testing.T) {
	s := newControlServer(t)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.Queue = queue.New(cs)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "queue", "jobs", "acme/__queue__/jobs", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(t.Context(), "acme", "jobs", []byte(`{"n":1}`), "json", 0, ""); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/queues/jobs/purge", nil)
	req.Header.Set("Authorization", "Bearer admin-tok")
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("purge=%d %s", rr.Code, rr.Body.String())
	}
	if n, err := s.Queue.Depth(t.Context(), "acme", "jobs"); err != nil || n != 0 {
		t.Fatalf("depth=%d err=%v", n, err)
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
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/queues/jobs/consumers", `{"type":"worker","script_name":"worker","settings":{"batch_size":10,"max_retries":2,"retry_delay":7,"max_wait_time_ms":5000}}`); rr.Code != 200 {
		t.Fatalf("consumer create %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/queues/jobs/consumers", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"consumer_id":"worker"`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"retry_delay":7`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"max_wait_time_ms":5000`)) {
		t.Fatalf("consumer list %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/queues/jobs/consumers", `{"type":"worker","script_name":"worker","settings":{"max_retries":0}}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("explicit zero max_retries = %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodPost, "/client/v4/accounts/acme/queues/jobs/consumers", `{"type":"worker","script_name":"worker","settings":{}}`); rr.Code != 200 {
		t.Fatalf("default consumer create %d %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, "/client/v4/accounts/acme/queues/jobs/consumers", ""); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"max_retries":3`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"max_wait_time_ms":5000`)) {
		t.Fatalf("default consumer settings %d %s", rr.Code, rr.Body.String())
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
	s.Cfg.TokenLog = "tok"
	s.Logs = logbuf.New(16, 4)
	const event = `{"events":[{"scriptName":"untrusted","scriptVersion":{"id":"7","name":"web","tags":[]},"outcome":"exception","exceptions":[{"name":"Error","message":"boom","timestamp":42}],"logs":[{"message":["hello",{"a":1}],"level":"warn","timestamp":41}],"eventTimestamp":40,"event_type":"fetch","event":{"request":{"method":"GET","url":"https://example.com/"}},"requestId":"req-1"}]}`
	ingest := httptest.NewRequest(http.MethodPost, "/v1/internal/logs?ns=acme&worker=web", strings.NewReader(event))
	ingest.Header.Set("x-cellhive-internal-token", "tok")
	ingested := httptest.NewRecorder()
	s.Handler().ServeHTTP(ingested, ingest)
	if ingested.Code != http.StatusOK {
		t.Fatalf("ingest = %d %s", ingested.Code, ingested.Body.String())
	}
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
	if strings.Contains(env.Result.URL, "admin-tok") || u.Query().Get("token") == "" {
		t.Fatalf("tail URL contains admin credential or lacks stream token: %q", env.Result.URL)
	}
	u.Scheme = "ws"
	u.Host = base.Host
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"trace-v1"}
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.Subprotocol() != "trace-v1" {
		t.Fatalf("tail subprotocol = %q", conn.Subprotocol())
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var item struct {
		ScriptName string `json:"scriptName"`
		Outcome    string `json:"outcome"`
		EventType  string `json:"event_type"`
		Logs       []struct {
			Message []any  `json:"message"`
			Level   string `json:"level"`
		} `json:"logs"`
		Exceptions []struct {
			Message string `json:"message"`
		} `json:"exceptions"`
	}
	if err := conn.ReadJSON(&item); err != nil {
		t.Fatal(err)
	}
	if item.ScriptName != "web" || item.Outcome != "exception" || item.EventType != "fetch" || len(item.Logs) != 1 || item.Logs[0].Level != "warn" || len(item.Logs[0].Message) != 2 || len(item.Exceptions) != 1 || item.Exceptions[0].Message != "boom" {
		t.Fatalf("tail item = %+v", item)
	}
	pong := make(chan struct{}, 1)
	conn.SetPongHandler(func(string) error { pong <- struct{}{}; return nil })
	if err := conn.WriteControl(websocket.PingMessage, []byte("keepalive"), time.Now().Add(time.Second)); err != nil {
		t.Fatalf("tail ping: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	go func() { _, _, _ = conn.ReadMessage() }()
	select {
	case <-pong:
	case <-time.After(2 * time.Second):
		t.Fatal("tail did not answer WebSocket keepalive")
	}
	if lines := s.Logs.Since("acme", "web", 0, 10); len(lines) != 2 || lines[0].Message != `hello {"a":1}` || lines[0].EventType != "fetch" || lines[0].Outcome != "exception" || string(lines[0].ScriptVersion) != `{"id":"7","name":"web","tags":[]}` {
		t.Fatalf("native line view = %+v", lines)
	}
	// Official Wrangler 4.133.0 sends its first debug data frame with
	// mask:false. Its connection must remain alive and receive its fixed Pong.
	wranglerHeaders := http.Header{"User-Agent": {"wrangler/4.133.0"}}
	wranglerConn, _, err := dialer.Dial(u.String(), wranglerHeaders)
	if err != nil {
		t.Fatal(err)
	}
	defer wranglerConn.Close()
	debug := []byte(`{"debug":false}`)
	if _, err := wranglerConn.UnderlyingConn().Write(append([]byte{0x81, byte(len(debug))}, debug...)); err != nil {
		t.Fatalf("wrangler debug frame: %v", err)
	}
	wranglerPong := make(chan struct{}, 1)
	wranglerConn.SetPongHandler(func(data string) error {
		if data == "wrangler tail ping" {
			wranglerPong <- struct{}{}
		}
		return nil
	})
	_ = wranglerConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	go func() {
		for {
			if _, _, err := wranglerConn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	select {
	case <-wranglerPong:
	case <-time.After(5 * time.Second):
		t.Fatal("Wrangler-compatible tail did not receive keepalive Pong")
	}
}

func TestCloudflareTailReadsPeerNodeEvents(t *testing.T) {
	a, _ := newTestServer(t)
	a.Logs = logbuf.New(16, 4)
	b := New(Deps{
		Cfg:  config.Config{NodeID: "node-2", TokenPeer: "tok", ScopeSecret: "tok"},
		Logs: logbuf.New(16, 4),
	})
	peerHTTP := httptest.NewServer(b.Handler())
	defer peerHTTP.Close()
	otherLease := lease.NewManager(a.Bucket, "node-2", "session-2", peerHTTP.URL, peerHTTP.URL, 30*time.Second)
	otherLease.Instance = "tail-persistent-instance"
	if err := otherLease.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	if err := a.Lease.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	b.Logs.AddEvent(logbuf.TailEvent{Namespace: "acme", Worker: "web", ScriptName: "web", Outcome: "exception", EventType: "queue", EventTimestamp: 41, Exceptions: []logbuf.TailException{{Name: "Error", Message: "remote-boom", Timestamp: 41}}, Logs: []logbuf.TailLog{}})
	b.Logs.AddEvent(logbuf.TailEvent{Namespace: "other", Worker: "web", ScriptName: "web", Outcome: "ok", EventType: "fetch", EventTimestamp: 42})
	request := httptest.NewRequest(http.MethodGet, "/v1/internal/logs/events?ns=acme&worker=web&since=0", nil)
	unauthorized := httptest.NewRecorder()
	b.Handler().ServeHTTP(unauthorized, request)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("peer log endpoint without peer token = %d", unauthorized.Code)
	}
	adminHTTP := httptest.NewServer(a.AdminHandler())
	defer adminHTTP.Close()
	create, _ := http.NewRequest(http.MethodPost, adminHTTP.URL+"/client/v4/accounts/acme/workers/scripts/web/tails", strings.NewReader("{}"))
	create.Header.Set("Authorization", "Bearer admin-tok")
	resp, err := http.DefaultClient.Do(create)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create tail = %d", resp.StatusCode)
	}
	var created struct {
		Result struct {
			URL string `json:"url"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	streamURL, err := url.Parse(created.Result.URL)
	if err != nil {
		t.Fatal(err)
	}
	adminURL, _ := url.Parse(adminHTTP.URL)
	streamURL.Host = adminURL.Host
	streamURL.Scheme = "ws"
	conn, _, err := websocket.DefaultDialer.Dial(streamURL.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	var event struct {
		ScriptName string `json:"scriptName"`
		Outcome    string `json:"outcome"`
		EventType  string `json:"event_type"`
		Exceptions []struct {
			Message string `json:"message"`
		} `json:"exceptions"`
	}
	if err := conn.ReadJSON(&event); err != nil {
		t.Fatal(err)
	}
	if event.ScriptName != "web" || event.Outcome != "exception" || event.EventType != "queue" || len(event.Exceptions) != 1 || event.Exceptions[0].Message != "remote-boom" {
		t.Fatalf("peer Tail event = %+v", event)
	}
	// Restart node-2 with a fresh lease session and a new ring whose first seq
	// is 1. The stream must reset its peer cursor when discovery refreshes.
	restarted := New(Deps{
		Cfg:  config.Config{NodeID: "node-2", TokenPeer: "tok", ScopeSecret: "tok"},
		Logs: logbuf.New(16, 4),
	})
	restarted.Logs.AddEvent(logbuf.TailEvent{Namespace: "acme", Worker: "web", ScriptName: "web", Outcome: "ok", EventType: "scheduled", EventTimestamp: 43, Logs: []logbuf.TailLog{}})
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	newLease := lease.NewManager(a.Bucket, "node-2", "session-3", restartedHTTP.URL, restartedHTTP.URL, 30*time.Second)
	newLease.Instance = otherLease.Instance // same locked disk, new process session
	if err := newLease.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if err := conn.ReadJSON(&event); err != nil {
		t.Fatalf("peer Tail after node restart: %v", err)
	}
	if event.Outcome != "ok" || event.EventType != "scheduled" {
		t.Fatalf("peer Tail after node restart = %+v", event)
	}
}

func TestCloudflareTailTokenIsScopedExpiresAndWorksAcrossNodes(t *testing.T) {
	issuer := newControlServer(t)
	listener := newControlServer(t)
	listener.Logs = logbuf.New(16, 4)
	second := httptest.NewServer(listener.AdminHandler())
	defer second.Close()
	create := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/workers/scripts/web/tails", strings.NewReader(`{"search":"needle"}`))
	create.Header.Set("Authorization", "Bearer admin-tok")
	created := httptest.NewRecorder()
	issuer.AdminHandler().ServeHTTP(created, create)
	if created.Code != 200 {
		t.Fatalf("create = %d %s", created.Code, created.Body.String())
	}
	var envelope struct {
		Result struct {
			URL string `json:"url"`
		} `json:"result"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	stream, err := url.Parse(envelope.Result.URL)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := url.Parse(second.URL)
	stream.Host = base.Host
	conn, _, err := websocket.DefaultDialer.Dial(stream.String(), nil)
	if err != nil {
		t.Fatalf("second node rejected stream: %v", err)
	}
	_ = conn.Close()
	for _, path := range []string{
		strings.Replace(stream.Path, "/scripts/web/", "/scripts/other/", 1),
		strings.Replace(stream.Path, "/accounts/acme/", "/accounts/foreign/", 1),
	} {
		bad := *stream
		bad.Scheme = "http"
		bad.Path = path
		resp, err := http.Get(bad.String())
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong scope %s = %d", path, resp.StatusCode)
		}
	}
	bad := *stream
	bad.Scheme = "http"
	query := bad.Query()
	query.Set("token", "admin-tok")
	bad.RawQuery = query.Encode()
	resp, err := http.Get(bad.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin token accepted as stream token: %d", resp.StatusCode)
	}
	key, err := scopedtoken.IssuerKey([]byte(listener.Cfg.ScopeSecret), tailTokenIssuer)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := scopedtoken.Mint(key, scopedtoken.Claims{Namespace: "acme", Kind: tailTokenIssuer, Name: "web#id#", Iss: tailTokenIssuer, ExpiresMs: time.Now().Add(-time.Second).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	bad = *stream
	bad.Scheme = "http"
	bad.Path = "/client/v4/accounts/acme/workers/scripts/web/tails/id/stream"
	query = bad.Query()
	query.Set("token", expired)
	bad.RawQuery = query.Encode()
	resp, err = http.Get(bad.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired token = %d", resp.StatusCode)
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
