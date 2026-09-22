package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/admission"
	"cellhive/internal/scopedtoken"
)

// TestWorkflowEndpoints covers the cell-agent workflow surface: tenant create/get
// (scoped) and the internal step/sleep/finish callbacks the running workflow uses.
func TestWorkflowEndpoints(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "MY_WF"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	scopeDo := func(method, target, token string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		if token != "" {
			req.Header.Set("x-cellhive-scope-token", token)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	// Create with params.
	rr := scopeDo(http.MethodPost, "/v1/workflow/create?ns=acme&workflow=MY_WF&id=i1", tok, []byte(`{"n":1}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("create = %d %s", rr.Code, rr.Body.String())
	}
	// Get -> queued.
	rr = scopeDo(http.MethodGet, "/v1/workflow/get?ns=acme&workflow=MY_WF&id=i1", tok, nil)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"status":"queued"`)) {
		t.Fatalf("get = %d %s", rr.Code, rr.Body.String())
	}

	// Lifecycle: pause -> paused; restart clears steps and re-queues.
	if rr := scopeDo(http.MethodPost, "/v1/workflow/pause?ns=acme&workflow=MY_WF&id=i1", tok, nil); rr.Code != http.StatusOK {
		t.Fatalf("pause = %d %s", rr.Code, rr.Body.String())
	}
	rr = scopeDo(http.MethodGet, "/v1/workflow/get?ns=acme&workflow=MY_WF&id=i1", tok, nil)
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"status":"paused"`)) {
		t.Fatalf("after pause = %s", rr.Body.String())
	}
	// List returns the instance.
	rr = scopeDo(http.MethodGet, "/v1/workflow/list?ns=acme&workflow=MY_WF", tok, nil)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"instances"`)) {
		t.Fatalf("list = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, h, http.MethodPut, "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=i1&name=z", "tok", []byte(base64.StdEncoding.EncodeToString([]byte(`1`)))); rr.Code != http.StatusOK {
		t.Fatalf("pre-restart step = %d", rr.Code)
	}
	if rr := scopeDo(http.MethodPost, "/v1/workflow/restart?ns=acme&workflow=MY_WF&id=i1", tok, nil); rr.Code != http.StatusOK {
		t.Fatalf("restart = %d %s", rr.Code, rr.Body.String())
	}
	rr = do(t, h, http.MethodGet, "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=i1&name=z", "tok", nil)
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"found":false`)) {
		t.Fatalf("step survived restart: %s", rr.Body.String())
	}

	// Internal step put/get.
	stepURL := "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=i1&name=a"
	if rr := do(t, h, http.MethodPut, stepURL, "tok", []byte(base64.StdEncoding.EncodeToString([]byte(`"done"`)))); rr.Code != http.StatusOK {
		t.Fatalf("step put = %d %s", rr.Code, rr.Body.String())
	}
	rr = do(t, h, http.MethodGet, stepURL, "tok", nil)
	var stepResp struct {
		Found  bool   `json:"found"`
		Result string `json:"result"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &stepResp)
	got, _ := base64.StdEncoding.DecodeString(stepResp.Result)
	if !stepResp.Found || string(got) != `"done"` {
		t.Fatalf("step get = %s", rr.Body.String())
	}
	// A resumed worker decodes the GET result once; persisted bytes must be
	// the JSON payload, not the base64 transport envelope.
	stored, found, err := srv.Workflows.GetStep(context.Background(), "acme", "MY_WF", "i1", "a")
	if err != nil || !found || string(stored) != `"done"` {
		t.Fatalf("persisted step = %q, found=%v, err=%v", stored, found, err)
	}

	// Sleep schedules a timer.
	if rr := do(t, h, http.MethodPost, "/v1/internal/workflow/sleep?ns=acme&workflow=MY_WF&id=i1&wake_at_ms=1700000000000", "tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("sleep = %d %s", rr.Code, rr.Body.String())
	}

	// Finish stores output and completes.
	out := base64.StdEncoding.EncodeToString([]byte(`{"ok":true}`))
	if rr := do(t, h, http.MethodPost, "/v1/internal/workflow/finish?ns=acme&workflow=MY_WF&id=i1", "tok", []byte(out)); rr.Code != http.StatusOK {
		t.Fatalf("finish = %d %s", rr.Code, rr.Body.String())
	}
	rr = scopeDo(http.MethodGet, "/v1/workflow/get?ns=acme&workflow=MY_WF&id=i1", tok, nil)
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"status":"complete"`)) {
		t.Fatalf("final get = %s", rr.Body.String())
	}
	var final struct {
		Output string `json:"output"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &final); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(final.Output)
	if err != nil || string(decoded) != `{"ok":true}` {
		t.Fatalf("workflow output = %q, decode err=%v", decoded, err)
	}
}

// A base64 transport envelope must neither lower the 8 MiB result budget nor
// silently commit the valid prefix of an oversized result.
func TestWorkflowResultDecodedBudget(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	ctx := context.Background()
	url := "/v1/internal/workflow/finish?ns=acme&workflow=MY_WF&id=large"
	stepURL := "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=large&name=large"
	if _, err := srv.Workflows.Create(ctx, "acme", "MY_WF", "large", nil); err != nil {
		t.Fatal(err)
	}
	accepted := append(append([]byte{'"'}, bytes.Repeat([]byte{'x'}, 7<<20)...), '"')
	encoded := []byte(base64.StdEncoding.EncodeToString(accepted))
	if rr := do(t, h, http.MethodPut, stepURL, "tok", encoded); rr.Code != http.StatusOK {
		t.Fatalf("7 MiB step = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, h, http.MethodPost, url, "tok", encoded); rr.Code != http.StatusOK {
		t.Fatalf("7 MiB output = %d %s", rr.Code, rr.Body.String())
	}
	step, found, err := srv.Workflows.GetStep(ctx, "acme", "MY_WF", "large", "large")
	if err != nil || !found || !bytes.Equal(step, accepted) {
		t.Fatalf("large step lost bytes: found=%v size=%d err=%v", found, len(step), err)
	}
	in, err := srv.Workflows.Get(ctx, "acme", "MY_WF", "large")
	if err != nil || !bytes.Equal(in.Output, accepted) {
		t.Fatalf("large output lost bytes: size=%d err=%v", len(in.Output), err)
	}
	if _, err := srv.Workflows.Create(ctx, "acme", "MY_WF", "overflow", nil); err != nil {
		t.Fatal(err)
	}
	tooLarge := []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'y'}, (8<<20)+1)))
	for _, tc := range []struct{ method, target string }{
		{http.MethodPut, "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=overflow&name=large"},
		{http.MethodPost, "/v1/internal/workflow/finish?ns=acme&workflow=MY_WF&id=overflow"},
	} {
		rr := do(t, h, tc.method, tc.target, "tok", tooLarge)
		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized %s = %d %s", tc.target, rr.Code, rr.Body.String())
		}
	}
	in, err = srv.Workflows.Get(ctx, "acme", "MY_WF", "overflow")
	if err != nil || in.Status == "complete" || len(in.Output) != 0 {
		t.Fatalf("oversized output committed: status=%s size=%d err=%v", in.Status, len(in.Output), err)
	}
	if step, found, err := srv.Workflows.GetStep(ctx, "acme", "MY_WF", "overflow", "large"); err != nil || found {
		t.Fatalf("oversized step committed: found=%v size=%d err=%v", found, len(step), err)
	}
}

func TestWorkflowCreateBatchCreatesEveryInstance(t *testing.T) {
	srv, _ := newTestServer(t)
	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "MY_WF"})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`[{"id":"b1","params":{"n":1}},{"id":"b2","params":{"n":2}}]`)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow/create-batch?ns=acme&workflow=MY_WF", bytes.NewReader(body))
	req.Header.Set("x-cellhive-scope-token", tok)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("create batch = %d: %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Instances []struct {
			ID string `json:"id"`
		} `json:"instances"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Instances) != 2 || got.Instances[0].ID != "b1" || got.Instances[1].ID != "b2" {
		t.Fatalf("instances = %#v", got.Instances)
	}
	for _, id := range []string{"b1", "b2"} {
		if _, err := srv.Workflows.Get(context.Background(), "acme", "MY_WF", id); err != nil {
			t.Fatalf("instance %s missing: %v", id, err)
		}
	}
}

// TestAdmissionRateLimit covers ADR-035 admission: a namespace over its write
// rate gets 429 on a tenant write endpoint.
func TestAdmissionRateLimit(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Admission = admission.New(0.0001, 1) // burst 1, effectively no refill
	h := srv.Handler()
	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "MY_WF"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	post := func(id string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/workflow/create?ns=acme&workflow=MY_WF&id="+id, bytes.NewReader(nil))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := post("a"); code != http.StatusOK {
		t.Fatalf("first write = %d", code)
	}
	if code := post("b"); code != http.StatusTooManyRequests {
		t.Fatalf("second write = %d, want 429", code)
	}
}

func TestAdminUIServesConsole(t *testing.T) {
	srv, _ := newTestServer(t)
	rr := httptest.NewRecorder()
	srv.handleAdminUI(rr, httptest.NewRequest(http.MethodGet, "/admin", nil))
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte("CellHive admin")) {
		t.Fatalf("admin UI = %d", rr.Code)
	}
}

// TestWorkflowRunLeasePreventsDoubleDispatch covers run concurrency safety: two
// dispatch attempts on the same instance produce one run while the lease is held.
func TestWorkflowRunLeasePreventsDoubleDispatch(t *testing.T) {
	srv, _ := newTestServer(t)
	ctx := context.Background()
	in, err := srv.Workflows.Create(ctx, "acme", "MY_WF", "", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	count := 0
	srv.DispatchWorkflow = func(context.Context, string, string, string, string) error {
		count++
		return nil
	}
	srv.startWorkflowRun(ctx, "acme", "MY_WF", in.ID)
	srv.startWorkflowRun(ctx, "acme", "MY_WF", in.ID) // lease held -> skipped
	if count != 1 {
		t.Fatalf("dispatches = %d, want 1", count)
	}
	// Fenced: a stale token cannot renew.
	if err := srv.Workflows.RenewRun(ctx, "acme", "MY_WF", in.ID, "bogus", 1000); err == nil {
		t.Fatal("stale token renewed")
	}
}
