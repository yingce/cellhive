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
	// Put a step, then restart -> step cleared, queued.
	if rr := do(t, h, http.MethodPut, "/v1/internal/workflow/step?ns=acme&workflow=MY_WF&id=i1&name=z", "tok", []byte(`1`)); rr.Code != http.StatusOK {
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
	if rr := do(t, h, http.MethodPut, stepURL, "tok", []byte(`"done"`)); rr.Code != http.StatusOK {
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
