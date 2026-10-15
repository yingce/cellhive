package dispatch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/timer"
	"cellhive/internal/workflow"
)

func TestWorkflowDispatcherRunAndSleepTimer(t *testing.T) {
	var got map[string]any
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workflows/run" || r.Header.Get("x-cellhive-internal-token") != "tok" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		calls++
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	next := &recorder{}
	d := &WorkflowDispatcher{
		URL: srv.URL, Token: "tok", Next: next,
		Params: func(context.Context, string, string, string) ([]byte, error) { return []byte(`{"seed":20}`), nil },
		Claim: func(context.Context, string, string, string) (string, uint64, bool, error) {
			return "tok2", 2, true, nil
		},
		Target: func(context.Context, string, string) (control.WorkflowTarget, bool) {
			return control.WorkflowTarget{Namespace: "acme", Worker: "api", Name: "MY_WF", ClassName: "MyWF", BundleSHA: "sha1", Version: 1}, true
		},
		TargetForInstance: func(context.Context, string, string, string) (control.WorkflowTarget, bool, error) {
			return control.WorkflowTarget{Namespace: "acme", Worker: "api-v1", Name: "MY_WF", ClassName: "MyWFv1", BundleSHA: "sha-created", Version: 7}, true, nil
		},
	}
	if err := d.Run(context.Background(), "acme", "MY_WF", "i1", "tok1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got["worker"] != "api-v1" || got["class_name"] != "MyWFv1" || got["bundle_sha"] != "sha-created" || got["version"] != float64(7) || got["id"] != "i1" || got["workflow"] != "MY_WF" {
		t.Fatalf("payload = %+v", got)
	}
	if got["params"] != base64.StdEncoding.EncodeToString([]byte(`{"seed":20}`)) {
		t.Fatalf("workflow input lost on first dispatch: %+v", got)
	}

	// A KindWorkflowSleep timer resumes the instance.
	if err := d.Dispatch(context.Background(), timer.New(1, timer.KindWorkflowSleep, "acme/__workflow__/MY_WF", "i2")); err != nil {
		t.Fatalf("sleep dispatch: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if got["params"] != base64.StdEncoding.EncodeToString([]byte(`{"seed":20}`)) {
		t.Fatalf("workflow input lost on timer resume: %+v", got)
	}
	// Other kinds pass through.
	if err := d.Dispatch(context.Background(), timer.New(1, timer.KindCron, "acme/__cron__/api", "x")); err != nil {
		t.Fatalf("cron dispatch: %v", err)
	}
	if next.calls != 1 {
		t.Fatalf("next calls = %d, want 1", next.calls)
	}
}

type recorder struct{ calls int }

func (r *recorder) Dispatch(context.Context, timer.Timer) error { r.calls++; return nil }

func TestWorkflowBusyClaimKeepsWakePending(t *testing.T) {
	d := &WorkflowDispatcher{Claim: func(context.Context, string, string, string) (string, uint64, bool, error) { return "", 0, false, nil }, CreateScheduled: func(context.Context, string, string, string) (string, error) { return "i", nil }}
	for _, kind := range []timer.Kind{timer.KindWorkflowSleep, timer.KindWorkflowSchedule} {
		if err := d.Dispatch(context.Background(), timer.New(1, kind, "acme/__workflow__/wf", "i")); err == nil {
			t.Fatalf("%s consumed while claim unavailable", kind)
		}
	}
}

func TestWorkflowOutboxRetriesBusyRunAfterReopen(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	store := workflow.New(cs)
	for _, id := range []string{"a", "b"} {
		if _, err := store.Create(ctx, "acme", "wf", id, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, ok, err := store.ClaimRunLimited(ctx, "acme", "wf", "a", 60000, 1); err != nil || !ok {
		t.Fatalf("claim a: %v %v", ok, err)
	}
	// A new store has no original request goroutine or run bookkeeping.
	store = workflow.New(cs)
	c, err := cs.Cell(ctx, workflow.Scope("acme", "wf"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err := store.SetOutput(r.Context(), "acme", "wf", in.ID, []byte(`"done"`)); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	d := &WorkflowDispatcher{URL: server.URL, Target: func(context.Context, string, string) (control.WorkflowTarget, bool) {
		return control.WorkflowTarget{Worker: "web", BundleSHA: "sha", ClassName: "WF", Version: 1}, true
	}, Params: func(context.Context, string, string, string) ([]byte, error) { return nil, nil }, Skip: store.SkipWake, Claim: func(ctx context.Context, ns, name, id string) (string, uint64, bool, error) {
		token, gen, _, ok, err := store.ClaimRunLimited(ctx, ns, name, id, 60000, 1)
		return token, gen, ok, err
	}}
	if n, err := timer.DispatchDue(ctx, st, d, time.Now().UnixMilli(), 100, time.Hour, nil); err != nil || n != 0 {
		t.Fatalf("busy wakes consumed: %d %v", n, err)
	}
	if err := store.SetOutput(ctx, "acme", "wf", "a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := timer.DispatchDue(ctx, st, d, time.Now().UnixMilli(), 100, time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	in, err := store.Get(ctx, "acme", "wf", "b")
	if err != nil || in.Status != workflow.StatusComplete || string(in.Output) != `"done"` {
		t.Fatalf("queued run lost after reopen: %+v %v", in, err)
	}
}
