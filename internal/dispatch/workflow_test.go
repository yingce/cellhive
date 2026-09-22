package dispatch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/control"
	"cellhive/internal/timer"
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
			return control.WorkflowTarget{Namespace: "acme", Worker: "api", Name: "MY_WF", ClassName: "MyWF", BundleSHA: "sha1"}, true
		},
	}
	if err := d.Run(context.Background(), "acme", "MY_WF", "i1", "tok1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got["worker"] != "api" || got["class_name"] != "MyWF" || got["id"] != "i1" || got["workflow"] != "MY_WF" {
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
