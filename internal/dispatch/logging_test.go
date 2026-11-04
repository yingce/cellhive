package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/timer"
)

type enrichedDispatcher struct{ next timer.Dispatcher }

func (d enrichedDispatcher) Dispatch(ctx context.Context, tm timer.Timer) error {
	tm.Worker = "web"
	return d.next.Dispatch(ctx, tm)
}

func TestTimerFailureLogsSafeCorrelationAndRetry(t *testing.T) {
	for _, body := range []string{
		`{"error":"handler_failed","message":"business-secret","invocation_id":"secret-id"}`,
		`<html>business-secret</html>`,
		`business-secret`,
		`{"error":"business-secret","message":"business-secret"}`,
	} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assertJWT(t, r, "dispatch")
				w.WriteHeader(500)
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			c, err := cs.Open(t.Context(), cell.Scope{Namespace: "acme", Class: "__cron__", ID: "web"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			st, err := timer.NewStore(t.Context(), c)
			if err != nil {
				t.Fatal(err)
			}
			tm := timer.New(1000, timer.KindCron, c.Scope.String(), "occurrence-secret")
			if err := st.Upsert(t.Context(), tm); err != nil {
				t.Fatal(err)
			}
			d := NewHTTP(srv.URL+"?secret=query-secret", jwtTestKey)
			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, nil))
			for i, now := range []int64{1000, 6000} {
				buf.Reset()
				n, err := timer.DispatchDue(t.Context(), st, enrichedDispatcher{d}, now, 10, time.Hour, log)
				if err != nil || n != 0 {
					t.Fatalf("dispatch %d %v", n, err)
				}
				var got map[string]any
				if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				want := map[string]any{"ns": "acme", "scope": tm.Scope, "worker": "web", "token": tm.Token, "kind": "cron", "scheduled_time_ms": float64(1000), "attempt": float64(i + 1), "http_status": float64(500), "target_host": strings.TrimPrefix(srv.URL, "http://"), "target_path": "/v1/timers/dispatch", "retry_delay_ms": float64(5000 * (i + 1))}
				for k, v := range want {
					if got[k] != v {
						t.Errorf("%s=%v want %v; log=%s", k, got[k], v, buf.String())
					}
				}
				if _, ok := got["duration_ms"]; !ok {
					t.Error("missing duration")
				}
				if strings.Contains(body, "handler_failed") && got["error_code"] != "handler_failed" {
					t.Errorf("missing controlled code: %s", buf.String())
				}
				if !strings.Contains(body, "handler_failed") && got["error_code"] != "http_error" {
					t.Errorf("uncontrolled error code: %s", buf.String())
				}
				for _, secret := range []string{"business-secret", "query-secret", "occurrence-secret", "secret-id", jwtTestKey, "Bearer", "Authorization"} {
					if strings.Contains(buf.String(), secret) {
						t.Errorf("leaked %q: %s", secret, buf.String())
					}
				}
				if due, err := st.Due(t.Context(), now+4999, 10); err != nil || len(due) != 0 {
					t.Fatalf("retry fired early: %v %v", due, err)
				}
			}
		})
	}
}

type responseTransport func(*http.Request) (*http.Response, error)

func (f responseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countingBody struct {
	read   int
	closed bool
}

func (b *countingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}
func (b *countingBody) Close() error { b.closed = true; return nil }

func TestDispatchFailureBoundedAndTransportSafe(t *testing.T) {
	b := &countingBody{}
	d := NewHTTP("http://user:password@runtime/base-secret?secret=query-secret", "")
	d.Client = &http.Client{Transport: responseTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: b, Header: make(http.Header)}, nil
	})}
	err := d.Dispatch(t.Context(), timer.Timer{})
	if err == nil || b.read == 0 || b.read > 4096 || !b.closed {
		t.Fatalf("error=%v read=%d closed=%v", err, b.read, b.closed)
	}
	for _, tc := range []struct {
		err   error
		code  string
		phase string
	}{{context.Canceled, "canceled", "transport"}, {context.DeadlineExceeded, "timeout", "transport"}, {errors.New("business-secret"), "transport_error", "transport"}, {&net.DNSError{Err: "business-secret", Name: "query-secret"}, "transport_error", "dns"}, {&net.OpError{Op: "dial", Err: errors.New("business-secret")}, "transport_error", "dial"}} {
		d.Client.Transport = responseTransport(func(r *http.Request) (*http.Response, error) { return nil, tc.err })
		err := d.Dispatch(t.Context(), timer.Timer{})
		var diagnostic *Error
		if !errors.As(err, &diagnostic) || diagnostic.Phase != tc.phase || !errors.Is(err, tc.err) {
			t.Fatalf("classification/unwrap: %v", err)
		}
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).Warn("failed", "err", err)
		if !strings.Contains(buf.String(), tc.code) {
			t.Errorf("missing %s: %s", tc.code, buf.String())
		}
		for _, secret := range []string{"password", "query-secret", "base-secret", "business-secret"} {
			if strings.Contains(buf.String(), secret) {
				t.Errorf("leaked %s: %s", secret, buf.String())
			}
		}
	}
}

func TestTimerSuccessIsSilentAndMarkedFired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"no_retry":true,"result":"business-secret"}`)
	}))
	defer srv.Close()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c, err := cs.Open(t.Context(), cell.Scope{Namespace: "acme", Class: "__cron__", ID: "web"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	st, err := timer.NewStore(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	tm := timer.New(1000, timer.KindCron, c.Scope.String(), "slot")
	if err := st.Upsert(t.Context(), tm); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for i, want := range []int{1, 0} {
		n, err := timer.DispatchDue(t.Context(), st, NewHTTP(srv.URL, ""), 1000, 1, time.Hour, slog.New(slog.NewJSONHandler(&buf, nil)))
		if n != want || err != nil {
			t.Fatalf("pass %d: %d %v", i, n, err)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("success produced log: %s", buf.String())
	}
	if fired, err := st.IsFired(t.Context(), tm.Token, 1000); err != nil || !fired {
		t.Fatalf("not fired: %v %v", fired, err)
	}
}

func TestOtherTimerHTTPFailuresRetainTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, `{"error":"handler_failed","message":"business-secret"}`)
	}))
	defer srv.Close()
	wf := &WorkflowDispatcher{URL: srv.URL, Token: jwtTestKey,
		Target: func(context.Context, string, string) (control.WorkflowTarget, bool) {
			return control.WorkflowTarget{Worker: "pinned-worker"}, true
		},
		Params: func(context.Context, string, string, string) ([]byte, error) { return []byte(`"business-secret"`), nil },
		Claim: func(context.Context, string, string, string) (string, uint64, bool, error) {
			return "run-secret", 1, true, nil
		},
	}
	do := &DoAlarmDispatcher{Token: jwtTestKey,
		Resolve: func(context.Context, cell.Scope) (cell.Owner, string, error) {
			return cell.Owner{Address: srv.URL}, "", nil
		},
		Bundle: func(context.Context, string, string) (string, int, bool) { return "sha", 1, true },
	}
	for _, tc := range []struct {
		d    timer.Dispatcher
		tm   timer.Timer
		path string
	}{
		{wf, timer.New(1, timer.KindWorkflowSleep, "acme/__workflow__/wf", "id"), "/v1/workflows/run"},
		{do, timer.New(1, timer.KindDOAlarm, "acme/__timer__/do", `{"w":"pinned-worker","c":"C","id":"business-secret"}`), "/v1/do/invoke"},
	} {
		err := tc.d.Dispatch(t.Context(), tc.tm)
		var diagnostic *Error
		if !errors.As(err, &diagnostic) {
			t.Errorf("missing typed diagnostic: %v", err)
			continue
		}
		if diagnostic.StatusCode != 500 || diagnostic.Worker != "pinned-worker" || diagnostic.Path != tc.path || diagnostic.Code != "handler_failed" {
			t.Errorf("wrong diagnostic: %+v", diagnostic)
		}
	}
}

func handlerFailureDiagnostic(t *testing.T, name, frame string) *Error {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"error": "handler_failed", "message": "business-secret?token=query-secret",
		"invocation_id": "secret-id", "error_name": name, "error_frame": frame,
	})
	if err != nil {
		t.Fatal(err)
	}
	d := NewHTTP("http://runtime", "")
	d.Client = &http.Client{Transport: responseTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	err = d.Dispatch(t.Context(), timer.Timer{})
	var diagnostic *Error
	if !errors.As(err, &diagnostic) {
		t.Fatalf("missing typed diagnostic: %v", err)
	}
	return diagnostic
}

func logDiagnostic(t *testing.T, diagnostic *Error) string {
	t.Helper()
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(t.Context(), slog.LevelWarn, "failed", diagnostic.DiagnosticAttrs()...)
	return buf.String()
}

func TestDispatchHandlerDiagnosticsAllowlisted(t *testing.T) {
	diagnostic := handlerFailureDiagnostic(t, "TypeError", "index.js:12:7")
	if diagnostic.ErrorName != "TypeError" || diagnostic.ErrorFrame != "index.js:12:7" {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
	log := logDiagnostic(t, diagnostic)
	for _, want := range []string{`"error_name":"TypeError"`, `"error_frame":"index.js:12:7"`} {
		if !strings.Contains(log, want) {
			t.Errorf("missing %s: %s", want, log)
		}
	}
	for _, secret := range []string{"business-secret", "query-secret", "secret-id", "token"} {
		if strings.Contains(log, secret) {
			t.Errorf("leaked %q: %s", secret, log)
		}
	}
}

func TestDispatchHandlerDiagnosticsSanitized(t *testing.T) {
	for _, tc := range []struct {
		name, frame         string
		wantName, wantFrame string
	}{
		{"bad name!", "index.js:12:7", "", "index.js:12:7"},
		{strings.Repeat("A", 65), "index.js:12:7", "", "index.js:12:7"},
		{"TypeError", "/abs/private/index.js?token=query-secret:12:7", "TypeError", ""},
		{"TypeError", "index.js:1000000:7", "TypeError", ""},
		{"TypeError", "index.js:0:7", "TypeError", ""},
		{"TypeError", strings.Repeat("a", 81) + ":1:1", "TypeError", ""},
	} {
		t.Run(tc.name+"/"+tc.frame, func(t *testing.T) {
			diagnostic := handlerFailureDiagnostic(t, tc.name, tc.frame)
			if diagnostic.ErrorName != tc.wantName || diagnostic.ErrorFrame != tc.wantFrame {
				t.Fatalf("diagnostic name=%q frame=%q, want %q/%q", diagnostic.ErrorName, diagnostic.ErrorFrame, tc.wantName, tc.wantFrame)
			}
			log := logDiagnostic(t, diagnostic)
			if tc.wantName == "" && strings.Contains(log, "error_name") {
				t.Errorf("unexpected error_name: %s", log)
			}
			if tc.wantFrame == "" && strings.Contains(log, "error_frame") {
				t.Errorf("unexpected error_frame: %s", log)
			}
			for _, forbidden := range []string{"query-secret", "business-secret", "secret-id"} {
				if strings.Contains(log, forbidden) {
					t.Errorf("leaked %q: %s", forbidden, log)
				}
			}
		})
	}
}
