package doruntime

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoRuntimeReadinessDiagnosticsAndRecovery(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Fatal(err)
	}
	var mode atomic.Int32
	cell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			http.Error(w, "private backend diagnostic", 503)
		case 1:
			<-r.Context().Done()
		case 2:
			w.Write([]byte(`{"status":"ready"}`))
		}
	}))
	defer cell.Close()
	base, stop := startRuntimeAt(t, cell.URL, "", t.TempDir(), "", "diagnostic-recovery")
	defer stop()
	check := func(want string, status int) {
		t.Helper()
		resp, e := noProxyClient().Get(base + "/ready")
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		var body map[string]any
		if e = json.NewDecoder(resp.Body).Decode(&body); e != nil {
			t.Fatal(e)
		}
		if resp.StatusCode != status {
			t.Fatalf("status %d body %v", resp.StatusCode, body)
		}
		if want != "" && body["failure_kind"] != want {
			t.Fatalf("want failure_kind %q got %v", want, body)
		}
		if want == "http" && body["upstream_status"] != float64(503) {
			t.Fatalf("missing upstream status: %v", body)
		}
	}
	check("http", 503)
	mode.Store(1)
	check("timeout", 503)
	mode.Store(2)
	check("", 200)
	// Real connection loss and listener rebind, without restarting workerd.
	addr := cell.Listener.Addr().String()
	cell.Close()
	check("network", 503)
	listener, e := net.Listen("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	replacement := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	replacement.Listener = listener
	replacement.Start()
	defer replacement.Close()
	check("", 200)
}

// A never-ending background poll shares the workerd HTTP client's cached
// address with readiness. It must expire, even if response headers arrived.
func TestDoRuntimeLogCapturePollTimeout(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(fmt.Sprint("headers=", headers), func(t *testing.T) {
			var polls atomic.Int32
			var cancelled atomic.Int32
			cell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/internal/log-capture" {
					polls.Add(1)
					if headers {
						w.Header().Set("Content-Type", "application/json")
						w.Write([]byte(`{"mode":`))
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
					cancelled.Add(1)
					return
				}
				w.WriteHeader(200)
			}))
			defer cell.Close()
			base, stop := startRuntimeAt(t, cell.URL, "", t.TempDir(), "", "poll-timeout")
			defer stop()
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				resp, e := noProxyClient().Get(base + "/ready")
				if e != nil {
					t.Fatal(e)
				}
				resp.Body.Close()
				if polls.Load() >= 2 && cancelled.Load() >= 1 {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Fatalf("background poll did not expire/retry: polls=%d cancelled=%d", polls.Load(), cancelled.Load())
		})
	}
}
