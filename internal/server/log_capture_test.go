package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/telemetry"
)

// TestLogCaptureEndpoint covers ADR-238: runtimes need the live subscription
// set (and OTLP mode) to decide whether to attach native Tail.
func TestLogCaptureEndpoint(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	if rr := do(t, h, http.MethodGet, "/v1/internal/log-capture", "", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rr.Code)
	}
	rr := do(t, h, http.MethodGet, "/v1/internal/log-capture", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("capture = %d %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Mode    string   `json:"mode"`
		Workers []string `json:"workers"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, rr.Body.String())
	}
	if got.Mode != telemetry.LogsMode() {
		t.Fatalf("mode = %q, want %q", got.Mode, telemetry.LogsMode())
	}
	hasWorker := func(list []string, want string) bool {
		for _, w := range list {
			if w == want {
				return true
			}
		}
		return false
	}
	if hasWorker(got.Workers, "capture-test/worker") {
		t.Fatalf("workers = %v, want acme/web absent before subscription", got.Workers)
	}

	telemetry.Subscribe("capture-test", "worker", time.Minute)
	rr = do(t, h, http.MethodGet, "/v1/internal/log-capture", "tok", nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !hasWorker(got.Workers, "capture-test/worker") {
		t.Fatalf("workers = %v, want acme/web", got.Workers)
	}
	telemetry.Subscribe("capture-test", "worker", time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	rr = do(t, h, http.MethodGet, "/v1/internal/log-capture", "tok", nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hasWorker(got.Workers, "capture-test/worker") {
		t.Fatalf("workers after expiry = %v, want acme/web absent", got.Workers)
	}
}
