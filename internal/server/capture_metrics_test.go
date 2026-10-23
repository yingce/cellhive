package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestCaptureEnsureMetrics(t *testing.T) {
	s, _ := newTestServer(t)
	s.RecordCaptureEnsure(cell.Scope{Namespace: "acme"}, "resume", 12*time.Millisecond)
	s.RecordCaptureEnsure(cell.Scope{Namespace: "acme"}, "snapshot", 2*time.Second)
	rr := do(t, s.Handler(), http.MethodGet, "/metrics", "", nil)
	body := rr.Body.String()
	for _, want := range []string{
		`cellhive_capture_ensure_seconds_count{ns="acme"} 2`,
		`cellhive_capture_ensure_total{ns="acme",mode="resume"} 1`,
		`cellhive_capture_ensure_total{ns="acme",mode="snapshot"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}
