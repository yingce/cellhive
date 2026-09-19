package dosupervisor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSupervisorMetricsAndStats covers ADR-166: host actors report counter
// deltas and a ws-session delta to /internal/do/stats, and GET /metrics renders
// the do-runtime metrics (with the session gauge clamped at zero).
func TestSupervisorMetricsAndStats(t *testing.T) {
	s := &Supervisor{}
	h := s.Handler()

	post := func(body string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/internal/do/stats", strings.NewReader(body)))
		return rr
	}
	if rr := post(`{"alarms_ok":3,"alarms_error":1,"gate_timeouts":2,"ws_sessions":5}`); rr.Code != http.StatusOK {
		t.Fatalf("stats = %d %s", rr.Code, rr.Body.String())
	}
	// A host restart resets its local count; a large negative delta clamps to 0.
	if rr := post(`{"ws_sessions":-99}`); rr.Code != http.StatusOK {
		t.Fatalf("stats2 = %d %s", rr.Code, rr.Body.String())
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	out := rr.Body.String()
	for _, want := range []string{
		`cellhive_do_alarms_fired_total{outcome="ok"} 3`,
		`cellhive_do_alarms_fired_total{outcome="error"} 1`,
		`cellhive_do_output_gate_timeouts_total 2`,
		`cellhive_do_ws_sessions 0`,
		`# TYPE cellhive_do_wal_captured_bytes_total counter`,
		`# TYPE cellhive_do_restore_seconds summary`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q\n%s", want, out)
		}
	}
}
