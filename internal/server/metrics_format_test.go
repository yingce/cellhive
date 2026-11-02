package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestEscapeLabel pins the Prometheus text-format escaping rules: only
// backslash, double quote and newline are escaped; other bytes (tab, UTF-8)
// stay raw because the format only defines \\, \" and \n.
func TestEscapeLabel(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{"plain", "plain"},
		{`a"b`, `a\"b`},
		{`a\b`, `a\\b`},
		{"a\nb", `a\nb`},
		{"a\tb", "a\tb"},
		{"é", "é"},
	} {
		if got := escapeLabel(tc.in); got != tc.want {
			t.Fatalf("escapeLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestMetricsLabelEscaping covers Prometheus text-format compatibility. Go's
// %q emits \t/\xNN/\uNNNN for some values, which a Prometheus scraper rejects;
// dynamic label values must go through escapeLabel instead.
func TestMetricsLabelEscaping(t *testing.T) {
	srv, _ := newTestServer(t)
	ns := "acme\"q\\b\nc\td\u00e9"
	srv.recordBindingCall(ns, "kv", http.StatusOK)
	srv.recordProof(ns, 3*time.Millisecond)

	rr := httptest.NewRecorder()
	srv.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if got := rr.Header().Get("content-type"); got != "text/plain; version=0.0.4" {
		t.Fatalf("content-type = %q, want Prometheus text format", got)
	}
	body := rr.Body.String()
	assertPrometheusText(t, body)

	escaped := `acme\"q\\b\nc` + "\t" + `d` + "\u00e9"
	for _, want := range []string{
		`cellhive_binding_calls_total{ns="` + escaped + `",kind="kv",outcome="ok"} 1`,
		`cellhive_durability_proof_seconds_count{ns="` + escaped + `"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
	for _, bad := range []string{`\t`, `\x`, `\u`} {
		if strings.Contains(body, bad) {
			t.Fatalf("metrics contain non-Prometheus escape %q\n%s", bad, body)
		}
	}
}

// assertPrometheusText rejects label blocks a Prometheus scraper would fail on:
// an unclosed quote/label block or an escape other than \\, \" and \n. It also
// checks that any histogram family with samples carries _sum, _count and the
// +Inf bucket.
func assertPrometheusText(t *testing.T, body string) {
	t.Helper()
	types := map[string]string{}
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if len(fields) != 4 {
				t.Fatalf("bad TYPE line %q", line)
			}
			types[fields[2]] = fields[3]
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		open := strings.IndexByte(line, '{')
		if open < 0 {
			continue
		}
		inQuote, closed := false, false
		for i := open + 1; i < len(line); i++ {
			switch {
			case inQuote && line[i] == '\\':
				i++
				if i >= len(line) {
					t.Fatalf("dangling escape in %q", line)
				}
				switch line[i] {
				case '\\', '"', 'n':
				default:
					t.Fatalf("metric line %q uses non-Prometheus escape \\%c", line, line[i])
				}
			case inQuote && line[i] == '"':
				inQuote = false
			case !inQuote && line[i] == '"':
				inQuote = true
			case !inQuote && line[i] == '}':
				closed = true
				i = len(line)
			}
		}
		if inQuote || !closed {
			t.Fatalf("unterminated label block in %q", line)
		}
	}
	for name, typ := range types {
		if typ != "histogram" || !strings.Contains(body, name+"_bucket{") {
			continue
		}
		for _, want := range []string{name + "_sum{", name + "_count{", `le="+Inf"`} {
			if !strings.Contains(body, want) {
				t.Fatalf("histogram %q missing %q\n%s", name, want, body)
			}
		}
	}
}
