package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/config"
	"cellhive/internal/logbuf"
)

// BenchmarkLogIngest measures the per-line cost of log ingest with and without
// the W3C traceparent the tenant log tail now attaches (ADR-178). The parsing
// is the only cost the observability change adds to this (non-hot, async) path.
func benchmarkLogIngest(b *testing.B, body []byte) {
	srv := New(Deps{Cfg: config.Config{TokenLog: "tok"}, Logs: logbuf.New(1000, 200)})
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/logs?ns=acme&worker=web", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Body = io.NopCloser(bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.handleLogIngest(rr, req)
	}
}

func BenchmarkLogIngestPlain(b *testing.B) {
	benchmarkLogIngest(b, []byte(`[{"level":"log","message":"hello world"},{"level":"error","message":"boom"}]`))
}

func BenchmarkLogIngestTraceparent(b *testing.B) {
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	benchmarkLogIngest(b, []byte(`[{"level":"log","message":"hello world","traceparent":"`+tp+`"},{"level":"error","message":"boom","traceparent":"`+tp+`"}]`))
}
