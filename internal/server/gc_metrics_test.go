package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/compaction"
)

func TestMetricsExposeGCBacklog(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gc-metrics"}
	if _, _, err := s.Replica.Append(ctx, sc, 1, nativeTestFile(t, 1, 1, []byte("metrics"))); err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.Compact(ctx, s.Replica, sc, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	}
	// An explicit diagnostic refresh populates metrics; neither compaction nor
	// scraping adds a broad bucket listing.
	compaction.RecordStats(ctx, s.Replica, sc, 1)
	rr := httptest.NewRecorder()
	s.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		"cellhive_l0_backlog_objects{scope=\"acme/__kv__/gc-metrics\"}",
		"cellhive_l1_generations{scope=\"acme/__kv__/gc-metrics\"}",
		"cellhive_deferred_gc_bytes{scope=\"acme/__kv__/gc-metrics\"}",
		"cellhive_retained_epochs{scope=\"acme/__kv__/gc-metrics\"}",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
