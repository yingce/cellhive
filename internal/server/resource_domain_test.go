package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/auth"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/d1"
	queuepkg "cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/workflow"
)

// newDomainServer builds a control server with every data store wired so the
// per-domain resource + stats surface can be exercised end to end (ADR-157).
func newDomainServer(t *testing.T) *Server {
	t.Helper()
	s := newControlServer(t)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	s.Store = cs
	s.D1 = d1.New(cs)
	s.Queue = queuepkg.New(cs)
	s.Workflows = workflow.New(cs)
	s.R2 = r2.New(s.Bucket)
	return s
}

func statsBody(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return out
}

// TestPerKindResourceEndpoints covers the per-domain registry CRUD and its
// equivalence with the generic /v1/control/resource* aliases.
func TestPerKindResourceEndpoints(t *testing.T) {
	s := newDomainServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"other"}`)

	// Per-kind create defaults the scope server-side.
	rr := mustAdmin(t, admin, http.MethodPost, "/v1/kv/resources", `{"namespace":"acme","name":"KV"}`)
	if !strings.Contains(rr.Body.String(), `"scope":"acme/__kv__/KV"`) {
		t.Fatalf("kv create = %s", rr.Body.String())
	}
	// The generic list sees per-kind creates...
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/control/resources?namespace=acme&kind=kv", "")
	if !strings.Contains(rr.Body.String(), `"name":"KV"`) {
		t.Fatalf("generic list = %s", rr.Body.String())
	}
	// ...and the per-kind list sees generic creates.
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource", `{"namespace":"acme","kind":"d1","name":"DB"}`)
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/d1/resources?namespace=acme", "")
	if !strings.Contains(rr.Body.String(), `"name":"DB"`) || !strings.Contains(rr.Body.String(), `"kind":"d1"`) {
		t.Fatalf("d1 list = %s", rr.Body.String())
	}
	// A body kind conflicting with the path is rejected.
	rr = adminDo(t, admin, http.MethodPost, "/v1/kv/resources", "admin-tok", `{"namespace":"acme","kind":"queue","name":"X"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("kind mismatch = %d, want 400", rr.Code)
	}
	// Revoke: unknown -> 404, known -> 200, then the entry is gone.
	rr = adminDo(t, admin, http.MethodDelete, "/v1/kv/resources?namespace=acme&name=NOPE", "admin-tok", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("revoke unknown = %d, want 404", rr.Code)
	}
	rr = mustAdmin(t, admin, http.MethodDelete, "/v1/kv/resources?namespace=acme&name=KV", "")
	if !strings.Contains(rr.Body.String(), `"ok":true`) {
		t.Fatalf("revoke = %s", rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/kv/resources?namespace=acme", "")
	if strings.Contains(rr.Body.String(), `"name":"KV"`) {
		t.Fatalf("revoked resource still listed: %s", rr.Body.String())
	}
	// An in-use resource still requires force (ADR-156 semantics on the new path).
	b := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(b.Body.Bytes(), &bo)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/deploy",
		`{"namespace":"acme","worker":"web","bundle_sha":"`+bo.Sha+`","bindings":[{"type":"d1","name":"DB","id":"acme/__d1__/DB"}]}`)
	rr = adminDo(t, admin, http.MethodDelete, "/v1/d1/resources?namespace=acme&name=DB", "admin-tok", "")
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), `"referenced_by":["web"]`) {
		t.Fatalf("in-use revoke = %d %s", rr.Code, rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodDelete, "/v1/d1/resources?namespace=acme&name=DB&force=1", "")
	if !strings.Contains(rr.Body.String(), `"forced":true`) {
		t.Fatalf("forced revoke = %s", rr.Body.String())
	}
}

// TestPerKindStats covers each per-domain stats endpoint with real data.
func TestPerKindStats(t *testing.T) {
	ctx := context.Background()
	s := newDomainServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)

	// KV: page/file accounting, the expiry index, exact key counts.
	mustAdmin(t, admin, http.MethodPost, "/v1/kv/resources", `{"namespace":"acme","name":"KV"}`)
	kvCell, err := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "KV"})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b"} {
		if err := kvCell.PutOpts(ctx, k, []byte("value"), nil, time.Now().Add(time.Hour).UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	rr := mustAdmin(t, admin, http.MethodGet, "/v1/kv/stats?namespace=acme&name=KV&exact=1", "")
	kv := statsBody(t, rr)["stats"].(map[string]any)
	if kv["expires_indexed"] != true {
		t.Fatalf("expires_indexed = %v", kv["expires_indexed"])
	}
	if kv["keys"] != float64(2) || kv["next_expiry_ms"] == float64(0) {
		t.Fatalf("kv stats = %v", kv)
	}
	if kv["size_bytes"] == float64(0) || kv["total_bytes"] == float64(0) {
		t.Fatalf("kv size stats = %v", kv)
	}

	// D1: table inventory (+ opt-in per-table dbstat).
	mustAdmin(t, admin, http.MethodPost, "/v1/d1/resources", `{"namespace":"acme","name":"DB"}`)
	if _, err := s.D1.Exec(ctx, "acme", "DB", `CREATE TABLE users(id INTEGER PRIMARY KEY)`, nil); err != nil {
		t.Fatal(err)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/d1/stats?namespace=acme&name=DB", "")
	d1s := statsBody(t, rr)["stats"].(map[string]any)
	if d1s["tables"] != float64(1) || d1s["sqlite_version"] == "" {
		t.Fatalf("d1 stats = %v", d1s)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/d1/stats?namespace=acme&name=DB&tables=1", "")
	if body := rr.Body.String(); !strings.Contains(body, `"table_stats"`) || !strings.Contains(body, `"name":"users"`) {
		t.Fatalf("d1 table stats = %s", body)
	}

	// Queue: depth + lag fields via the per-domain path; the legacy flat
	// endpoint keeps working.
	mustAdmin(t, admin, http.MethodPost, "/v1/queue/resources", `{"namespace":"acme","name":"Q"}`)
	if _, err := s.Queue.Send(ctx, "acme", "Q", []byte("m"), "text/plain", 0, ""); err != nil {
		t.Fatal(err)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/queue/stats?namespace=acme&name=Q", "")
	qs := statsBody(t, rr)["stats"].(map[string]any)
	if qs["depth"] != float64(1) || qs["visible"] != float64(1) {
		t.Fatalf("queue stats = %v", qs)
	}
	if _, ok := qs["oldest_visible_ms"]; !ok {
		t.Fatalf("queue stats missing lag fields: %v", qs)
	}
	if _, ok := qs["disk"]; !ok {
		t.Fatalf("queue stats missing disk: %v", qs)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/control/queue/status?namespace=acme&queue=Q", "")
	legacy := statsBody(t, rr)
	if legacy["depth"] != float64(1) {
		t.Fatalf("legacy queue status = %v", legacy)
	}
	if _, wrapped := legacy["stats"]; wrapped {
		t.Fatalf("legacy queue status must stay flat: %v", legacy)
	}

	// R2: bounded listing stats.
	mustAdmin(t, admin, http.MethodPost, "/v1/r2/resources", `{"namespace":"acme","name":"BUCKET"}`)
	if _, err := s.R2.Put(ctx, "acme", "BUCKET", "k1", []byte("12345")); err != nil {
		t.Fatal(err)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/r2/stats?namespace=acme&bucket=BUCKET", "")
	r2s := statsBody(t, rr)["stats"].(map[string]any)
	if r2s["objects"] != float64(1) || r2s["bytes"] != float64(5) || r2s["truncated"] != false {
		t.Fatalf("r2 stats = %v", r2s)
	}

	// Workflow: exact instance breakdown on request.
	mustAdmin(t, admin, http.MethodPost, "/v1/workflow/resources", `{"namespace":"acme","name":"WF"}`)
	if _, err := s.Workflows.Create(ctx, "acme", "WF", "", nil); err != nil {
		t.Fatal(err)
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/workflow/stats?namespace=acme&name=WF&exact=1", "")
	wfs := statsBody(t, rr)["stats"].(map[string]any)
	if wfs["instances"] != float64(1) {
		t.Fatalf("workflow stats = %v", wfs)
	}

	// Hyperdrive: registration metadata only (never the origin URL).
	mustAdmin(t, admin, http.MethodPost, "/v1/hyperdrive/resources",
		`{"namespace":"acme","name":"HY","connection_string":"postgres://u:p@db:5432/app"}`)
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/hyperdrive/stats?namespace=acme&name=HY", "")
	if strings.Contains(rr.Body.String(), "postgres://") {
		t.Fatalf("hyperdrive stats leaked the origin URL: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"has_config":true`) {
		t.Fatalf("hyperdrive stats = %s", rr.Body.String())
	}

	// DO: the object index is off in this config, so stats must say so.
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/do/stats?namespace=acme", "")
	if !strings.Contains(rr.Body.String(), `"available":false`) ||
		!strings.Contains(rr.Body.String(), "object_index_disabled") {
		t.Fatalf("do stats = %s", rr.Body.String())
	}
}

// TestStatsFailClosedAndScoped: stats 404 for revoked/unknown resources and 403
// for a namespace the caller has no grant on.
func TestStatsFailClosedAndScoped(t *testing.T) {
	ctx := context.Background()
	s := newDomainServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"other"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/kv/resources", `{"namespace":"acme","name":"KV"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/kv/resources", `{"namespace":"other","name":"FOREIGN"}`)

	// Unknown resource -> 404.
	rr := adminDo(t, admin, http.MethodGet, "/v1/kv/stats?namespace=acme&name=NOPE", "admin-tok", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown stats = %d, want 404", rr.Code)
	}
	// Revoked resource -> 404 (tombstone).
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource", `{"namespace":"acme","kind":"queue","name":"Q"}`)
	mustAdmin(t, admin, http.MethodDelete, "/v1/queue/resources?namespace=acme&name=Q", "")
	rr = adminDo(t, admin, http.MethodGet, "/v1/queue/stats?namespace=acme&name=Q", "admin-tok", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("revoked stats = %d, want 404", rr.Code)
	}

	// A JWT scoped to acme cannot read another namespace's stats.
	secret := []byte("test-secret-0123456789abcdef")
	s.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	token := signJWT(t, secret, map[string]any{
		"sub": "alice@corp", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	rr = adminTokenDo(t, s, http.MethodGet, "/v1/kv/stats?namespace=other&name=FOREIGN", token, "")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("foreign ns stats = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	_ = ctx
}

// TestMetricsBucketAndCellGauges: /metrics exposes the bucket op counters, the
// hot-path List counter and the owned/resident cell gauges (ADR-157 doc sync).
func TestMetricsBucketAndCellGauges(t *testing.T) {
	s := newDomainServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)

	rr := do(t, s.Handler(), http.MethodGet, "/metrics", "", nil)
	body := rr.Body.String()
	for _, want := range []string{
		"cellhive_list_calls_total",
		"cellhive_bucket_ops_total{op=\"list\"}",
		"cellhive_resident_cells",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, body)
		}
	}
}

// TestMetricsPagedStats: /metrics exposes the paged-cell counters (ADR-160).
func TestMetricsPagedStats(t *testing.T) {
	s := newDomainServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	rr := do(t, s.Handler(), http.MethodGet, "/metrics", "", nil)
	body := rr.Body.String()
	for _, want := range []string{
		"cellhive_paged_cells",
		"cellhive_paged_faults_total",
		"cellhive_paged_runs_total",
		"cellhive_paged_prefetch_hits_total",
		"cellhive_paged_hydrated_pages",
		"cellhive_paged_total_pages",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q", want)
		}
	}
}
