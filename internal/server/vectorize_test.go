package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/vectorize"
)

// newVectorizeServer wires a control server with a vectorize store.
func newVectorizeServer(t *testing.T) *Server {
	t.Helper()
	s := newControlServer(t)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	s.Store = cs
	s.Vectorize = vectorize.New(cs)
	return s
}

// vectorizeDo calls a binding endpoint with both the internal token (internal
// mux auth) and a freshly minted scoped token.
func vectorizeDo(t *testing.T, s *Server, method, path, ns, index, body string) *httptest.ResponseRecorder {
	t.Helper()
	tok, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{
		Namespace: ns, Kind: "vectorize", Name: index,
		ExpiresMs: time.Now().Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// The binding data plane needs ONLY the scoped token (like KV/D1/R2/Queue):
	// sending the broad internal token is neither required nor expected.
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("x-cellhive-scope-token", tok)
	if body != "" {
		req.Header.Set("content-type", "application/json")
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

// TestVectorizeEndpoints covers the resource config path, the operator stats and
// the tenant binding API end to end (ADR-158).
func TestVectorizeEndpoints(t *testing.T) {
	s := newVectorizeServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)

	// A vectorize resource without config is rejected (dimensions/metric are
	// immutable and required).
	rr := adminDo(t, admin, http.MethodPost, "/v1/vectorize/resources",
		"admin-tok", `{"namespace":"acme","name":"DOCS"}`)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "vectorize_bad_config") {
		t.Fatalf("config-less create = %d %s", rr.Code, rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodPost, "/v1/vectorize/resources",
		`{"namespace":"acme","name":"DOCS","config":{"dimensions":2,"metric":"cosine","description":"docs"}}`)
	if !strings.Contains(rr.Body.String(), `"scope":"acme/__vectorize__/DOCS"`) {
		t.Fatalf("create = %s", rr.Body.String())
	}
	// Bad metric is rejected.
	rr = adminDo(t, admin, http.MethodPost, "/v1/vectorize/resources",
		"admin-tok", `{"namespace":"acme","name":"BAD","config":{"dimensions":2,"metric":"hamming"}}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad metric = %d", rr.Code)
	}

	// Operator stats (ADR-157 surface).
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/vectorize/stats?namespace=acme&name=DOCS", "")
	var statsBody struct {
		Stats vectorize.Stats `json:"stats"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &statsBody); err != nil {
		t.Fatalf("stats decode: %v (%s)", err, rr.Body.String())
	}
	if statsBody.Stats.Dimensions != 2 || statsBody.Stats.Metric != "cosine" || statsBody.Stats.VectorCount != 0 {
		t.Fatalf("stats = %+v", statsBody.Stats)
	}

	// Binding: insert + query (score, values, metadata).
	const base = "/v1/vectorize/"
	rr = vectorizeDo(t, s, http.MethodPost, base+"insert", "acme", "DOCS",
		`{"vectors":[{"id":"a","values":[1,0],"metadata":{"lang":"en"}},{"id":"b","values":[0,1],"metadata":{"lang":"fr"}}]}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"count":2`) {
		t.Fatalf("insert = %d %s", rr.Code, rr.Body.String())
	}
	rr = vectorizeDo(t, s, http.MethodPost, base+"query", "acme", "DOCS",
		`{"vector":[1,0],"topK":2,"returnValues":true,"returnMetadata":"all"}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"a"`) ||
		!strings.Contains(rr.Body.String(), `"score":1`) || !strings.Contains(rr.Body.String(), `"lang":"en"`) {
		t.Fatalf("query = %d %s", rr.Code, rr.Body.String())
	}
	// Filter + namespace options.
	rr = vectorizeDo(t, s, http.MethodPost, base+"query", "acme", "DOCS",
		`{"vector":[1,0],"filter":{"lang":{"$ne":"en"}}}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"b"`) {
		t.Fatalf("filtered query = %d %s", rr.Code, rr.Body.String())
	}
	// queryById uses the stored vector.
	rr = vectorizeDo(t, s, http.MethodPost, base+"query", "acme", "DOCS", `{"id":"b","topK":1}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"b"`) {
		t.Fatalf("queryById = %d %s", rr.Code, rr.Body.String())
	}
	// get + list + describe.
	rr = vectorizeDo(t, s, http.MethodPost, base+"get", "acme", "DOCS", `{"ids":["a"]}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"a"`) {
		t.Fatalf("get = %d %s", rr.Code, rr.Body.String())
	}
	rr = vectorizeDo(t, s, http.MethodGet, base+"list", "acme", "DOCS", "")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"ids":["a","b"]`) {
		t.Fatalf("list = %d %s", rr.Code, rr.Body.String())
	}
	rr = vectorizeDo(t, s, http.MethodGet, base+"describe", "acme", "DOCS", "")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"vectorCount":2`) {
		t.Fatalf("describe = %d %s", rr.Code, rr.Body.String())
	}
	// Dimension mismatch is a 400.
	rr = vectorizeDo(t, s, http.MethodPost, base+"insert", "acme", "DOCS", `{"vectors":[{"id":"c","values":[1]}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("dim mismatch = %d %s", rr.Code, rr.Body.String())
	}
	// delete
	rr = vectorizeDo(t, s, http.MethodPost, base+"delete", "acme", "DOCS", `{"ids":["b"]}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"count":1`) {
		t.Fatalf("delete = %d %s", rr.Code, rr.Body.String())
	}

	// Metadata index management (operator).
	rr = mustAdmin(t, admin, http.MethodPost, "/v1/vectorize/metadata-index",
		`{"namespace":"acme","name":"DOCS","property":"lang","type":"string"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("create metadata index = %d %s", rr.Code, rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/vectorize/metadata-indexes?namespace=acme&name=DOCS", "")
	if !strings.Contains(rr.Body.String(), `"property":"lang"`) {
		t.Fatalf("list metadata indexes = %s", rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodDelete, "/v1/vectorize/metadata-index",
		`{"namespace":"acme","name":"DOCS","property":"lang"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete metadata index = %d", rr.Code)
	}

	// Revoking the index stops binding calls (fail closed).
	mustAdmin(t, admin, http.MethodDelete, "/v1/vectorize/resources?namespace=acme&name=DOCS", "")
	rr = vectorizeDo(t, s, http.MethodPost, base+"query", "acme", "DOCS", `{"vector":[1,0]}`)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "binding_not_registered") {
		t.Fatalf("revoked query = %d %s", rr.Code, rr.Body.String())
	}
	// A token for another namespace cannot read the index.
	tok, _ := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{
		Namespace: "other", Kind: "vectorize", Name: "DOCS", ExpiresMs: time.Now().Add(time.Minute).UnixMilli(),
	})
	req := httptest.NewRequest(http.MethodGet, base+"describe?ns=acme&index=DOCS", nil)
	req.Header.Set("x-cellhive-scope-token", tok)
	rrr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrr, req)
	if rrr.Code != http.StatusForbidden {
		t.Fatalf("cross-ns describe = %d %s", rrr.Code, rrr.Body.String())
	}
	_ = context.Background()
}

// TestVectorizeANNEndpoints covers the operator ANN lifecycle: train+rebuild via
// POST /v1/vectorize/rebuild and dropping back to flat via DELETE /v1/vectorize/ann
// (ADR-159).
func TestVectorizeANNEndpoints(t *testing.T) {
	s := newVectorizeServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/vectorize/resources",
		`{"namespace":"acme","name":"ANN","config":{"dimensions":4,"metric":"euclidean"}}`)

	// Seed enough vectors for training through the binding API.
	var sb strings.Builder
	sb.WriteString(`{"vectors":[`)
	for i := 0; i < 128; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"id":"v%d","values":[%d,%d,%d,%d]}`, i, (i*7)%101, (i*13)%101, (i*29)%101, (i*31)%101)
	}
	sb.WriteString(`]}`)
	rr := vectorizeDo(t, s, http.MethodPost, "/v1/vectorize/insert", "acme", "ANN", sb.String())
	if rr.Code != http.StatusOK {
		t.Fatalf("insert = %d %s", rr.Code, rr.Body.String())
	}

	rr = mustAdmin(t, admin, http.MethodPost, "/v1/vectorize/rebuild",
		`{"namespace":"acme","name":"ANN","buckets":8,"quantizer":"none","codesize":0,"nprobe":8}`)
	if !strings.Contains(rr.Body.String(), `"buckets":8`) {
		t.Fatalf("rebuild = %s", rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/vectorize/stats?namespace=acme&name=ANN", "")
	if !strings.Contains(rr.Body.String(), `"ann":{"buckets":8`) {
		t.Fatalf("stats missing ann: %s", rr.Body.String())
	}
	// Queries keep working while ANN is installed.
	rr = vectorizeDo(t, s, http.MethodPost, "/v1/vectorize/query", "acme", "ANN", `{"vector":[0,0,0,0],"topK":3}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"count":3`) {
		t.Fatalf("ann query = %d %s", rr.Code, rr.Body.String())
	}
	// Drop ANN -> exact flat index.
	rr = mustAdmin(t, admin, http.MethodDelete, "/v1/vectorize/ann?namespace=acme&name=ANN", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("drop ann = %d %s", rr.Code, rr.Body.String())
	}
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/vectorize/stats?namespace=acme&name=ANN", "")
	if strings.Contains(rr.Body.String(), `"ann":{`) {
		t.Fatalf("ann still installed: %s", rr.Body.String())
	}
	// The dot-product metric is explicitly unsupported by the vec1 engine.
	rr = adminDo(t, admin, http.MethodPost, "/v1/vectorize/resources",
		"admin-tok", `{"namespace":"acme","name":"DOT","config":{"dimensions":4,"metric":"dot-product"}}`)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "vectorize_bad_config") {
		t.Fatalf("dot-product create = %d %s", rr.Code, rr.Body.String())
	}
}
