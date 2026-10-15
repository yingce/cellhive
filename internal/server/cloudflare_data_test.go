package server

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCloudflareDataPlaneMatchesNativeBindings(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.AdminToken = "admin-tok"
	h := s.AdminHandler()
	call := func(method, path string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	base := "/client/v4/accounts/acme"
	for _, item := range []struct{ path, body string }{
		{"/d1/database", `{"name":"main"}`},
		{"/storage/kv/namespaces", `{"title":"KV"}`},
		{"/r2/buckets", `{"name":"bucket"}`},
	} {
		rr := call(http.MethodPost, base+item.path, []byte(item.body))
		if rr.Code != 200 {
			t.Fatalf("create %s = %d: %s", item.path, rr.Code, rr.Body.String())
		}
	}
	d1Path := base + "/d1/database/main/query"
	for _, sql := range []string{"CREATE TABLE witness(v TEXT); INSERT INTO witness VALUES ('one');", "SELECT v FROM witness"} {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		rr := call(http.MethodPost, d1Path, body)
		if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"success":true`) {
			t.Fatalf("d1 query = %d: %s", rr.Code, rr.Body.String())
		}
		if strings.HasPrefix(sql, "SELECT") && !strings.Contains(rr.Body.String(), `"v":"one"`) {
			t.Fatalf("d1 rows = %s", rr.Body.String())
		}
	}
	pragma := call(http.MethodPost, d1Path, []byte(`{"sql":"PRAGMA table_info(witness)"}`))
	if pragma.Code != 200 || !strings.Contains(pragma.Body.String(), `"name":"v"`) {
		t.Fatalf("d1 pragma = %d: %s", pragma.Code, pragma.Body.String())
	}
	kvPath := base + "/storage/kv/namespaces/KV"
	if rr := call(http.MethodPut, kvPath+"/values/a", []byte("hello")); rr.Code != 200 {
		t.Fatalf("kv put = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, kvPath+"/values/a", nil); rr.Code != 200 || rr.Body.String() != "hello" {
		t.Fatalf("kv get = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, kvPath+"/keys", nil); rr.Code != 200 || !strings.Contains(rr.Body.String(), `"name":"a"`) {
		t.Fatalf("kv keys = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodDelete, kvPath+"/values/a", nil); rr.Code != 200 {
		t.Fatalf("kv delete = %d: %s", rr.Code, rr.Body.String())
	}
	r2Path := base + "/r2/buckets/bucket/objects/dir/a.txt"
	if rr := call(http.MethodPut, r2Path, []byte("r2 bytes")); rr.Code != 200 {
		t.Fatalf("r2 put = %d: %s", rr.Code, rr.Body.String())
	}
	metaReq := httptest.NewRequest(http.MethodPut, r2Path, bytes.NewReader([]byte("r2 bytes")))
	metaReq.Header.Set("Authorization", "Bearer admin-tok")
	metaReq.Header.Set("Content-Type", "text/plain")
	metaResponse := httptest.NewRecorder()
	h.ServeHTTP(metaResponse, metaReq)
	if metaResponse.Code != 200 {
		t.Fatalf("r2 metadata put = %d: %s", metaResponse.Code, metaResponse.Body.String())
	}
	storedMeta, err := s.R2.GetMeta(t.Context(), "acme", "bucket", "dir/a.txt")
	if err != nil || storedMeta == nil || storedMeta.HTTP["contentType"] != "text/plain" {
		t.Fatalf("r2 metadata = %+v, %v", storedMeta, err)
	}
	if rr := call(http.MethodGet, r2Path, nil); rr.Code != 200 || rr.Body.String() != "r2 bytes" {
		t.Fatalf("r2 get = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodDelete, r2Path, nil); rr.Code != 200 {
		t.Fatalf("r2 delete = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(http.MethodGet, base+"/storage/kv/namespaces/other/keys", nil); rr.Code != 400 {
		t.Fatalf("unregistered KV = %d", rr.Code)
	}
}

func TestCloudflareD1FileImportRetry(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.AdminToken = "admin-tok"
	h := s.AdminHandler()
	call := func(method, path string, body []byte, auth bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		if auth {
			req.Header.Set("Authorization", "Bearer admin-tok")
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	base := "/client/v4/accounts/acme/d1/database"
	if rr := call(http.MethodPost, base, []byte(`{"name":"main"}`), true); rr.Code != 200 {
		t.Fatalf("create = %d: %s", rr.Code, rr.Body.String())
	}
	sql := []byte("CREATE TABLE t(v TEXT); INSERT INTO t VALUES ('one');")
	digest := md5.Sum(sql)
	etag := hex.EncodeToString(digest[:])
	importPath := base + "/main/import"
	initBody, _ := json.Marshal(map[string]string{"action": "init", "etag": etag})
	init := call(http.MethodPost, importPath, initBody, true)
	if init.Code != 200 {
		t.Fatalf("init = %d: %s", init.Code, init.Body.String())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(init.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Filename  string `json:"filename"`
		UploadURL string `json:"upload_url"`
	}
	if err := json.Unmarshal(envelope["result"], &result); err != nil {
		t.Fatal(err)
	}
	if result.Filename == "" || result.UploadURL == "" {
		t.Fatalf("init result = %s", init.Body.String())
	}
	path := strings.TrimPrefix(result.UploadURL, "http://example.com")
	if rr := call(http.MethodPut, path, sql, false); rr.Code != 200 {
		t.Fatalf("upload = %d: %s", rr.Code, rr.Body.String())
	}
	ingestBody, _ := json.Marshal(map[string]string{"action": "ingest", "etag": etag, "filename": result.Filename})
	for range 2 {
		if rr := call(http.MethodPost, importPath, ingestBody, true); rr.Code != 200 {
			t.Fatalf("ingest = %d: %s", rr.Code, rr.Body.String())
		}
	}
	q := call(http.MethodPost, base+"/main/query", []byte(`{"sql":"SELECT count(*) AS n FROM t"}`), true)
	if q.Code != 200 || !strings.Contains(q.Body.String(), `"n":1`) {
		t.Fatalf("import duplicate = %d: %s", q.Code, q.Body.String())
	}
}

func TestCloudflareMultiSQL(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"SELECT ';' AS x;", false},
		{"SELECT 1; -- trailing ;", false},
		{"SELECT 1; SELECT 2;", true},
		{"CREATE TABLE t(x); /* comment ; */ INSERT INTO t VALUES (1);", true},
	} {
		if got := cloudflareMultiSQL(tc.sql); got != tc.want {
			t.Errorf("%q = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

func TestCloudflareWranglerMigrationDetectsChangedFile(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.AdminToken = "admin-tok"
	h := s.AdminHandler()
	call := func(sql string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req := httptest.NewRequest(http.MethodPost, "/client/v4/accounts/acme/d1/database/main/query", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "d1", "main", "acme/__d1__/main", "ops"); err != nil {
		t.Fatal(err)
	}
	sql := "CREATE TABLE migrated(v TEXT);\nINSERT INTO \"d1_migrations\" (name) values ('0001_init.sql');"
	if rr := call(sql); rr.Code != 200 {
		t.Fatalf("apply = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := call(sql); rr.Code != 200 {
		t.Fatalf("retry = %d: %s", rr.Code, rr.Body.String())
	}
	changed := "CREATE TABLE migrated(v TEXT, extra TEXT);\nINSERT INTO \"d1_migrations\" (name) values ('0001_init.sql');"
	if rr := call(changed); rr.Code == 200 || !strings.Contains(rr.Body.String(), "content changed") {
		t.Fatalf("changed migration = %d: %s", rr.Code, rr.Body.String())
	}
}
