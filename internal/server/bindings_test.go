package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func scopeDo(t *testing.T, s *Server, method, path, scopeToken string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("x-cellhive-internal-token", "tok")
	if scopeToken != "" {
		req.Header.Set("x-cellhive-scope-token", scopeToken)
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

func TestBindingAPIs(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	for _, res := range []struct{ kind, name string }{{"kv", "main"}, {"d1", "main"}, {"r2", "files"}, {"queue", "jobs"}} {
		if _, err := s.Control.CreateResource(ctx, "acme", res.kind, res.name, "acme/__"+res.kind+"__/"+res.name, "ops"); err != nil {
			t.Fatalf("resource %s: %v", res.kind, err)
		}
	}
	kvT := mint(t, s, "acme", "kv", "main")
	d1T := mint(t, s, "acme", "d1", "main")
	r2T := mint(t, s, "acme", "r2", "files")
	qT := mint(t, s, "acme", "queue", "jobs")

	// KV: put/get/delete/list.
	if rr := scopeDo(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=a", kvT, []byte("v1")); rr.Code != 200 {
		t.Fatalf("kv put = %d: %s", rr.Code, rr.Body.String())
	}
	_ = scopeDo(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=b", kvT, []byte("v2"))
	if rr := scopeDo(t, s, http.MethodGet, "/v1/kv/list?ns=acme&prefix=", kvT, nil); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"a"`)) {
		t.Fatalf("kv list = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodDelete, "/v1/kv/delete?ns=acme&key=a", kvT, nil); rr.Code != 200 {
		t.Fatalf("kv delete = %d", rr.Code)
	}
	if rr := scopeDo(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=a", kvT, nil); rr.Code != 404 {
		t.Fatalf("kv get after delete = %d, want 404", rr.Code)
	}

	// D1: exec create+insert, query, batch.
	execSQL := func(sql string, params ...any) []byte {
		b, _ := json.Marshal(map[string]any{"sql": sql, "params": params})
		return b
	}
	if rr := scopeDo(t, s, http.MethodPost, "/v1/d1/exec?ns=acme&db=main", d1T, execSQL(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`)); rr.Code != 200 {
		t.Fatalf("d1 create = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodPost, "/v1/d1/exec?ns=acme&db=main", d1T, execSQL(`INSERT INTO t (id,n) VALUES (?,?)`, 1, "one")); rr.Code != 200 {
		t.Fatalf("d1 insert = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodPost, "/v1/d1/query?ns=acme&db=main", d1T, execSQL(`SELECT n FROM t WHERE id=?`, 1)); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"one"`)) {
		t.Fatalf("d1 query = %d: %s", rr.Code, rr.Body.String())
	}
	batchBody, _ := json.Marshal(map[string]any{"statements": []map[string]any{
		{"sql": `INSERT INTO t (id,n) VALUES (2,'two')`}, {"sql": `SELECT count(1) FROM t`},
	}})
	if rr := scopeDo(t, s, http.MethodPost, "/v1/d1/batch?ns=acme&db=main", d1T, batchBody); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte(`"success":true`)) {
		t.Fatalf("d1 batch = %d: %s", rr.Code, rr.Body.String())
	}

	// R2: put/get/range/list/delete.
	if rr := scopeDo(t, s, http.MethodPut, "/v1/r2/object?ns=acme&bucket=files&key=dir/x.txt", r2T, []byte("hello-r2")); rr.Code != 200 {
		t.Fatalf("r2 put = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodGet, "/v1/r2/object?ns=acme&bucket=files&key=dir/x.txt", r2T, nil); rr.Code != 200 || rr.Body.String() != "hello-r2" {
		t.Fatalf("r2 get = %d %q", rr.Code, rr.Body.String())
	}
	rreq := httptest.NewRequest(http.MethodGet, "/v1/r2/object?ns=acme&bucket=files&key=dir/x.txt", nil)
	rreq.Header.Set("x-cellhive-internal-token", "tok")
	rreq.Header.Set("x-cellhive-scope-token", r2T)
	rreq.Header.Set("Range", "bytes=0-4")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, rreq)
	if rr.Code != http.StatusPartialContent || rr.Body.String() != "hello" {
		t.Fatalf("r2 range = %d %q", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodGet, "/v1/r2/list?ns=acme&bucket=files&prefix=dir/", r2T, nil); rr.Code != 200 || !bytes.Contains(rr.Body.Bytes(), []byte("dir/x.txt")) {
		t.Fatalf("r2 list = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := scopeDo(t, s, http.MethodDelete, "/v1/r2/object?ns=acme&bucket=files&key=dir/x.txt", r2T, nil); rr.Code != 200 {
		t.Fatalf("r2 delete = %d", rr.Code)
	}

	// Queue: send/claim/ack, idempotent send.
	send := func(key string) string {
		rr := scopeDo(t, s, http.MethodPost, "/v1/queue/send?ns=acme&queue=jobs&idempotency_key="+key, qT, []byte("payload"))
		if rr.Code != 200 {
			t.Fatalf("queue send = %d: %s", rr.Code, rr.Body.String())
		}
		var out struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &out)
		return out.ID
	}
	id1 := send("k1")
	if id2 := send("k1"); id2 != id1 {
		t.Fatalf("idempotent send ids differ: %s %s", id1, id2)
	}
	rr = scopeDo(t, s, http.MethodPost, "/v1/queue/claim?ns=acme&queue=jobs&limit=10", qT, nil)
	if rr.Code != 200 {
		t.Fatalf("queue claim = %d: %s", rr.Code, rr.Body.String())
	}
	var claimed struct {
		Messages []struct {
			ID   string `json:"id"`
			Body string `json:"body"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &claimed)
	if len(claimed.Messages) != 1 {
		t.Fatalf("claim = %+v", claimed)
	}
	if b, _ := base64.StdEncoding.DecodeString(claimed.Messages[0].Body); string(b) != "payload" {
		t.Fatalf("claimed body = %q", claimed.Messages[0].Body)
	}
	if rr := scopeDo(t, s, http.MethodPost, "/v1/queue/ack?ns=acme&queue=jobs&id="+claimed.Messages[0].ID, qT, nil); rr.Code != 200 {
		t.Fatalf("queue ack = %d", rr.Code)
	}

	// Cross-kind token is rejected.
	if rr := scopeDo(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=b", d1T, nil); rr.Code != http.StatusForbidden {
		t.Fatalf("cross-kind token = %d, want 403", rr.Code)
	}
}

func TestQueueSendRejectsOversizedBodyWithoutTruncation(t *testing.T) {
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(context.Background(), "acme", "queue", "jobs", "acme/__queue__/jobs", "ops"); err != nil {
		t.Fatal(err)
	}
	token := mint(t, s, "acme", "queue", "jobs")
	oversized := bytes.Repeat([]byte("x"), (1<<20)+1)
	rr := scopeDo(t, s, http.MethodPost, "/v1/queue/send?ns=acme&queue=jobs", token, oversized)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized queue send = %d: %s", rr.Code, rr.Body.String())
	}
	if depth, err := s.Queue.Depth(context.Background(), "acme", "jobs"); err != nil || depth != 0 {
		t.Fatalf("queue depth after rejected send = %d, %v; want 0", depth, err)
	}
}

// TestR2ListCursorPaging: /v1/r2/list is cursor-paged with sizes and reports
// truncation (ADR-145).
func TestR2ListCursorPaging(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "r2", "files", "acme/__r2__/files", "ops"); err != nil {
		t.Fatal(err)
	}
	r2T := mint(t, s, "acme", "r2", "files")
	for _, k := range []string{"d/a", "d/b", "d/c"} {
		if rr := scopeDo(t, s, http.MethodPut, "/v1/r2/object?ns=acme&bucket=files&key="+k, r2T, []byte(k)); rr.Code != 200 {
			t.Fatalf("put %s = %d", k, rr.Code)
		}
	}
	page := func(cursor string) (keys []string, truncated bool, next string) {
		t.Helper()
		url := "/v1/r2/list?ns=acme&bucket=files&prefix=d/&limit=2"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		rr := scopeDo(t, s, http.MethodGet, url, r2T, nil)
		if rr.Code != 200 {
			t.Fatalf("list = %d: %s", rr.Code, rr.Body.String())
		}
		var out struct {
			Objects []struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"objects"`
			Truncated bool   `json:"truncated"`
			Cursor    string `json:"cursor"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, o := range out.Objects {
			if o.Size != int64(len(o.Key)) {
				t.Fatalf("size for %s = %d", o.Key, o.Size)
			}
			keys = append(keys, o.Key)
		}
		return keys, out.Truncated, out.Cursor
	}
	k1, trunc, cur := page("")
	if len(k1) != 2 || !trunc || cur != "d/b" {
		t.Fatalf("page1 = %v truncated=%v cursor=%q", k1, trunc, cur)
	}
	k2, trunc2, cur2 := page(cur)
	if len(k2) != 1 || k2[0] != "d/c" || trunc2 || cur2 != "" {
		t.Fatalf("page2 = %v truncated=%v cursor=%q", k2, trunc2, cur2)
	}
}
func TestKVGetBatchPreservesOrderMissingAndMetadata(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatal(err)
	}
	tok := mint(t, s, "acme", "kv", "main")
	meta := base64.StdEncoding.EncodeToString([]byte(`{"tag":"x"}`))
	if rr := scopeDo(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=a&metadata="+meta, tok, []byte("one")); rr.Code != http.StatusOK {
		t.Fatalf("put = %d: %s", rr.Code, rr.Body.String())
	}
	body, _ := json.Marshal(map[string]any{"keys": []string{"a", "missing", "a"}})
	rr := scopeDo(t, s, http.MethodPost, "/v1/kv/get-batch?ns=acme", tok, body)
	if rr.Code != http.StatusOK {
		t.Fatalf("batch = %d: %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Results []struct {
			Found    bool   `json:"found"`
			Value    string `json:"value"`
			Metadata string `json:"metadata"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 3 || !got.Results[0].Found || got.Results[1].Found || !got.Results[2].Found {
		t.Fatalf("batch result shape = %+v", got.Results)
	}
	for _, i := range []int{0, 2} {
		value, err := base64.StdEncoding.DecodeString(got.Results[i].Value)
		if err != nil || string(value) != "one" || got.Results[i].Metadata != meta {
			t.Fatalf("result[%d] = %+v, decoded=%q err=%v", i, got.Results[i], value, err)
		}
	}
}

func TestKVGetBatchAcceptsMaximumSizedKeys(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, maxKVGetBatchKeys)
	for i := range keys {
		keys[i] = strings.Repeat(string(rune('a'+i%26)), 512)
	}
	body, _ := json.Marshal(map[string]any{"keys": keys})
	rr := scopeDo(t, s, http.MethodPost, "/v1/kv/get-batch?ns=acme", mint(t, s, "acme", "kv", "main"), body)
	if rr.Code != http.StatusOK {
		t.Fatalf("maximum batch (%d bytes) = %d: %s", len(body), rr.Code, rr.Body.String())
	}
}
