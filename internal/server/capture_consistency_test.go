package server

import (
	"cellhive/internal/cellstore"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/d1"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/scopedtoken"
)

func captureServer(t *testing.T) (*Server, *replica.Manager, []byte) {
	t.Helper()
	srv, _ := newTestServer(t)
	srv.D1 = d1.New(srv.Store)
	if err := registerTestBinding(srv, "acme", "kv", "KV", "default"); err != nil {
		t.Fatalf("kv binding: %v", err)
	}
	if _, err := srv.Control.CreateResource(context.Background(), "acme", "d1", "DB", "bench", "test"); err != nil {
		t.Fatalf("d1 binding: %v", err)
	}
	rep := replica.New(srv.Bucket)
	srv.Capture = &cellcapture.Manager{
		Store:        srv.Store,
		Committer:    replicaCommitter{rep: rep},
		AutoSnapshot: true,
		Owner:        func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
	}
	return srv, rep, []byte(srv.Cfg.ScopeSecret)
}

// restoreDB restores a scope from the bucket into a fresh directory and returns
// an opened database, mirroring cold recovery on a node that never saw the
// original writes (RPO=0 for acked writes).
func restoreDB(t *testing.T, rep *replica.Manager, scope cell.Scope, epoch uint64) *sql.DB {
	t.Helper()
	segs, err := rep.Restore(context.Background(), scope, epoch)
	if err != nil {
		t.Fatalf("restore %s: %v", scope, err)
	}
	if len(segs) == 0 {
		t.Fatalf("restore %s: no segments", scope)
	}
	raw := make([][]byte, 0, len(segs))
	for _, s := range segs {
		raw = append(raw, s.Raw)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(dst, raw); err != nil {
		t.Fatalf("apply %s: %v", scope, err)
	}
	db, err := cellstore.Open(dst)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	var res string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&res); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if res != "ok" {
		t.Fatalf("integrity_check = %q, want ok", res)
	}
	return db
}

// TestKVCaptureConsistencyN writes N unique KV keys with mixed value sizes,
// then restores from the bucket and verifies byte-exact values, no missing and
// no duplicate keys, and SQLite integrity.
func TestKVCaptureConsistencyN(t *testing.T) {
	const n = 250
	srv, rep, secret := captureServer(t)
	h := srv.Handler()
	tok, err := scopedtoken.Mint(secret, scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "KV"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%04d", i)
		// 0..~8KB, all unique and byte-distinct.
		val := fmt.Sprintf("v-%04d-%s", i, strings.Repeat("x", (i*37)%8192))
		want[key] = val
		req := httptest.NewRequest(http.MethodPost, "/v1/kv/put?ns=acme&key="+key, bytesReader(val))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("put %s = %d %s", key, rr.Code, rr.Body.String())
		}
	}

	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	db := restoreDB(t, rep, scope, 1)

	got := make(map[string]string, n)
	rows, err := db.Query("SELECT key, value FROM kv")
	if err != nil {
		t.Fatalf("query restored kv: %v", err)
	}
	defer rows.Close()
	dup := 0
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if _, seen := got[k]; seen {
			dup++
		}
		got[k] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if dup != 0 {
		t.Fatalf("restored %d duplicate keys, want 0", dup)
	}
	if len(got) != n {
		t.Fatalf("restored %d keys, want %d", len(got), n)
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("missing key %s after restore", k)
		}
		if gv != v {
			t.Fatalf("value mismatch for %s: got %d bytes, want %d bytes", k, len(gv), len(v))
		}
	}
}

// TestD1CaptureConsistency writes rows through D1 and verifies they survive a
// bucket restore byte-exact with integrity intact.
func TestD1CaptureConsistency(t *testing.T) {
	const n = 100
	srv, rep, secret := captureServer(t)
	h := srv.Handler()
	tok, err := scopedtoken.Mint(secret, scopedtoken.Claims{Namespace: "acme", Kind: "d1", Name: "DB"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	do := func(sqlStr string, params ...any) {
		t.Helper()
		pj, _ := json.Marshal(params)
		body := fmt.Sprintf(`{"sql":%q,"params":%s}`, sqlStr, pj)
		req := httptest.NewRequest(http.MethodPost, "/v1/d1/exec?ns=acme&db=main", bytesReader(body))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("d1 exec %q = %d %s", sqlStr, rr.Code, rr.Body.String())
		}
	}
	do("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, extra TEXT NOT NULL)")
	want := make(map[int64]string, n)
	for i := 0; i < n; i++ {
		val := fmt.Sprintf("row-%03d-%s", i, strings.Repeat("y", (i*53)%512))
		want[int64(i)] = val
		do("INSERT INTO items (id, name, extra) VALUES (?, ?, ?)", int64(i), val, val)
	}

	db := restoreDB(t, rep, d1.Scope("acme", "main"), 1)
	rows, err := db.Query("SELECT id, name, extra FROM items")
	if err != nil {
		t.Fatalf("query restored items: %v", err)
	}
	defer rows.Close()
	got := make(map[int64]string)
	for rows.Next() {
		var id int64
		var name, extra string
		if err := rows.Scan(&id, &name, &extra); err != nil {
			t.Fatalf("scan: %v", err)
		}
		e := want[id]
		if name != e || extra != e {
			t.Fatalf("row %d mismatch: name=%q extra=%q want %q", id, name, extra, e)
		}
		got[id] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) != n {
		var missing []int64
		for i := 0; i < n; i++ {
			if _, ok := got[int64(i)]; !ok {
				missing = append(missing, int64(i))
			}
		}
		t.Fatalf("restored %d rows, want %d; missing ids=%v", len(got), n, missing)
	}
}
