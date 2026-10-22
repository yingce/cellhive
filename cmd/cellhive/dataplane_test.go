package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRootKey = "00112233445566778899aabbccddeeff"

func scopeTestServer(t *testing.T, wantMethod, wantPath string, wantQuery map[string]string, gotBody *[]byte) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != wantMethod {
			t.Errorf("method = %s, want %s", r.Method, wantMethod)
		}
		if r.URL.Path != wantPath {
			t.Errorf("path = %s, want %s", r.URL.Path, wantPath)
		}
		for k, want := range wantQuery {
			if got := r.URL.Query().Get(k); got != want {
				t.Errorf("query %s = %q, want %q", k, got, want)
			}
		}
		if gotBody != nil {
			body, _ := io.ReadAll(r.Body)
			*gotBody = body
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_CONTROL_URL", srv.URL)
	t.Setenv("CELLHIVE_ROOT_KEY", testRootKey)
}

func TestKVKeyPutUsesTypedFlags(t *testing.T) {
	var body []byte
	scopeTestServer(t, http.MethodPost, "/v1/kv/put", map[string]string{
		"ns": "acme", "key": "k", "expiration_ttl": "60", "metadata": "eyJhIjoxfQ==",
	}, &body)
	if _, _, err := executeRoot(t, "kv", "key", "put", "acme", "KV", "k", "v", "--ttl", "60", "--metadata", `{"a":1}`); err != nil {
		t.Fatal(err)
	}
	if string(body) != "v" {
		t.Fatalf("body = %q, want v", body)
	}
}

func TestKVKeyListPrefix(t *testing.T) {
	scopeTestServer(t, http.MethodGet, "/v1/kv/list", map[string]string{"ns": "acme", "prefix": "user:"}, nil)
	if _, _, err := executeRoot(t, "kv", "key", "list", "acme", "KV", "--prefix", "user:"); err != nil {
		t.Fatal(err)
	}
}

func TestD1ExecuteCommand(t *testing.T) {
	var body []byte
	scopeTestServer(t, http.MethodPost, "/v1/d1/exec", map[string]string{"ns": "acme", "db": "DB"}, &body)
	if _, _, err := executeRoot(t, "d1", "execute", "acme", "DB", "--command", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got["sql"] != "SELECT 1" {
		t.Fatalf("sql = %#v", got["sql"])
	}
}

func TestR2ObjectPutFile(t *testing.T) {
	var body []byte
	scopeTestServer(t, http.MethodPut, "/v1/r2/object", map[string]string{
		"ns": "acme", "bucket": "BUCKET", "key": "hello.txt",
	}, &body)
	file := filepath.Join(t.TempDir(), "hello.txt")
	if err := os.WriteFile(file, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := executeRoot(t, "r2", "object", "put", "acme", "BUCKET", "hello.txt", "--file", file, "--content-type", "text/plain"); err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Fatalf("body = %q", body)
	}
}

func TestDataPlaneHelpAndArity(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"kv", "key", "put", "--help"}, "--ttl"},
		{[]string{"kv", "key", "list", "--help"}, "--prefix"},
		{[]string{"d1", "execute", "--help"}, "--command"},
		{[]string{"d1", "export", "--help"}, "--output"},
		{[]string{"d1", "migrations", "list", "--help"}, "--applied"},
		{[]string{"r2", "object", "put", "--help"}, "--content-type"},
		{[]string{"r2", "object", "list", "--help"}, "--prefix"},
	} {
		out, _, err := executeRoot(t, tc.args...)
		if err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if !strings.Contains(out, tc.want) {
			t.Errorf("%v help missing %q", tc.args, tc.want)
		}
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"kv", "key", "put"}, "usage: cellhive kv key put"},
		{[]string{"d1", "execute"}, "usage: cellhive d1 execute"},
		{[]string{"r2", "object", "put"}, "usage: cellhive r2 object put"},
	} {
		_, _, err := executeRoot(t, tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v err = %v, want %q", tc.args, err, tc.want)
		}
	}
}
