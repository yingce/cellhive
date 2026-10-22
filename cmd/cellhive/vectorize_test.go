package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVectorizeCreateUsesTypedFlags(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/vectorize/resources" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")

	if _, _, err := executeRoot(t, "vectorize", "create", "acme", "IDX", "--dimensions", "3", "--metric", "cosine"); err != nil {
		t.Fatal(err)
	}
	cfg, _ := got["config"].(map[string]any)
	if cfg["dimensions"] != float64(3) || cfg["metric"] != "cosine" {
		t.Fatalf("config = %#v", got["config"])
	}
	if got["namespace"] != "acme" || got["name"] != "IDX" || got["kind"] != "vectorize" {
		t.Fatalf("payload = %#v", got)
	}
}

func TestVectorizeQueryUsesTypedFlags(t *testing.T) {
	var body []byte
	scopeTestServer(t, http.MethodPost, "/v1/vectorize/query", map[string]string{
		"ns": "acme", "index": "IDX",
	}, &body)
	if _, _, err := executeRoot(t, "vectorize", "query", "acme", "IDX", "--vector", "1,2,3", "--top-k", "2", "--return-values"); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode query body: %v", err)
	}
	if got["topK"] != float64(2) || got["returnValues"] != true {
		t.Fatalf("query body = %#v", got)
	}
	vec, _ := got["vector"].([]any)
	if len(vec) != 3 {
		t.Fatalf("vector = %#v", got["vector"])
	}
}

func TestVectorizeHelpAndArity(t *testing.T) {
	out, _, err := executeRoot(t, "vectorize", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"create", "insert", "query", "rebuild", "create-metadata-index"} {
		if !strings.Contains(out, "\n  "+want+" ") {
			t.Errorf("vectorize help missing %q", want)
		}
	}
	out, _, err = executeRoot(t, "vectorize", "query", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--vector", "--vector-id", "--top-k", "--return-metadata", "--filter"} {
		if !strings.Contains(out, flag) {
			t.Errorf("vectorize query help missing %s", flag)
		}
	}
	for _, args := range [][]string{
		{"vectorize", "create"},
		{"vectorize", "query", "acme"},
		{"vectorize", "get-vectors", "acme", "IDX"},
	} {
		if _, _, err := executeRoot(t, args...); err == nil {
			t.Errorf("%v unexpectedly succeeded", args)
		}
	}
}
