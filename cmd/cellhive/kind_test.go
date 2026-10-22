package main

import (
	"net/http"
	"strings"
	"testing"
)

func TestKVCommandTreeUsesTypedRegistryFlags(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/kv/resources", nil, map[string]string{
		"namespace": "acme", "kind": "kv", "name": "KV", "scope": "acme/__kv__/main",
	})
	if _, _, err := executeRoot(t, "kv", "namespace", "create", "acme", "KV", "--scope", "acme/__kv__/main"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodPost, "/v1/kv/resources", nil, map[string]string{
		"namespace": "acme", "kind": "kv", "name": "KV2", "scope": "acme/__kv__/KV2",
	})
	if _, _, err := executeRoot(t, "kv", "create", "acme", "KV2"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/kv/resources", map[string]string{"namespace": "acme"}, nil)
	if _, _, err := executeRoot(t, "kv", "namespace", "list", "acme"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/kv/stats", map[string]string{
		"namespace": "acme", "name": "KV", "exact": "1", "prefix": "user:",
	}, nil)
	if _, _, err := executeRoot(t, "kv", "stats", "acme", "KV", "--exact", "--prefix", "user:"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/kv/resources", map[string]string{
		"namespace": "acme", "name": "KV", "force": "1",
	}, nil)
	if _, _, err := executeRoot(t, "kv", "namespace", "rm", "acme", "KV", "--force"); err != nil {
		t.Fatal(err)
	}
}

func TestD1CommandTreeRegistryAndStats(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/d1/resources", nil, map[string]string{
		"namespace": "acme", "kind": "d1", "name": "DB", "scope": "acme/__d1__/DB",
	})
	if _, _, err := executeRoot(t, "d1", "create", "acme", "DB"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/d1/stats", map[string]string{
		"namespace": "acme", "name": "DB", "tables": "1", "exact": "1",
	}, nil)
	if _, _, err := executeRoot(t, "d1", "stats", "acme", "DB", "--tables", "--exact"); err != nil {
		t.Fatal(err)
	}

	// `info` is a compatibility alias for stats.
	adminTestServer(t, http.MethodGet, "/v1/d1/stats", map[string]string{"namespace": "acme", "name": "DB"}, nil)
	if _, _, err := executeRoot(t, "d1", "info", "acme", "DB"); err != nil {
		t.Fatal(err)
	}
}

func TestR2CommandTreeBucketAndStats(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/r2/resources", nil, map[string]string{
		"namespace": "acme", "kind": "r2", "name": "BUCKET", "scope": "acme/__r2__/BUCKET",
	})
	if _, _, err := executeRoot(t, "r2", "bucket", "create", "acme", "BUCKET"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/r2/stats", map[string]string{
		"namespace": "acme", "name": "BUCKET", "bucket": "BUCKET", "scan": "1", "limit": "5",
	}, nil)
	if _, _, err := executeRoot(t, "r2", "stats", "acme", "BUCKET", "--scan", "--limit", "5"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/r2/resources", map[string]string{
		"namespace": "acme", "name": "BUCKET",
	}, nil)
	if _, _, err := executeRoot(t, "r2", "bucket", "delete", "acme", "BUCKET"); err != nil {
		t.Fatal(err)
	}
}

func TestHyperdriveCreateUsesConnectionString(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/hyperdrive/resources", nil, map[string]string{
		"namespace": "acme", "kind": "hyperdrive", "name": "HYDR",
		"scope": "acme/__hyperdrive__/HYDR", "connection_string": "postgres://db.example/app",
	})
	if _, _, err := executeRoot(t, "hyperdrive", "create", "acme", "HYDR", "--connection-string", "postgres://db.example/app"); err != nil {
		t.Fatal(err)
	}
}

func TestKindCommandArityAndDataVerbWrappers(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"kv namespace create missing name", []string{"kv", "namespace", "create", "acme"}, "accepts 2 arg(s)"},
		{"r2 bucket list missing namespace", []string{"r2", "bucket", "list"}, "accepts 1 arg(s)"},
		{"hyperdrive create missing name", []string{"hyperdrive", "create", "acme"}, "accepts 2 arg(s)"},
		{"d1 execute missing sql", []string{"d1", "execute", "acme", "DB"}, "usage: cellhive d1 execute"},
		{"d1 migrations missing args", []string{"d1", "migrations", "list"}, "usage: cellhive d1 migrations"},
		{"kv key put missing args", []string{"kv", "key", "put"}, "usage: cellhive kv key"},
		{"r2 object put missing args", []string{"r2", "object", "put"}, "usage: cellhive r2 object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := executeRoot(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestKindHelpShowsCommandGroups(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"kv", "--help"}, []string{"namespace", "key", "bulk", "create", "stats"}},
		{[]string{"d1", "--help"}, []string{"execute", "query", "export", "migrations", "stats"}},
		{[]string{"r2", "--help"}, []string{"bucket", "object", "create", "stats"}},
		{[]string{"hyperdrive", "--help"}, []string{"create", "list", "delete", "stats"}},
	} {
		out, _, err := executeRoot(t, tc.args...)
		if err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		for _, want := range tc.want {
			if !strings.Contains(out, "\n  "+want+" ") {
				t.Errorf("%v help missing %q", tc.args, want)
			}
		}
	}
}
