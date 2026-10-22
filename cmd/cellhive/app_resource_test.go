package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// adminTestServer points the CLI at an httptest admin listener and returns the
// captured request pieces. Requests are made sequentially by these tests.
func adminTestServer(t *testing.T, wantMethod, wantPath string, wantQuery map[string]string, wantBody map[string]string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != wantMethod {
			t.Errorf("method = %s, want %s", r.Method, wantMethod)
		}
		if r.URL.Path != wantPath {
			t.Errorf("path = %s, want %s", r.URL.Path, wantPath)
		}
		for k, v := range wantQuery {
			if got := r.URL.Query().Get(k); got != v {
				t.Errorf("query %s = %q, want %q", k, got, v)
			}
		}
		if r.Body != nil && wantBody != nil {
			var got map[string]string
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode body: %v", err)
			}
			for k, v := range wantBody {
				if got[k] != v {
					t.Errorf("body %s = %q, want %q", k, got[k], v)
				}
			}
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")
}

func TestAppCommandsUseAdminAPI(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/app", nil, map[string]string{"namespace": "acme"})
	if _, _, err := executeRoot(t, "app", "create", "acme"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/apps", nil, nil)
	if _, _, err := executeRoot(t, "app", "list"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/control/app", map[string]string{"namespace": "acme"}, nil)
	if _, _, err := executeRoot(t, "app", "delete", "acme"); err != nil {
		t.Fatal(err)
	}
}

func TestResourceCommandsUseTypedFlags(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/resource", nil, map[string]string{
		"namespace": "acme", "kind": "kv", "name": "KV", "scope": "acme/__kv__/main",
	})
	if _, _, err := executeRoot(t, "resource", "create", "acme", "kv", "KV", "--scope", "acme/__kv__/main"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodPost, "/v1/control/resource", nil, map[string]string{
		"namespace": "acme", "kind": "hyperdrive", "name": "HYDR",
		"scope": "acme/__hyperdrive__/HYDR", "connection_string": "postgres://db.example/app",
	})
	if _, _, err := executeRoot(t, "resource", "create", "acme", "hyperdrive", "HYDR", "--connection-string", "postgres://db.example/app"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/resources", map[string]string{"namespace": "acme", "kind": "kv"}, nil)
	if _, _, err := executeRoot(t, "resource", "list", "acme", "--kind", "kv"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/control/resource", map[string]string{
		"namespace": "acme", "kind": "kv", "name": "KV", "force": "1",
	}, nil)
	if _, _, err := executeRoot(t, "resource", "rm", "acme", "kv", "KV", "--force"); err != nil {
		t.Fatal(err)
	}
}

func TestAppResourceArityAndFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"app create missing namespace", []string{"app", "create"}, "accepts 1 arg(s)"},
		{"app list extra", []string{"app", "list", "extra"}, "unknown command"},
		{"resource create missing name", []string{"resource", "create", "acme", "kv"}, "accepts 3 arg(s)"},
		{"resource delete missing name", []string{"resource", "delete", "acme", "kv"}, "accepts 3 arg(s)"},
		{"resource unknown flag", []string{"resource", "list", "acme", "--bogus"}, "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := executeRoot(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAppResourceHelpListsSubcommandsAndFlags(t *testing.T) {
	out, _, err := executeRoot(t, "app", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"create", "list", "delete"} {
		if !strings.Contains(out, "\n  "+sub+" ") {
			t.Errorf("app help missing %s", sub)
		}
	}

	out, _, err = executeRoot(t, "resource", "create", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--scope", "--connection-string"} {
		if !strings.Contains(out, flag) {
			t.Errorf("resource create help missing %s", flag)
		}
	}
	out, _, err = executeRoot(t, "resource", "list", "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "--kind") {
		t.Errorf("resource list help missing --kind")
	}
	out, _, err = executeRoot(t, "resource", "delete", "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "--force") {
		t.Errorf("resource delete help missing --force")
	}
}

func TestCompatShimsRouteThroughCobra(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/app", nil, map[string]string{"namespace": "acme"})
	if err := cmdApp([]string{"create", "acme"}); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodPost, "/v1/control/resource", nil, map[string]string{
		"namespace": "acme", "kind": "queue", "name": "JOBS", "scope": "acme/__queue__/JOBS",
	})
	if err := cmdResource([]string{"create", "acme", "queue", "JOBS"}); err != nil {
		t.Fatal(err)
	}
}
