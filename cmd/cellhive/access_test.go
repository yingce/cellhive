package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDomainCommandsUseAdminAPI(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/domain", nil, map[string]string{
		"namespace": "acme", "host": "app.test",
	})
	if _, _, err := executeRoot(t, "domain", "add", "acme", "app.test"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/domains", map[string]string{"namespace": "acme"}, nil)
	if _, _, err := executeRoot(t, "domain", "ls", "acme"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/control/domain", map[string]string{
		"namespace": "acme", "host": "app.test",
	}, nil)
	if _, _, err := executeRoot(t, "domain", "rm", "acme", "app.test"); err != nil {
		t.Fatal(err)
	}
}

func TestRouteAddSendsPath(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/route", nil, map[string]string{
		"namespace": "acme", "host": "app.test", "worker": "api", "path": "/api",
	})
	if _, _, err := executeRoot(t, "route", "add", "acme", "app.test", "api", "--path", "/api"); err != nil {
		t.Fatal(err)
	}
}

func TestSecretCommandsUseAdminAPI(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/secret", nil, map[string]string{
		"namespace": "acme", "worker": "api", "key": "TOKEN", "value": "djE=",
	})
	if _, _, err := executeRoot(t, "secret", "put", "acme", "api", "TOKEN", "--value", "djE="); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/secret", map[string]string{
		"namespace": "acme", "worker": "api", "key": "TOKEN",
	}, nil)
	if _, _, err := executeRoot(t, "secret", "get", "acme", "api", "TOKEN"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/secrets", map[string]string{
		"namespace": "acme", "worker": "api",
	}, nil)
	if _, _, err := executeRoot(t, "secret", "list", "acme", "api"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/control/secret", map[string]string{
		"namespace": "acme", "worker": "api", "key": "TOKEN",
	}, nil)
	if _, _, err := executeRoot(t, "secret", "delete", "acme", "api", "TOKEN"); err != nil {
		t.Fatal(err)
	}
}

func TestSecretBulkJSON(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if r.URL.Path != "/client/v4/accounts/acme/workers/scripts/api/secrets-bulk" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")

	file := filepath.Join(t.TempDir(), "secrets.json")
	if err := os.WriteFile(file, []byte(`{"A":"1","B":null}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := executeRoot(t, "secret", "bulk", "acme", "api", file); err != nil {
		t.Fatal(err)
	}
	secrets, _ := got["secrets"].(map[string]any)
	a, _ := secrets["A"].(map[string]any)
	if a["text"] != "1" || a["type"] != "secret_text" {
		t.Errorf("secret A = %#v", secrets["A"])
	}
	if secrets["B"] != nil {
		t.Errorf("secret B = %#v, want null", secrets["B"])
	}
}

func TestServiceACLCommands(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/service-acl", nil, map[string]string{
		"namespace": "acme", "worker": "api", "caller_namespace": "caller",
	})
	if _, _, err := executeRoot(t, "service-acl", "add", "acme", "api", "caller"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodGet, "/v1/control/service-acls", map[string]string{"namespace": "acme"}, nil)
	if _, _, err := executeRoot(t, "service-acl", "ls", "acme"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodDelete, "/v1/control/service-acl", map[string]string{
		"namespace": "acme", "worker": "api", "caller_namespace": "caller",
	}, nil)
	if _, _, err := executeRoot(t, "service-acl", "rm", "acme", "api", "caller"); err != nil {
		t.Fatal(err)
	}
}

func TestAccessCommandArityAndHelp(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"domain add missing host", []string{"domain", "add", "acme"}, "accepts 2 arg(s)"},
		{"route add missing worker", []string{"route", "add", "acme", "app.test"}, "accepts 3 arg(s)"},
		{"secret put missing key", []string{"secret", "put", "acme", "api"}, "accepts 3 arg(s)"},
		{"service-acl add missing caller", []string{"service-acl", "add", "acme", "api"}, "accepts 3 arg(s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := executeRoot(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"domain", "--help"}, []string{"add", "ls", "rm"}},
		{[]string{"route", "--help"}, []string{"add", "rm"}},
		{[]string{"secret", "--help"}, []string{"put", "get", "delete", "list", "bulk"}},
		{[]string{"service-acl", "--help"}, []string{"add", "ls", "rm"}},
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
