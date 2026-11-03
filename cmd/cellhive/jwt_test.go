package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
)

func jwtCLIEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CELLHIVE_CLI_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	t.Setenv("CELLHIVE_TOKEN", "")
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "")
	t.Setenv("CELLHIVE_ADMIN_JWT", "ignored-oidc")
	t.Setenv("CELLHIVE_ROOT_KEY", strings.Repeat("ab", 32))
}

func TestJWTAdminRequests(t *testing.T) {
	jwtCLIEnv(t)
	key, err := (config.Config{RootKey: config.LoadRootKey()}).JWTKey("token/admin")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-cellhive-admin-token") != "" {
			t.Error("static admin header sent")
		}
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		kid, claims, err := bearertoken.Verify(key, tok, time.Now())
		if err != nil || kid != "token/admin" || claims.Subject != "CLI" || claims.ExpiresAt-claims.IssuedAt != 300 {
			t.Errorf("admin JWT: %s %+v %v", kid, claims, err)
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	if _, err := adminCall("GET", "/test", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adminCallRaw("/test", []byte("bundle")); err != nil {
		t.Fatal(err)
	}
}

func TestJWTTokenCLI(t *testing.T) {
	jwtCLIEnv(t)
	id := "00112233445566778899aabbccddeeff"
	key := make([]byte, 32)
	encoded := base64.RawURLEncoding.EncodeToString(key)
	for _, tc := range []struct {
		args []string
		typ  string
	}{
		{[]string{"--ns", "acme,team-*"}, "namespace"},
		{[]string{"--ns", "acme", "--kind", "kv", "--name", "cache"}, "binding"},
	} {
		args := append([]string{"token", "--issuer", id, "--key", encoded, "--ttl", "5m"}, tc.args...)
		out, _, err := executeRoot(t, args...)
		if err != nil {
			t.Fatal(err)
		}
		kid, c, err := bearertoken.Verify(key, strings.TrimSpace(out), time.Now())
		if err != nil || kid != "issuer/"+id || c.Type != tc.typ || c.ExpiresAt-c.IssuedAt != 300 {
			t.Fatalf("JWT = %s %+v %v (%s)", kid, c, err, out)
		}
	}
	for _, args := range [][]string{
		{"--issuer", "vwork", "--key", encoded},
		{"--issuer", id},
		{"--issuer", id, "--key", "YQ"},
		{"--issuer", id, "--key", encoded + "="},
		{"--issuer", id, "--key", encoded, "--ttl", "0s"},
		{"--issuer", id, "--key", encoded, "--kind", "kv"},
		{"--issuer", id, "--key", encoded, "--kind", "kv", "--name", "cache", "--ns", "acme*"},
	} {
		if _, _, err := executeRoot(t, append([]string{"token", "--ns", "acme"}, args...)...); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}

func TestJWTBindingMintRequiresExactResource(t *testing.T) {
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	for _, scope := range [][2]string{{"*", "CACHE"}, {"kv", "*"}, {"kv", "CACHE*"}} {
		if _, err := mintScopeToken("vwork-42", scope[0], scope[1], "00112233445566778899aabbccddeeff", time.Minute, key); err == nil {
			t.Fatalf("wildcard binding %v accepted; use namespace token for all-business access", scope)
		}
	}
}

func TestJWTIssuerCommands(t *testing.T) {
	jwtCLIEnv(t)
	id := "00112233445566778899aabbccddeeff"
	var method, path string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		body = nil
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		w.Write([]byte(`{"issuer":{"id":"` + id + `"},"key":"secret"}`))
	}))
	defer srv.Close()
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	for _, tc := range []struct {
		args         []string
		method, path string
	}{
		{[]string{"issuer", "create", "vwork", "--ns", "vwork-*,shared", "--max-token-ttl", "1h"}, "POST", "/v1/control/issuers"},
		{[]string{"issuer", "list"}, "GET", "/v1/control/issuers"},
		{[]string{"issuer", "get", id}, "GET", "/v1/control/issuers/" + id},
		{[]string{"issuer", "disable", id}, "PATCH", "/v1/control/issuers/" + id},
		{[]string{"issuer", "delete", id}, "DELETE", "/v1/control/issuers/" + id},
	} {
		if _, _, err := executeRoot(t, tc.args...); err != nil {
			t.Fatal(err)
		}
		if method != tc.method || path != tc.path {
			t.Fatalf("request = %s %s", method, path)
		}
		if method == "POST" && (body["name"] != "vwork" || body["max_token_ttl_seconds"] != float64(3600) || len(body["namespaces"].([]any)) != 2) {
			t.Fatalf("create body = %#v", body)
		}
		if method == "PATCH" && body["enabled"] != false {
			t.Fatalf("disable body = %#v", body)
		}
	}
	if _, _, err := executeRoot(t, "creds", "issuer", "vwork"); err == nil {
		t.Fatal("unregistered name-derived issuer allowed")
	}
}

func TestJWTDeployBundleNamespace(t *testing.T) {
	jwtCLIEnv(t)
	seen := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/control/bundle" {
			seen = true
			if r.URL.Query().Get("namespace") != "acme" {
				t.Errorf("bundle missing namespace: %s", r.URL)
			}
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	path := filepath.Join(t.TempDir(), "worker.js")
	if err := os.WriteFile(path, []byte("export default {}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := deployCore("acme", "worker", deployOptions{bundleFile: path}); err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("bundle not uploaded")
	}
}

func TestJWTExplicitCredentials(t *testing.T) {
	jwtCLIEnv(t)
	rootToken, err := adminToken()
	if err != nil {
		t.Fatal(err)
	}
	issuerToken, err := mintScopeToken("acme", "", "", "00112233445566778899aabbccddeeff", time.Minute, base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	cfg := cliConfig{Profiles: map[string]cliProfile{activeProfileName(): {Token: issuerToken, AdminToken: "obsolete-static-secret"}}}
	if err := saveCLIConfig(cliConfigPath(), cfg); err != nil {
		t.Fatal(err)
	}
	if got, err := adminToken(); err != nil || got != issuerToken {
		t.Fatalf("profile token precedence = %q %v", got, err)
	}
	t.Setenv("CELLHIVE_TOKEN", rootToken)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "obsolete-static-secret")
	if got, err := adminToken(); err != nil || got != rootToken {
		t.Fatalf("env token precedence = %q %v", got, err)
	}
	t.Setenv("CELLHIVE_TOKEN", "static-key-is-not-bearer")
	if _, err := adminToken(); err == nil {
		t.Fatal("static key accepted as JWT")
	}
	if _, _, err := executeRoot(t, "login", "--url", "https://example.test", "--jwt", rootToken); err == nil {
		t.Fatal("OIDC login flag accepted")
	}
}

func TestJWTEnvRootBootstrap(t *testing.T) {
	jwtCLIEnv(t)
	t.Chdir(t.TempDir())
	if err := saveCLIConfig(cliConfigPath(), cliConfig{Profiles: map[string]cliProfile{"default": {AdminURL: "https://admin.example"}}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CELLHIVE_PROFILE", "default")
	if _, _, err := executeRoot(t, "project", "init", "--namespace", "acme"); err != nil {
		t.Fatal(err)
	}
	out, _, err := executeRoot(t, "env")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "export 'CLOUDFLARE_API_TOKEN=") {
			continue
		}
		token := strings.TrimSuffix(strings.TrimPrefix(line, "export 'CLOUDFLARE_API_TOKEN="), "'")
		key, _ := (config.Config{RootKey: config.LoadRootKey()}).JWTKey("token/admin")
		if _, _, err := bearertoken.Verify(key, token, time.Now()); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("Wrangler JWT missing")
}

func TestJWTCredsScopePrintsSigningKey(t *testing.T) {
	jwtCLIEnv(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	original := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = original }()
	err = credsPrint("scope")
	w.Close()
	os.Stdout = original
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	key, err := (config.Config{RootKey: config.LoadRootKey()}).JWTKey("token/scope")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(out)) != base64.RawURLEncoding.EncodeToString(key) {
		t.Fatal("scope credential is not the JWT signing key")
	}
}
