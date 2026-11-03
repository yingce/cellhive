package main

import (
	"encoding/base64"
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
	id := "acme*"
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "")
	t.Setenv("CELLHIVE_ADMIN_URL", "://no-server")
	key := make([]byte, 32)
	encoded := base64.RawURLEncoding.EncodeToString(key)
	for _, tc := range []struct {
		args []string
		typ  string
	}{
		{[]string{"--ns", "acme,acme-team*"}, "namespace"},
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
		{"--issuer", id, "--key", encoded, "--ttl", "24h1s"},
		{"--issuer", id, "--key", encoded, "--ttl", "1.5s"},
		{"--issuer", id, "--key", encoded, "--ns", "*"},
		{"--issuer", id, "--key", encoded, "--ns", "ac*"},
		{"--issuer", id, "--key", encoded, "--ns", "acme,other"},
		{"--issuer", "acme", "--key", encoded, "--ns", "acme*"},
		{"--issuer", "ac**", "--key", encoded},
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
		if _, err := mintScopeToken("vwork-42", scope[0], scope[1], "vwork-*", time.Minute, key); err == nil {
			t.Fatalf("wildcard binding %v accepted; use namespace token for all-business access", scope)
		}
	}
}

func TestJWTIssuerCredentialsOffline(t *testing.T) {
	jwtCLIEnv(t)
	t.Setenv("CELLHIVE_ADMIN_URL", "://no-server")
	for _, pattern := range []string{"acme", "acme*", "*"} {
		out, _, err := executeRoot(t, "creds", "issuer", pattern)
		if err != nil {
			t.Fatal(err)
		}
		key, err := (config.Config{RootKey: config.LoadRootKey()}).JWTKey("issuer/" + pattern)
		if err != nil || strings.TrimSpace(out) != base64.RawURLEncoding.EncodeToString(key) {
			t.Fatalf("issuer key = %q, error %v", out, err)
		}
		token, _, err := executeRoot(t, "token", "--issuer", pattern, "--key", strings.TrimSpace(out), "--ns", "acme", "--ttl", "24h")
		if err != nil {
			t.Fatal(err)
		}
		kid, claims, err := bearertoken.Verify(key, strings.TrimSpace(token), time.Now())
		if err != nil || kid != "issuer/"+pattern || claims.ExpiresAt-claims.IssuedAt != 86400 {
			t.Fatalf("offline JWT = %s %+v %v", kid, claims, err)
		}
	}
	for _, pattern := range []string{"", "a*b", "a**", "__system", "a/b", "a,b", "a b", ".."} {
		if _, _, err := executeRoot(t, "creds", "issuer", pattern); err == nil {
			t.Errorf("accepted invalid issuer %q", pattern)
		}
	}
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "")
	if _, _, err := executeRoot(t, "creds", "issuer", "acme"); err == nil {
		t.Fatal("issuer key issued without root")
	}
}

func TestJWTIssuerCRUDRemoved(t *testing.T) {
	jwtCLIEnv(t)
	t.Setenv("CELLHIVE_ADMIN_URL", "://no-server")
	for _, verb := range []string{"create", "list", "get", "disable", "delete"} {
		if _, _, err := executeRoot(t, "issuer", verb); err == nil || !strings.Contains(err.Error(), "creds issuer") {
			t.Errorf("issuer %s must direct users to local credentials, got %v", verb, err)
		}
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
	issuerToken, err := mintScopeToken("acme", "", "", "acme", time.Minute, base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
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
