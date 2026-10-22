package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeployBuildsPayloadFromTypedFlags(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/control/deploy" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode deploy body: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true,"number":1}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")

	if _, _, err := executeRoot(t,
		"deploy", "acme", "api",
		"--bundle-sha", "abc123",
		"--kv", "KV=kv-id",
		"--cron", "*/5 * * * *",
		"--var", "A=1",
		"--dry-run",
	); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{
		"namespace": "acme", "worker": "api", "bundle_sha": "abc123", "dry_run": true,
	} {
		if got[k] != want {
			t.Errorf("payload[%s] = %v, want %v", k, got[k], want)
		}
	}
	if vars, ok := got["vars"].(map[string]any); !ok || vars["A"] != "1" {
		t.Errorf("payload vars = %#v, want A=1", got["vars"])
	}
	bindings, ok := got["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("payload bindings = %#v", got["bindings"])
	}
	binding, _ := bindings[0].(map[string]any)
	if binding["type"] != "kv" || binding["name"] != "KV" || binding["id"] != "kv-id" {
		t.Errorf("binding = %#v", binding)
	}
	crons, _ := got["crons"].([]any)
	if len(crons) != 1 || crons[0] != "*/5 * * * *" {
		t.Errorf("crons = %#v", got["crons"])
	}
}

func TestDeployRequiresBundleSource(t *testing.T) {
	t.Setenv("CELLHIVE_ADMIN_URL", "http://127.0.0.1:1")
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")
	_, _, err := executeRoot(t, "deploy", "acme", "api")
	if err == nil || !strings.Contains(err.Error(), "--bundle, --bundle-sha or --config") {
		t.Fatalf("deploy without bundle = %v", err)
	}
}

func TestPromoteAndRollbackPayloads(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		switch r.URL.Path {
		case "/v1/control/promote":
			if r.Method != http.MethodPost {
				t.Errorf("promote method = %s", r.Method)
			}
		case "/v1/control/rollback":
			if r.Method != http.MethodPost {
				t.Errorf("rollback method = %s", r.Method)
			}
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")

	if _, _, err := executeRoot(t, "promote", "acme", "api", "--version", "3"); err != nil {
		t.Fatal(err)
	}
	if got["version"] != float64(3) {
		t.Errorf("promote version = %#v", got["version"])
	}

	if _, _, err := executeRoot(t, "rollback", "acme", "api", "--idempotency-key", "rb-1"); err != nil {
		t.Fatal(err)
	}
	if got["idempotency_key"] != "rb-1" {
		t.Errorf("rollback idempotency_key = %#v", got["idempotency_key"])
	}
}

func TestDeployHelpListsTypedFlags(t *testing.T) {
	out, _, err := executeRoot(t, "deploy", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--bundle-sha", "--config", "--kv", "--consumer", "--dry-run", "--var"} {
		if !strings.Contains(out, flag) {
			t.Errorf("deploy help missing %s", flag)
		}
	}
}
