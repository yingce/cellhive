package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoginAuthProjectEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CELLHIVE_CLI_CONFIG", filepath.Join(dir, "config.json"))
	t.Setenv("CELLHIVE_PROFILE", "staging")

	if _, _, err := executeRoot(t, "login", "--profile", "staging", "--url", "https://admin.example", "--token", "tok"); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadCLIConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profiles["staging"].AdminURL != "https://admin.example" || cfg.Profiles["staging"].AdminToken != "tok" {
		t.Fatalf("profile = %#v", cfg.Profiles["staging"])
	}

	out, _, err := executeRoot(t, "auth", "list")
	if err != nil || !strings.Contains(out, "staging") {
		t.Fatalf("auth list = %q, %v", out, err)
	}
	out, _, err = executeRoot(t, "auth", "current")
	if err != nil || strings.TrimSpace(out) != "staging" {
		t.Fatalf("auth current = %q, %v", out, err)
	}

	t.Chdir(dir)
	if _, _, err := executeRoot(t, "project", "init", "--namespace", "acme", "--profile", "staging"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ".cellhive", "project.json"))
	if err != nil {
		t.Fatal(err)
	}
	var project projectConfig
	if err := json.Unmarshal(raw, &project); err != nil {
		t.Fatal(err)
	}
	if project.Namespace != "acme" || project.Profile != "staging" {
		t.Fatalf("project = %#v", project)
	}

	out, _, err = executeRoot(t, "env")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "CLOUDFLARE_ACCOUNT_ID=acme") || !strings.Contains(out, "CLOUDFLARE_API_TOKEN=tok") {
		t.Fatalf("env output = %q", out)
	}

	if _, _, err := executeRoot(t, "auth", "logout", "staging"); err != nil {
		t.Fatal(err)
	}
	cfg, err = loadCLIConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Profiles["staging"]; ok {
		t.Fatal("logout left the profile behind")
	}
}
