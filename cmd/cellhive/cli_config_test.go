package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCLIProfileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	want := cliConfig{Profiles: map[string]cliProfile{
		"staging": {AdminURL: "https://cell.example/client/v4", AdminToken: "token"},
	}}
	if err := saveCLIConfig(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := loadCLIConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profiles["staging"] != want.Profiles["staging"] {
		t.Fatalf("profile = %+v", got.Profiles["staging"])
	}
	if mode := fileMode(path); mode&0077 != 0 {
		t.Fatalf("config mode = %o", mode)
	}
}

func TestProjectNamespaceAutoFillsWranglerDeploy(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".cellhive"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".cellhive", "project.json"), []byte(`{"profile":"staging","namespace":"acme"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte(`{"name":"web","main":"index.js","compatibility_date":"2026-09-01"}`), 0600); err != nil {
		t.Fatal(err)
	}
	old, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	got, err := translateWrangler([]string{"deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 3 || got[0] != "deploy" || got[1] != "acme" || got[2] != "-" {
		t.Fatalf("translated = %#v", got)
	}
}

func TestShellQuote(t *testing.T) {
	if got := shellQuote("A='quoted'"); got != `'A='\''quoted'\'''` {
		t.Fatalf("shell quote = %q", got)
	}
}

func TestProjectWorkerConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".cellhive"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".cellhive", "project.json"), []byte(`{"namespace":"acme","workers":{"api":"apps/api/wrangler.jsonc"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	old, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	want := filepath.Join(dir, "apps/api/wrangler.jsonc")
	if got := projectWorkerConfig("api"); got != want {
		t.Fatalf("worker config = %q want %q", got, want)
	}
}

func fileMode(path string) os.FileMode { info, _ := os.Stat(path); return info.Mode() }
