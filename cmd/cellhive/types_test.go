package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCmdTypesGenerateAndCheck(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "wrangler.jsonc")
	output := filepath.Join(dir, "worker-configuration.d.ts")
	if err := os.WriteFile(config, []byte(`{"name":"demo","vars":{"TOKEN":"sample"},"kv_namespaces":[{"binding":"CACHE","id":"cache"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{output, "--config", config}
	if err := cmdTypes(args); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(output)
	if err != nil || !strings.Contains(string(got), "CACHE: KVNamespace;") {
		t.Fatalf("generated file = %q, %v", got, err)
	}
	if err := cmdTypes(append(args, "--check")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("stale\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := cmdTypes(append(args, "--check")); err == nil {
		t.Fatal("stale declaration passed --check")
	}
}
