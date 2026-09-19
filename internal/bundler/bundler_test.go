package bundler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildBundlesModulesAndTextRule(t *testing.T) {
	if _, err := FindEsbuild(); err != nil {
		t.Skipf("esbuild unavailable: %v", err)
	}
	dir := t.TempDir()
	must := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	must("msg.js", `export const msg = "bundled-ok";`)
	must("note.txt", "hello-text")
	must("entry.js", `import { msg } from "./msg.js";
import note from "./note.txt";
export default { fetch() { return new Response(msg + ":" + note); } };`)

	out := filepath.Join(dir, "out.js")
	data, err := Build(context.Background(), Options{
		EntryPoint: filepath.Join(dir, "entry.js"),
		OutFile:    out,
		Rules:      []Rule{{Type: "Text", Globs: []string{"**/*.txt"}}},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, "bundled-ok") {
		t.Fatalf("output missing inlined module: %s", s)
	}
	if !strings.Contains(s, "hello-text") {
		t.Fatalf("output missing text loader content: %s", s)
	}
}

func TestExtFromGlob(t *testing.T) {
	cases := map[string]string{
		"**/*.txt":   ".txt",
		"**/*.wasm":  ".wasm",
		"**/*.bin":   ".bin",
		"**/*":       "",
		"*.json":     ".json",
		"a/b/*.html": ".html",
	}
	for glob, want := range cases {
		if got := extFromGlob(glob); got != want {
			t.Fatalf("extFromGlob(%q) = %q, want %q", glob, got, want)
		}
	}
}

func TestBuildRejectsMissingInputs(t *testing.T) {
	if _, err := Build(context.Background(), Options{OutFile: "x"}); err == nil {
		t.Fatal("expected error for missing entrypoint")
	}
}

// TestBuildExternalizesPlatformModules: cloudflare:* is always external, and
// node:* is external under nodejs_compat, so framework prebuilt workers (which
// import them) bundle instead of failing on unresolved imports (ADR-153).
func TestBuildExternalizesPlatformModules(t *testing.T) {
	if _, err := FindEsbuild(); err != nil {
		t.Skipf("esbuild unavailable: %v", err)
	}
	dir := t.TempDir()
	entry := filepath.Join(dir, "worker.js")
	src := `import { DurableObject } from "cloudflare:workers";
import { Buffer } from "node:buffer";
export class R extends DurableObject {}
export default { fetch() { return new Response(typeof Buffer + R.name); } };
`
	if err := os.WriteFile(entry, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.js")
	b, err := Build(context.Background(), Options{EntryPoint: entry, OutFile: out, NodeJSCompat: true})
	if err != nil {
		t.Fatalf("build with platform imports: %v", err)
	}
	if !strings.Contains(string(b), "cloudflare:workers") || !strings.Contains(string(b), "node:buffer") {
		t.Fatalf("platform imports were not preserved as external:\n%s", b)
	}
	// Without nodejs_compat, node:* stays unresolved (fails closed).
	if _, err := Build(context.Background(), Options{EntryPoint: entry, OutFile: out}); err == nil {
		t.Fatal("node: import bundled without nodejs_compat")
	}
}
