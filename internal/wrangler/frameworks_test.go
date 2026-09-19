package wrangler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bundler"
)

// Framework prebuilt-output acceptance (ADR-153): the shapes Next/OpenNext,
// SvelteKit (adapter-cloudflare) and Astro (adapter-cloudflare) produce must map
// through the wrangler config loader and bundle with the platform bundler. This
// is the per-framework parity check the compatibility matrix requires, run
// offline (no framework toolchain).

type frameworkCase struct {
	name       string
	main       string
	assetsDir  string
	extraJSON  string
	wantFlags  []string
	entry      string
	assetsFile string
}

func TestFrameworkPrebuiltLayouts(t *testing.T) {
	cases := []frameworkCase{
		{
			name:      "opennext",
			main:      ".open-next/worker.js",
			assetsDir: ".open-next/assets",
			extraJSON: `"compatibility_flags": ["nodejs_compat"],`,
			wantFlags: []string{"nodejs_compat"},
			entry: `import { DurableObject } from "cloudflare:workers";
export class DO extends DurableObject {}
export default { async fetch(req, env) { return new Response("opennext"); } };
`,
			assetsFile: "index.html",
		},
		{
			name:       "sveltekit",
			main:       ".svelte-kit/cloudflare/_worker.js",
			assetsDir:  ".svelte-kit/cloudflare",
			extraJSON:  `"compatibility_flags": ["nodejs_compat"],`,
			wantFlags:  []string{"nodejs_compat"},
			entry:      `export default { async fetch() { return new Response("sveltekit"); } };`,
			assetsFile: "_app/immutable/entry.js",
		},
		{
			name:       "astro",
			main:       "dist/_worker.js/index.js",
			assetsDir:  "dist/client",
			extraJSON:  `"compatibility_flags": ["nodejs_compat"],`,
			wantFlags:  []string{"nodejs_compat"},
			entry:      `export default { async fetch() { return new Response("astro"); } };`,
			assetsFile: "_astro/index.css",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(rel, body string) {
				p := filepath.Join(dir, rel)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			write(tc.main, tc.entry)
			write(filepath.Join(tc.assetsDir, tc.assetsFile), "asset")
			cfg := `{"name": "` + tc.name + `-app", "main": "` + tc.main + `", ` + tc.extraJSON + `
  "compatibility_date": "2026-05-01",
  "assets": {"directory": "` + tc.assetsDir + `"}}`
			write("wrangler.jsonc", cfg)

			m, err := Load(filepath.Join(dir, "wrangler.jsonc"), "")
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if m.Worker != tc.name+"-app" {
				t.Fatalf("worker = %q", m.Worker)
			}
			// Load resolves main/assets relative to the config directory.
			if m.Main != filepath.Join(dir, tc.main) {
				t.Fatalf("main = %q, want %q", m.Main, filepath.Join(dir, tc.main))
			}
			if m.AssetsDir != filepath.Join(dir, tc.assetsDir) {
				t.Fatalf("assets dir = %q, want %q", m.AssetsDir, filepath.Join(dir, tc.assetsDir))
			}
			if strings.Join(m.CompatibilityFlags, ",") != strings.Join(tc.wantFlags, ",") {
				t.Fatalf("flags = %v, want %v", m.CompatibilityFlags, tc.wantFlags)
			}

			// The entry bundles with the platform bundler (skip if esbuild is
			// unavailable; CI installs it).
			if _, err := bundler.FindEsbuild(); err != nil {
				t.Skipf("esbuild unavailable: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			out := filepath.Join(dir, "out.js")
			b, err := bundler.Build(ctx, bundler.Options{
				EntryPoint:   m.Main,
				OutFile:      out,
				NodeJSCompat: true,
			})
			if err != nil {
				t.Fatalf("bundle %s: %v", tc.main, err)
			}
			if len(b) == 0 {
				t.Fatalf("empty bundle for %s", tc.main)
			}
		})
	}
}
