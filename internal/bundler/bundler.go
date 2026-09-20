// Package bundler builds worker bundles with esbuild (ADR-005: Go + esbuild;
// ADR-014: strict wrangler-compatible semantics). It shells out to the esbuild
// binary so the platform stays a single Go binary with no cgo or JS runtime.
package bundler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Rule maps a module rule type (wrangler "rules") to esbuild loaders for globs.
type Rule struct {
	Type  string
	Globs []string
}

// Options configures a build. OutFile is required.
type Options struct {
	EntryPoint   string
	OutFile      string
	Minify       bool
	SourceMap    bool
	KeepNames    bool
	NodeJSCompat bool
	Rules        []Rule
	Define       map[string]string
	External     []string
	// EsbuildPath overrides esbuild discovery (also CELLHIVE_ESBUILD).
	EsbuildPath string
}

// FindEsbuild locates the esbuild binary: CELLHIVE_ESBUILD, then PATH. A local
// dev package store can be added via CELLHIVE_ESBUILD_DIR (searched for
// esbuild@*/...), keeping the platform decoupled from any host layout.
func FindEsbuild() (string, error) {
	if p := os.Getenv("CELLHIVE_ESBUILD"); p != "" {
		return p, nil
	}
	if p, err := exec.LookPath("esbuild"); err == nil {
		return p, nil
	}
	if dir := os.Getenv("CELLHIVE_ESBUILD_DIR"); dir != "" {
		matches, _ := filepath.Glob(filepath.Join(dir, "esbuild@*", "node_modules", "esbuild", "bin", "esbuild"))
		if len(matches) > 0 {
			sort.Strings(matches)
			return matches[len(matches)-1], nil
		}
	}
	return "", errors.New("bundler: esbuild not found (install esbuild or set CELLHIVE_ESBUILD)")
}

func loaderFor(typ string) string {
	switch strings.ToLower(typ) {
	case "text":
		return "text"
	case "data":
		return "binary"
	case "compiledwasm":
		return "binary"
	default: // ESModule, CommonJS
		return "js"
	}
}

// extFromGlob returns the esbuild loader extension for a glob like "**/*.txt".
func extFromGlob(glob string) string {
	base := glob
	if i := strings.LastIndex(base, "*"); i >= 0 {
		base = base[i+1:]
	}
	if !strings.HasPrefix(base, ".") || strings.Contains(base, "/") {
		return ""
	}
	return base
}

// Build bundles EntryPoint to OutFile and returns the bytes.
func Build(ctx context.Context, opts Options) ([]byte, error) {
	if opts.EntryPoint == "" || opts.OutFile == "" {
		return nil, errors.New("bundler: entrypoint and outfile are required")
	}
	bin := opts.EsbuildPath
	if bin == "" {
		var err error
		if bin, err = FindEsbuild(); err != nil {
			return nil, err
		}
	}

	args := []string{
		"--bundle",
		"--format=esm",
		"--platform=neutral",
		"--target=es2022",
		"--log-level=warning",
		"--outfile=" + opts.OutFile,
		// workerd provides these at runtime: `cloudflare:*` always (DurableObject,
		// WorkerEntrypoint, env, …) and `node:*` under nodejs_compat. Without
		// them esbuild fails on unresolved imports, which broke every framework
		// prebuilt worker that imports cloudflare:workers (ADR-153).
		"--external:cloudflare:*",
	}
	if opts.NodeJSCompat {
		args = append(args, "--external:node:*")
	}
	if opts.Minify {
		args = append(args, "--minify")
	}
	if opts.SourceMap {
		args = append(args, "--sourcemap")
	}
	if opts.KeepNames {
		args = append(args, "--keep-names")
	}
	if opts.NodeJSCompat {
		args = append(args, "--conditions=workerd,worker,webworker,node")
	}
	for _, r := range opts.Rules {
		loader := loaderFor(r.Type)
		for _, g := range r.Globs {
			if ext := extFromGlob(g); ext != "" {
				args = append(args, "--loader:"+ext+"="+loader)
			}
		}
	}
	for k, v := range opts.Define {
		args = append(args, "--define:"+k+"="+v)
	}
	for _, e := range opts.External {
		args = append(args, "--external:"+e)
	}
	args = append(args, opts.EntryPoint)

	cmd := exec.CommandContext(ctx, bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("bundler: esbuild failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return os.ReadFile(opts.OutFile)
}
