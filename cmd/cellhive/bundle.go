package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/bundler"
)

// cmdBundleBuild bundles a worker entry with the platform esbuild pipeline
// (ADR-005). It is the strict-build target so dev and deploy share one bundler.
//
// Flags are parsed manually (not flag.FlagSet) so they may follow the entry
// positional, matching the Bun CLI's ergonomics (`bundle build <entry> --out f`).
func cmdBundleBuild(args []string) error {
	var (
		entry       string
		output      string
		esbuildPath string
		minify      bool
		keepNames   bool
		nodeCompat  bool
		defines     []string
		rules       []string
	)
	next := func(i *int, name string) (string, error) {
		*i++
		if *i >= len(args) {
			return "", fmt.Errorf("%s expects a value", name)
		}
		return args[*i], nil
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		var err error
		switch {
		case a == "--out":
			output, err = next(&i, a)
		case a == "--esbuild":
			esbuildPath, err = next(&i, a)
		case a == "--define":
			var v string
			if v, err = next(&i, a); err == nil {
				defines = append(defines, v)
			}
		case a == "--rule":
			var v string
			if v, err = next(&i, a); err == nil {
				rules = append(rules, v)
			}
		case a == "--minify":
			minify = true
		case a == "--keep-names":
			keepNames = true
		case a == "--nodejs-compat":
			nodeCompat = true
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("unknown flag %q", a)
		default:
			if entry != "" {
				return fmt.Errorf("unexpected argument %q", a)
			}
			entry = a
		}
		if err != nil {
			return err
		}
	}
	if entry == "" {
		return fmt.Errorf("usage: cellhive bundle build <entry> --out <file> [--minify] [--nodejs-compat] [--define K=V] [--rule type:glob]")
	}
	if output == "" {
		output = entry + ".bundle.js"
	}

	opts := bundler.Options{
		EntryPoint:   entry,
		OutFile:      output,
		Minify:       minify,
		KeepNames:    keepNames,
		NodeJSCompat: nodeCompat,
		EsbuildPath:  esbuildPath,
		Define:       map[string]string{},
	}
	for _, d := range defines {
		k, v, ok := strings.Cut(d, "=")
		if !ok {
			return fmt.Errorf("--define expects K=V, got %q", d)
		}
		opts.Define[k] = v
	}
	for _, r := range rules {
		typ, glob, ok := strings.Cut(r, ":")
		if !ok {
			return fmt.Errorf("--rule expects type:glob, got %q", r)
		}
		opts.Rules = append(opts.Rules, bundler.Rule{Type: typ, Globs: []string{glob}})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	data, err := bundler.Build(ctx, opts)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	res, err := json.Marshal(map[string]any{
		"out":  output,
		"sha":  hex.EncodeToString(sum[:]),
		"size": len(data),
	})
	if err != nil {
		return err
	}
	printJSON(res)
	return nil
}

// listFlag collects repeatable string flags.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error {
	*l = append(*l, v)
	return nil
}
