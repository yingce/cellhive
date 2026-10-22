package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func cmdWrangler(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive wrangler <deploy|delete|versions|deployments|secret|tail|rollback|promote|d1|kv|r2|vectorize|hyperdrive> ...")
	}
	mapped, err := translateWrangler(args)
	if err != nil {
		return err
	}
	return dispatch(mapped)
}

// wranglerDeployUnsupported maps wrangler `deploy` flags we do not accept to the
// supported equivalent, so a migrating user gets an actionable error instead of
// a generic "flag provided but not defined" (ADR-138).
var wranglerDeployUnsupported = map[string]string{
	"--minify":              "set build.minify in wrangler.jsonc",
	"--no-bundle":           "set no_bundle in wrangler.jsonc",
	"--outdir":              "not needed: CellHive builds and stores the bundle",
	"--outfile":             "not needed: CellHive builds and stores the bundle",
	"--metafile":            "not supported",
	"--keep-vars":           "not supported",
	"--tag":                 "use `cellhive promote --version`",
	"--compatibility-date":  "set compatibility_date in wrangler.jsonc",
	"--compatibility-flags": "set compatibility_flags in wrangler.jsonc",
	"--upload-source-maps":  "not supported",
	"--legacy-env":          "not supported",
	"--alias":               "not supported",
	"--dispatch-namespace":  "not supported",
	"--assets":              "set assets in wrangler.jsonc",
	"--site":                "set assets.directory in wrangler.jsonc",
}

// wranglerReject maps every known-but-unsupported wrangler subcommand to an
// actionable alternative, so migrating users never see a bare "unknown
// command" (ADR-138/148). Commands not listed here are either mapped by
// translateWrangler or genuinely unknown.
var wranglerReject = map[string]string{
	"dev":              "use `cellhive dev` (Bun CLI, cli/) for local development; it reads the same wrangler config",
	"pages":            "Cloudflare Pages is not supported; deploy a Worker with `assets` instead (docs/wrangler-compat.md)",
	"dispatch":         "dispatch namespaces are not supported (rejected at deploy)",
	"login":            "use `cellhive login --profile <name> --url <admin-url> --token <token>`",
	"logout":           "CellHive has no Cloudflare account; nothing to log out",
	"whoami":           "CellHive has no Cloudflare account; use `cellhive creds` / `cellhive status`",
	"containers":       "Containers are not supported (docs/compatibility-matrix.md)",
	"pubsub":           "Cloudflare Pub/Sub is not supported",
	"mtls-certificate": "mTLS certificates are not supported; internal traffic uses derived secrets (docs/security.md)",
	"cert":             "edge TLS is operator-managed (Traefik); CellHive does not manage certificates",
	"check":            "use `cellhive deploy --dry-run` for a server-side preflight of the config",
	"docs":             "see the docs/ directory in the CellHive repository",
	"telemetry":        "CellHive sends no usage telemetry; nothing to configure",
}

// translateWrangler converts a wrangler-style invocation into native cellhive
// arguments.
func translateWrangler(args []string) ([]string, error) {
	sub, rest := args[0], args[1:]
	if msg, ok := wranglerReject[sub]; ok {
		return nil, fmt.Errorf("wrangler %s is not supported: %s", sub, msg)
	}
	if strings.HasPrefix(sub, "unstable_") {
		return nil, fmt.Errorf("wrangler %s is not supported: unstable/internal wrangler command", sub)
	}
	switch sub {
	case "deploy":
		return translateWranglerDeploy(rest)
	case "delete":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: cellhive wrangler delete <namespace> <worker>")
		}
		return append([]string{"worker", "delete"}, rest...), nil
	case "secret":
		return append([]string{"secret"}, rest...), nil
	case "tail":
		return append([]string{"tail"}, rest...), nil
	case "rollback":
		return append([]string{"rollback"}, rest...), nil
	case "promote":
		return append([]string{"promote"}, rest...), nil
	case "versions", "deployments", "triggers", "workflows", "queues", "types", "init":
		// The compat group maps these onto native commands or returns a
		// structured rejection with the alternative (ADR-148).
		return append([]string{sub}, rest...), nil
	case "d1", "r2":
		// Both registry and supported data-plane verbs dispatch to the native CLI.
		return append([]string{sub}, rest...), nil
	case "kv", "kv:key", "kv:namespace":
		return append([]string{"kv"}, rest...), nil
	case "kv:bulk":
		return append([]string{"kv", "bulk"}, rest...), nil
	case "vectorize", "hyperdrive":
		return append([]string{sub}, rest...), nil
	default:
		return nil, fmt.Errorf("unknown wrangler command %q; supported: deploy, delete, secret, tail, rollback, promote, versions, deployments, triggers, workflows, queues, types, init, d1, r2, kv, vectorize, hyperdrive", sub)
	}
}

func translateWranglerDeploy(rest []string) ([]string, error) {
	var ns, name, configPath, envName string
	var out []string
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		val := ""
		hasVal := false
		if strings.HasPrefix(a, "--") {
			if k, v, ok := strings.Cut(a, "="); ok {
				a, val, hasVal = k, v, true
			}
		}
		if hint, bad := wranglerDeployUnsupported[a]; bad {
			return nil, fmt.Errorf("wrangler deploy %s: %s", a, hint)
		}
		if a == "--dry-run" || a == "--var" || a == "--secrets-file" {
			// Supported natively now (ADR-148): pass through with its value.
			if a == "--dry-run" {
				out = append(out, a)
				continue
			}
			if !hasVal {
				i++
				if i >= len(rest) {
					return nil, fmt.Errorf("wrangler deploy %s needs a value", a)
				}
				val = rest[i]
			}
			out = append(out, a, val)
			continue
		}
		switch a {
		case "-c", "--config", "-e", "--env", "-n", "--name", "--namespace":
			if !hasVal {
				i++
				if i >= len(rest) {
					return nil, fmt.Errorf("wrangler deploy %s needs a value", a)
				}
				val = rest[i]
			}
			switch a {
			case "-c", "--config":
				configPath = val
			case "-e", "--env":
				envName = val
			case "-n", "--name":
				name = val
			case "--namespace":
				ns = val
			}
		default:
			out = append(out, rest[i])
		}
	}
	if ns == "" {
		ns = projectNamespace()
	}
	if ns == "" {
		return nil, fmt.Errorf("wrangler deploy: pass --namespace <ns> or run `cellhive project init --namespace <ns>`")
	}
	if configPath == "" {
		if name != "" {
			configPath = projectWorkerConfig(name)
		}
	}
	if configPath == "" {
		if found := discoverWranglerConfig("."); found != "" {
			configPath = found
		} else if !hasBundleFlag(out) {
			return nil, fmt.Errorf("wrangler deploy: no wrangler.jsonc/json in the current directory; pass -c/--config <file> or --bundle/--bundle-sha")
		}
	}
	if name == "" {
		name = "-" // let the config's `name` decide
	}
	mapped := append([]string{"deploy", ns, name}, out...)
	if configPath != "" {
		mapped = append(mapped, "--config", configPath)
	}
	if envName != "" {
		mapped = append(mapped, "--env", envName)
	}
	return mapped, nil
}

func hasBundleFlag(args []string) bool {
	for _, a := range args {
		if a == "--bundle" || a == "--bundle-sha" || strings.HasPrefix(a, "--bundle=") || strings.HasPrefix(a, "--bundle-sha=") {
			return true
		}
	}
	return false
}

// discoverWranglerConfig finds a wrangler config in dir (jsonc preferred).
func discoverWranglerConfig(dir string) string {
	for _, n := range []string{"wrangler.jsonc", "wrangler.json"} {
		p := filepath.Join(dir, n)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}
