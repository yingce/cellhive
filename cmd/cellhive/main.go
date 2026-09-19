// Command cellhive is the CellHive CLI: control-plane operations (app/resource/
// deploy/promote/rollback/route/secret) against the admin listener, plus a
// data-plane diagnose and the routing-projection pull.
package main

import (
	"bufio"
	"bytes"
	"cellhive/internal/scopedtoken"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bundler"
	"cellhive/internal/config"
	"cellhive/internal/wrangler"
)

const version = "0.0.1-p1"

func main() {
	for _, w := range config.LegacyEnvWarnings() {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// dispatch runs a top-level command. It is separate from main so that the
// `wrangler` namespace can translate its arguments and re-enter it (ADR-138).
func dispatch(args []string) error {
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Println("cellhive", version)
		return nil
	case "diagnose":
		return diagnose()
	case "app":
		return cmdApp(args[1:])
	case "resource":
		return cmdResource(args[1:])
	case "domain":
		return cmdDomain(args[1:])
	case "deploy":
		return cmdDeploy(args[1:])
	case "promote":
		return cmdPromote(args[1:])
	case "rollback":
		return cmdRollback(args[1:])
	case "route":
		return cmdRoute(args[1:])
	case "secret":
		return cmdSecret(args[1:])
	case "bundle":
		return cmdBundle(args[1:])
	case "asset":
		return cmdAsset(args[1:])
	case "worker":
		return cmdWorker(args[1:])
	case "workflow":
		return cmdWorkflow(args[1:])
	case "releases":
		return cmdReleases(args[1:])
	case "tail":
		return cmdTail(args[1:])
	case "creds":
		return cmdCreds(args[1:])
	case "status":
		return cmdStatus()
	case "capacity":
		return cmdCapacity()
	case "gc":
		return cmdGC(args[1:])
	case "routes":
		return cmdRoutes()
	case "audit":
		return cmdAudit(args[1:])
	case "service-acl":
		return cmdServiceACL(args[1:])
	case "queue":
		return cmdQueue(args[1:])
	case "kv":
		return cmdKind("kv", args[1:])
	case "d1":
		return cmdKind("d1", args[1:])
	case "r2":
		return cmdKind("r2", args[1:])
	case "hyperdrive":
		return cmdKind("hyperdrive", args[1:])
	case "vectorize":
		return cmdVectorize(args[1:])
	case "versions", "deployments", "triggers", "workflows", "queues", "types", "init":
		return cmdCompat(args)
	case "wrangler":
		return cmdWrangler(args[1:])
	case "help", "--help", "-h":
		usage()
		return nil
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		usage()
		os.Exit(2)
		return nil
	}
}

// cmdWrangler runs a wrangler-style command against CellHive: translateWrangler
// maps the subcommand and common flags onto the native CLI, then dispatch runs
// it (ADR-138).
func cmdWrangler(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive wrangler <deploy|delete|versions list|deployments list|secret|tail|rollback|promote> ...")
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
	"login":            "CellHive has no Cloudflare account; use `cellhive creds` for platform credentials",
	"logout":           "CellHive has no Cloudflare account; nothing to log out",
	"whoami":           "CellHive has no Cloudflare account; use `cellhive creds` / `cellhive status`",
	"containers":       "Containers are not supported (docs/compatibility-matrix.md)",
	"pubsub":           "Cloudflare Pub/Sub is not supported",
	"mtls-certificate": "mTLS certificates are not supported; internal traffic uses derived secrets (docs/security.md)",
	"cert":             "edge TLS is operator-managed (Traefik); CellHive does not manage certificates",
	"check":            "use `cellhive deploy --dry-run` for a server-side preflight of the config",
	"docs":             "see the docs/ directory in the CellHive repository",
	"telemetry":        "CellHive sends no usage telemetry; nothing to configure",
	"kv:bulk":          "bulk KV upload is a data-plane op; use a Worker with a KV binding or `cellhive dev`",
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
		// Real per-domain commands (ADR-157): registry verbs work, data-plane
		// verbs get a structured rejection from cmdKind (ADR-148).
		return append([]string{sub}, rest...), nil
	case "kv", "kv:key", "kv:namespace":
		return append([]string{"kv"}, rest...), nil
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
		return nil, fmt.Errorf("wrangler deploy: pass --namespace <ns> (CellHive has no account concept; the namespace is the tenant)")
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

func adminURL() string { return getenv("CELLHIVE_ADMIN_URL", "http://127.0.0.1:8082") }

// adminToken prefers the explicit CELLHIVE_ADMIN_TOKEN (rotatable separately)
// and otherwise uses the root-derived credential (ADR-137).
func adminToken() string {
	if v := getenv("CELLHIVE_ADMIN_TOKEN", ""); v != "" {
		return v
	}
	return config.DeriveCredentials(config.LoadRootKey()).Admin
}

func internalURL() string { return getenv("CELLHIVE_CONTROL_URL", "http://127.0.0.1:7001") }

func internalToken() string { return config.DeriveCredentials(config.LoadRootKey()).Internal }

var httpClient = &http.Client{Timeout: 15 * time.Second}

func adminCall(method, path string, body any) (json.RawMessage, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, adminURL()+path, rdr)
	if err != nil {
		return nil, err
	}
	if jwt := strings.TrimSpace(os.Getenv("CELLHIVE_ADMIN_JWT")); jwt != "" {
		// Namespace-scoped OIDC/JWT credential (ADR-131).
		req.Header.Set("Authorization", "Bearer "+jwt)
	} else {
		req.Header.Set("x-cellhive-admin-token", adminToken())
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	return doJSON(req)
}

// cmdDomain manages custom domains (ADR-131/133): a registered domain routes
// immediately; rm removes the domain and its routes.
func cmdDomain(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive domain add <namespace> <host> | domain ls <namespace> | domain rm <namespace> <host>")
	}
	if (args[0] == "add" || args[0] == "rm") && len(args) < 3 {
		return fmt.Errorf("usage: cellhive domain %s <namespace> <host>", args[0])
	}
	switch args[0] {
	case "add":
		data, err := adminCall(http.MethodPost, "/v1/control/domain", map[string]string{
			"namespace": args[1], "host": args[2],
		})
		if err != nil {
			return err
		}
		printJSON(data)
	case "ls", "list":
		q := url.Values{"namespace": {args[1]}}
		data, err := adminCall(http.MethodGet, "/v1/control/domains?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "rm":
		q := url.Values{"namespace": {args[1]}, "host": {args[2]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/domain?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	default:
		return fmt.Errorf("usage: cellhive domain add|ls|rm ...")
	}
	return nil
}

func internalGet(path string) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodGet, internalURL()+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-internal-token", internalToken())
	return doJSON(req)
}

// adminCallRaw POSTs raw bytes to an admin endpoint (bundle/asset upload).
func adminCallRaw(path string, data []byte) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodPost, adminURL()+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-admin-token", adminToken())
	req.Header.Set("content-type", "application/octet-stream")
	return doJSON(req)
}

func doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(data))
	}
	return data, nil
}

func printJSON(data json.RawMessage) {
	var v any
	if json.Unmarshal(data, &v) == nil {
		out, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(out))
		return
	}
	fmt.Println(string(data))
}

func cmdApp(args []string) error {
	if len(args) == 2 && args[0] == "delete" {
		q := url.Values{"namespace": {args[1]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/app?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	if len(args) == 1 && args[0] == "list" {
		data, err := adminCall(http.MethodGet, "/v1/control/apps", nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	if len(args) < 2 || args[0] != "create" {
		return fmt.Errorf("usage: cellhive app create <namespace> | cellhive app list")
	}
	data, err := adminCall(http.MethodPost, "/v1/control/app", map[string]string{"namespace": args[1]})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func cmdResource(args []string) error {
	if len(args) >= 1 && (args[0] == "delete" || args[0] == "rm" || args[0] == "revoke") {
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive resource delete <namespace> <kind> <name> [--force]")
		}
		fs := flag.NewFlagSet("resource delete", flag.ContinueOnError)
		force := fs.Bool("force", false, "revoke even while worker versions still bind the resource")
		if err := fs.Parse(args[4:]); err != nil {
			return err
		}
		q := url.Values{"namespace": {args[1]}, "kind": {args[2]}, "name": {args[3]}}
		if *force {
			q.Set("force", "1")
		}
		data, err := adminCall(http.MethodDelete, "/v1/control/resource?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	if len(args) >= 1 && args[0] == "list" {
		if len(args) < 2 {
			return fmt.Errorf("usage: cellhive resource list <namespace> [--kind <kind>]")
		}
		fs := flag.NewFlagSet("resource list", flag.ContinueOnError)
		kind := fs.String("kind", "", "filter by kind")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		q := url.Values{"namespace": {args[1]}}
		if *kind != "" {
			q.Set("kind", *kind)
		}
		data, err := adminCall(http.MethodGet, "/v1/control/resources?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	if len(args) < 4 || args[0] != "create" {
		return fmt.Errorf("usage: cellhive resource create <namespace> <kind> <name> [--scope <scope>] [--connection-string <url>]")
	}
	fs := flag.NewFlagSet("resource create", flag.ContinueOnError)
	scope := fs.String("scope", "", "resource cell scope (default <ns>/__<kind>__/<name>)")
	conn := fs.String("connection-string", "", "hyperdrive origin URL (sealed server-side; kind=hyperdrive only)")
	if err := fs.Parse(args[4:]); err != nil {
		return err
	}
	ns, kind, name := args[1], args[2], args[3]
	sc := *scope
	if sc == "" {
		sc = fmt.Sprintf("%s/__%s__/%s", ns, kind, name)
	}
	payload := map[string]string{
		"namespace": ns, "kind": kind, "name": name, "scope": sc,
	}
	if *conn != "" {
		payload["connection_string"] = *conn
	}
	data, err := adminCall(http.MethodPost, "/v1/control/resource", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdWorker manages workers (delete).
func cmdWorker(args []string) error {
	if len(args) == 3 && args[0] == "delete" {
		q := url.Values{"namespace": {args[1]}, "worker": {args[2]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/worker?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	return fmt.Errorf("usage: cellhive worker delete <namespace> <worker>")
}

// cmdWorkflow creates a workflow definition resource (kind "workflow"). The
// worker's `workflow` binding then references it (docs/control-plane.md).
func cmdWorkflow(args []string) error {
	if len(args) < 3 || args[0] != "create" {
		return fmt.Errorf("usage: cellhive workflow create <namespace> <name>")
	}
	ns, name := args[1], args[2]
	data, err := adminCall(http.MethodPost, "/v1/control/resource", map[string]string{
		"namespace": ns, "kind": "workflow", "name": name,
		"scope": fmt.Sprintf("%s/__workflow__/%s", ns, name),
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdStatus prints the control-plane overview.
func cmdStatus() error {
	data, err := adminCall(http.MethodGet, "/v1/control/status", nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdCapacity prints the autoscaler's capacity recommendation.
func cmdCapacity() error {
	data, err := adminCall(http.MethodGet, "/v1/control/capacity", nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdGC runs operator garbage collection over the object store: "bundles" or
// "assets" (ADR-110/ADR-111).
func cmdGC(args []string) error {
	if len(args) < 1 || (args[0] != "bundles" && args[0] != "assets") {
		return fmt.Errorf("usage: cellhive gc bundles|assets")
	}
	data, err := adminCall(http.MethodPost, "/v1/control/gc/"+args[0], nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdReleases prints a worker's release log.
func cmdReleases(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive releases <namespace> <worker>")
	}
	data, err := adminCall(http.MethodGet, "/v1/control/releases?namespace="+url.QueryEscape(args[0])+"&worker="+url.QueryEscape(args[1]), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func cmdDeploy(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive deploy <namespace> <worker> (--bundle <file> | --bundle-sha <sha>) [--assets-sha] [--route <host>]")
	}
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	bundleFile := fs.String("bundle", "", "local bundle file: hash, upload, then deploy")
	bundleSHA := fs.String("bundle-sha", "", "content-addressed bundle sha256")
	assetsSHA := fs.String("assets-sha", "", "assets sha256")
	assetsDir := fs.String("assets-dir", "", "local assets directory: upload all files under one version token, then deploy")
	route := fs.String("route", "", "optional host route to set for this worker")
	configPath := fs.String("config", "", "wrangler.jsonc/json to map (instead of explicit flags)")
	envName := fs.String("env", "", "environment within --config")
	var consumers, crons, rwfPaths listFlag
	fs.Var(&consumers, "consumer", "queue consumer: <queue>[:maxRetries[:deadLetterQueue[:maxConcurrency]]] (repeatable)")
	fs.Var(&crons, "cron", "cron expression this worker handles via scheduled() (repeatable)")
	migrationsJSON := fs.String("migrations", "", "DO migrations JSON array (e.g. '[{\"tag\":\"v1\",\"new_sqlite_classes\":[\"Room\"]}]')")
	idemKey := fs.String("idempotency-key", "", "de-duplicate a repeated deploy (returns the first version)")
	var kvB, d1B, r2B, qB, svcB, wfB, hdB listFlag
	fs.Var(&kvB, "kv", "KV binding NAME=ID (repeatable)")
	fs.Var(&d1B, "d1", "D1 binding NAME=ID (repeatable)")
	fs.Var(&r2B, "r2", "R2 binding NAME=BUCKET (repeatable)")
	fs.Var(&qB, "queue", "Queue binding NAME=QUEUE (repeatable)")
	fs.Var(&svcB, "service", "Service binding NAME=TARGET_WORKER (repeatable)")
	fs.Var(&wfB, "workflow", "Workflow binding NAME=RESOURCE:CLASS (repeatable)")
	fs.Var(&hdB, "hyperdrive", "Hyperdrive binding NAME=<registered-resource|[postgres|mysql]://url> (repeatable)")
	assetsNotFound := fs.String("assets-not-found", "", "assets not_found_handling: none|404-page|single-page-application")
	runWorkerFirst := fs.Bool("run-worker-first", false, "run the worker before assets")
	fs.Var(&rwfPaths, "run-worker-first-path", "path prefix where the worker runs before assets (repeatable)")
	dryRun := fs.Bool("dry-run", false, "validate only: compatibility gate + bundle + ACL, without deploying")
	var vars listFlag
	fs.Var(&vars, "var", "var override NAME=VALUE (repeatable; merges over config vars)")
	secretsFile := fs.String("secrets-file", "", "JSON object {\"KEY\":\"value\"} uploaded as secrets before deploying")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if *bundleFile != "" {
		sha, err := uploadBundle(*bundleFile)
		if err != nil {
			return err
		}
		bundleSHA = &sha
	}
	ns := args[0]
	worker := args[1]

	// --config maps a wrangler.jsonc/json to the deploy request (ADR-124): the
	// worker name may come from the config, and main/assets are bundled/uploaded
	// unless the caller passed explicit overrides.
	var mapped *wrangler.Deploy
	if *configPath != "" {
		if *bundleFile != "" || *bundleSHA != "" || len(kvB)+len(d1B)+len(r2B)+len(qB)+len(svcB)+len(wfB)+len(hdB) > 0 ||
			len(consumers) > 0 || len(crons) > 0 || *assetsSHA != "" || *assetsDir != "" || *migrationsJSON != "" {
			return fmt.Errorf("--config cannot be combined with explicit binding/bundle/assets flags")
		}
		m, err := wrangler.Load(*configPath, *envName)
		if err != nil {
			return err
		}
		mapped = m
		if worker == "" || worker == "-" {
			worker = m.Worker
		}
		if worker == "" {
			return fmt.Errorf("worker name is required (positional or config name)")
		}
		// Local preflight with the registered-resource set, then the server
		// re-validates authoritatively.
		if findings := m.ValidateWith(registeredResources(ns)); len(findings) > 0 {
			for _, f := range findings {
				fmt.Fprintf(os.Stderr, "%s: %s: %s\n", f.Code, f.FieldPath, f.Message)
			}
			return fmt.Errorf("config rejected by the compatibility gate")
		}
		if *bundleSHA == "" {
			sha, err := bundleFromConfig(m)
			if err != nil {
				return err
			}
			bundleSHA = &sha
		}
		if *assetsSHA == "" && m.AssetsDir != "" && !*dryRun {
			tok, err := uploadAssetsDir(ns, worker, m.AssetsDir)
			if err != nil {
				return err
			}
			assetsSHA = &tok
		}
	}
	if *bundleSHA == "" {
		return fmt.Errorf("--bundle, --bundle-sha or --config with a main is required")
	}
	if *assetsDir != "" && *assetsSHA == "" && !*dryRun {
		tok, err := uploadAssetsDir(ns, worker, *assetsDir)
		if err != nil {
			return err
		}
		assetsSHA = &tok
	}
	// --secrets-file uploads secrets before the deploy so the first request
	// already sees them (wrangler semantics). Skipped for --dry-run.
	if *secretsFile != "" && !*dryRun {
		if err := putSecretsFile(ns, worker, *secretsFile); err != nil {
			return err
		}
	}
	payload := map[string]any{
		"namespace": ns, "worker": worker, "bundle_sha": *bundleSHA, "assets_sha": *assetsSHA,
		"consumers": consumeSpecs(consumers),
		"crons":     []string(crons),
	}
	if mapped != nil {
		if mapped.CompatibilityDate != "" {
			payload["compatibility_date"] = mapped.CompatibilityDate
		}
		if len(mapped.CompatibilityFlags) > 0 {
			payload["compatibility_flags"] = mapped.CompatibilityFlags
		}
		if len(mapped.Vars) > 0 {
			payload["vars"] = mapped.Vars
		}
		if len(mapped.Bindings) > 0 {
			payload["bindings"] = mapped.Bindings
		}
		if len(mapped.Migrations) > 0 {
			payload["migrations"] = mapped.Migrations
		}
		if mapped.Assets != nil {
			payload["assets"] = mapped.Assets
		}
		if len(mapped.Consumers) > 0 {
			payload["consumers"] = mapped.Consumers
		}
		if len(mapped.Crons) > 0 {
			payload["crons"] = mapped.Crons
		}
	}
	// --var overrides win over config vars.
	if len(vars) > 0 {
		merged := map[string]string{}
		if m, ok := payload["vars"].(map[string]string); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
		for _, kv := range vars {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return fmt.Errorf("--var must be NAME=VALUE, got %q", kv)
			}
			merged[k] = v
		}
		payload["vars"] = merged
	}
	if *dryRun {
		payload["dry_run"] = true
	}
	if *idemKey != "" {
		payload["idempotency_key"] = *idemKey
	}
	if bs := buildBindings(kvB, d1B, r2B, qB, svcB, wfB, hdB); len(bs) > 0 {
		payload["bindings"] = bs
	}
	if *migrationsJSON != "" {
		var migs []map[string]any
		if err := json.Unmarshal([]byte(*migrationsJSON), &migs); err != nil {
			return fmt.Errorf("--migrations must be a JSON array: %w", err)
		}
		payload["migrations"] = migs
	}
	if *assetsNotFound != "" || *runWorkerFirst || len(rwfPaths) > 0 {
		payload["assets"] = map[string]any{
			"not_found_handling":     *assetsNotFound,
			"run_worker_first":       *runWorkerFirst,
			"run_worker_first_paths": []string(rwfPaths),
		}
	}
	data, err := adminCall(http.MethodPost, "/v1/control/deploy", payload)
	if err != nil {
		return err
	}
	if *route != "" && !*dryRun {
		if _, err := adminCall(http.MethodPost, "/v1/control/route", map[string]string{
			"namespace": ns, "host": *route, "worker": worker,
		}); err != nil {
			return err
		}
	}
	printJSON(data)
	return nil
}

func cmdPromote(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive promote <namespace> <worker> --version <n>")
	}
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	ver := fs.Int("version", 0, "version number to make active")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if *ver == 0 {
		return fmt.Errorf("--version is required")
	}
	data, err := adminCall(http.MethodPost, "/v1/control/promote", map[string]any{
		"namespace": args[0], "worker": args[1], "version": *ver,
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func cmdRollback(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive rollback <namespace> <worker>")
	}
	data, err := adminCall(http.MethodPost, "/v1/control/rollback", map[string]string{
		"namespace": args[0], "worker": args[1],
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func cmdRoute(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive route add <namespace> <host> <worker> | cellhive route rm <namespace> <host>")
	}
	switch args[0] {
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive route add <namespace> <host> <worker> [--path <prefix>]")
		}
		fs := flag.NewFlagSet("route add", flag.ContinueOnError)
		path := fs.String("path", "", "path prefix (always stripped before the worker sees the request)")
		if err := fs.Parse(args[4:]); err != nil {
			return err
		}
		data, err := adminCall(http.MethodPost, "/v1/control/route", map[string]string{
			"namespace": args[1], "host": args[2], "worker": args[3], "path": *path,
		})
		if err != nil {
			return err
		}
		printJSON(data)
	case "rm":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive route rm <namespace> <host>")
		}
		q := url.Values{"namespace": {args[1]}, "host": {args[2]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/route?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	default:
		return fmt.Errorf("usage: cellhive route add|rm ...")
	}
	return nil
}

func cmdSecret(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive secret put|get|delete|list ...")
	}
	switch args[0] {
	case "list":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive secret list <namespace> <worker>")
		}
		q := url.Values{"namespace": {args[1]}, "worker": {args[2]}}
		data, err := adminCall(http.MethodGet, "/v1/control/secrets?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "delete":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive secret delete <namespace> <worker> <KEY>")
		}
		q := url.Values{"namespace": {args[1]}, "worker": {args[2]}, "key": {args[3]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/secret?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "put":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive secret put <namespace> <worker> <KEY> [--value <b64>|stdin]")
		}
		fs := flag.NewFlagSet("secret put", flag.ContinueOnError)
		val := fs.String("value", "", "base64 value (default: read stdin)")
		if err := fs.Parse(args[4:]); err != nil {
			return err
		}
		raw := *val
		if raw == "" {
			in, _ := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
			raw = base64.StdEncoding.EncodeToString(in)
		}
		data, err := adminCall(http.MethodPost, "/v1/control/secret", map[string]string{
			"namespace": args[1], "worker": args[2], "key": args[3], "value": raw,
		})
		if err != nil {
			return err
		}
		printJSON(data)
	case "get":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive secret get <namespace> <worker> <KEY>")
		}
		q := url.Values{"namespace": {args[1]}, "worker": {args[2]}, "key": {args[3]}}
		data, err := adminCall(http.MethodGet, "/v1/control/secret?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	default:
		return fmt.Errorf("usage: cellhive secret put|get|delete|list ...")
	}
	return nil
}

// uploadBundle hashes a local file, uploads it, and returns the content sha.
// registeredResources fetches the namespace's registered resources so the local
// preflight can reject unregistered bindings before deploy (no auto-provisioning).
func registeredResources(ns string) func(kind, name string) bool {
	set := map[string]bool{}
	if data, err := adminCall(http.MethodGet, "/v1/control/resources?namespace="+url.QueryEscape(ns), nil); err == nil {
		var out struct {
			Resources []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"resources"`
		}
		if json.Unmarshal(data, &out) == nil {
			for _, r := range out.Resources {
				set[r.Kind+"|"+r.Name] = true
			}
		}
	}
	return func(kind, name string) bool { return set[kind+"|"+name] }
}

// bundleFromConfig builds and uploads the config's `main` (esbuild in-process,
// --no-bundle uploads the file as-is).
func bundleFromConfig(m *wrangler.Deploy) (string, error) {
	if m.Main == "" {
		return "", fmt.Errorf("config has no main; pass --bundle-sha")
	}
	if m.NoBundle {
		return uploadBundle(m.Main)
	}
	rules := make([]bundler.Rule, 0, len(m.Rules))
	for _, r := range m.Rules {
		rules = append(rules, bundler.Rule{Type: r.Type, Globs: r.Globs})
	}
	out, err := bundler.Build(context.Background(), bundler.Options{
		EntryPoint: m.Main, OutFile: filepath.Join(os.TempDir(), "cellhive-bundle.js"),
		Minify: m.Minify, KeepNames: m.KeepNames, NodeJSCompat: m.NodeCompat,
		Rules: rules, Define: m.Defines,
	})
	if err != nil {
		return "", err
	}
	return uploadBundleBytes(out)
}

func uploadBundle(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return uploadBundleBytes(data)
}

func uploadBundleBytes(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	local := hex.EncodeToString(sum[:])
	if _, err := adminCallRaw("/v1/control/bundle", data); err != nil {
		return "", err
	}
	fmt.Fprintln(os.Stderr, "uploaded bundle", local, "(", len(data), "bytes )")
	return local, nil
}

func cmdBundle(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive bundle put <file> | cellhive bundle build <entry> --out <file>")
	}
	switch args[0] {
	case "put":
		if len(args) < 2 {
			return fmt.Errorf("usage: cellhive bundle put <file>")
		}
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		out, err := adminCallRaw("/v1/control/bundle", data)
		if err != nil {
			return err
		}
		printJSON(out)
		return nil
	case "build":
		return cmdBundleBuild(args[1:])
	default:
		return fmt.Errorf("usage: cellhive bundle put <file> | cellhive bundle build <entry> --out <file>")
	}
}

func cmdAsset(args []string) error {
	if len(args) < 5 || args[0] != "put" {
		return fmt.Errorf("usage: cellhive asset put <namespace> <worker> <path> <file>")
	}
	ns, worker, path, file := args[1], args[2], args[3], args[4]
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	q := url.Values{"ns": {ns}, "worker": {worker}, "path": {path}}
	out, err := adminCallRaw("/v1/control/asset?"+q.Encode(), data)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func cmdRoutes() error {
	data, err := internalGet("/v1/control/routes")
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// uploadAssetsDir uploads every file under dir under a single deterministic
// version token (sha256 of the sorted path+content list) and returns the token,
// which the version stores as assets_sha (the loader's asset token).
// assetsDirToken computes a deterministic version token over dir's files
// (sha256 of sorted "rel\0contentHash\n" lines) and returns it with the sorted
// relative paths.
func assetsDirToken(dir string) (string, []string, error) {
	type entry struct{ rel, sum string }
	var entries []entry
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		sum := sha256.Sum256(data)
		entries = append(entries, entry{rel, hex.EncodeToString(sum[:])})
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })
	h := sha256.New()
	rels := make([]string, 0, len(entries))
	for _, e := range entries {
		h.Write([]byte(e.rel))
		h.Write([]byte{0})
		h.Write([]byte(e.sum))
		h.Write([]byte{10})
		rels = append(rels, e.rel)
	}
	return hex.EncodeToString(h.Sum(nil)), rels, nil
}

func uploadAssetsDir(ns, worker, dir string) (string, error) {
	token, rels, err := assetsDirToken(dir)
	if err != nil {
		return "", err
	}
	for _, rel := range rels {
		data, rerr := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		if rerr != nil {
			return "", rerr
		}
		q := url.Values{"ns": {ns}, "worker": {worker}, "path": {rel}, "token": {token}}
		if _, err := adminCallRaw("/v1/control/asset?"+q.Encode(), data); err != nil {
			return "", err
		}
	}
	fmt.Fprintf(os.Stderr, "uploaded %d assets under token %s\n", len(rels), token)
	return token, nil
}

// cmdTail follows a namespace's control-plane audit trail (poll-based) or, with
// --worker ns/worker, the bounded per-worker log buffer.
// putSecretsFile reads a JSON object {"KEY":"value"} and uploads each entry as a
// control-plane secret (ADR-147/148).
func putSecretsFile(ns, worker, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var secrets map[string]string
	if err := json.Unmarshal(raw, &secrets); err != nil {
		return fmt.Errorf("--secrets-file must be a JSON object of {\"KEY\":\"value\"}: %w", err)
	}
	for k, v := range secrets {
		if _, err := adminCall(http.MethodPost, "/v1/control/secret", map[string]string{
			"namespace": ns, "worker": worker, "key": k,
			"value": base64.StdEncoding.EncodeToString([]byte(v)),
		}); err != nil {
			return fmt.Errorf("secret %s: %w", k, err)
		}
	}
	return nil
}

// cmdVectorize implements the Cloudflare-compatible vectorize CLI (ADR-158):
// index lifecycle + data operations (insert/upsert/query/get/delete/list) via
// the scoped-token binding API, and metadata-index management via the operator
// API.
func cmdVectorize(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive vectorize create|list|delete|info|stats|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index ...")
	}
	verb := args[0]
	switch verb {
	case "create":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize create <namespace> <name> --dimensions <n> --metric <cosine|euclidean|dot-product> [--description <text>]")
		}
		fs := flag.NewFlagSet("vectorize create", flag.ContinueOnError)
		dims := fs.Int("dimensions", 0, "vector dimensions (1..1536)")
		metric := fs.String("metric", "", "cosine|euclidean|dot-product")
		desc := fs.String("description", "", "index description")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		ns, name := args[1], args[2]
		payload := map[string]any{
			"namespace": ns, "kind": "vectorize", "name": name,
			"scope":  fmt.Sprintf("%s/__vectorize__/%s", ns, name),
			"config": map[string]any{"dimensions": *dims, "metric": *metric, "description": *desc},
		}
		data, err := adminCall(http.MethodPost, "/v1/vectorize/resources", payload)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "list", "ls":
		if len(args) < 2 {
			return fmt.Errorf("usage: cellhive vectorize list <namespace>")
		}
		return cmdKind("vectorize", []string{"list", args[1]})
	case "delete", "rm":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize delete <namespace> <name> [--force]")
		}
		return cmdKind("vectorize", args)
	case "stats", "info":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize %s <namespace> <name>", verb)
		}
		// `info` mirrors wrangler and includes the immutable config + counts.
		data, err := adminCall(http.MethodGet,
			"/v1/vectorize/stats?"+url.Values{"namespace": {args[1]}, "name": {args[2]}}.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "insert", "upsert":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize %s <namespace> <name> --file <ndjson>", verb)
		}
		fs := flag.NewFlagSet("vectorize "+verb, flag.ContinueOnError)
		file := fs.String("file", "", "ndjson file with {id, values, metadata?, namespace?} per line")
		batch := fs.Int("batch-size", 1000, "vectors per request (max 1000)")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		if *file == "" {
			return fmt.Errorf("--file is required (ndjson)")
		}
		vectors, err := readVectorFile(*file)
		if err != nil {
			return err
		}
		if *batch <= 0 || *batch > 1000 {
			*batch = 1000
		}
		var last map[string]any
		for i := 0; i < len(vectors); i += *batch {
			end := i + *batch
			if end > len(vectors) {
				end = len(vectors)
			}
			data, err := scopeCall(http.MethodPost, "/v1/vectorize/"+verb, args[1], args[2],
				map[string]any{"vectors": vectors[i:end]})
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &last); err != nil {
				return err
			}
		}
		raw, err := json.Marshal(last)
		if err != nil {
			return err
		}
		printJSON(raw)
		return nil
	case "query":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize query <namespace> <name> (--vector '1,2,3' | --vector-id <id>) [--top-k N] [--return-values] [--return-metadata none|indexed|all] [--namespace <ns>] [--filter JSON]")
		}
		fs := flag.NewFlagSet("vectorize query", flag.ContinueOnError)
		vec := fs.String("vector", "", "comma-separated query vector")
		vecID := fs.String("vector-id", "", "query using a stored vector id")
		topK := fs.Int("top-k", 5, "number of matches")
		returnValues := fs.Bool("return-values", false, "include vector values")
		returnMeta := fs.String("return-metadata", "none", "none|indexed|all")
		nsFilter := fs.String("namespace", "", "namespace partition to search")
		filter := fs.String("filter", "", "metadata filter JSON")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		body := map[string]any{"topK": *topK, "returnValues": *returnValues, "returnMetadata": *returnMeta}
		if *vec != "" {
			vals, err := parseVector(strings.Split(*vec, ","))
			if err != nil {
				return err
			}
			body["vector"] = vals
		} else if *vecID != "" {
			body["id"] = *vecID
		} else {
			return fmt.Errorf("--vector or --vector-id is required")
		}
		if *nsFilter != "" {
			body["namespace"] = *nsFilter
		}
		if *filter != "" {
			var f any
			if err := json.Unmarshal([]byte(*filter), &f); err != nil {
				return fmt.Errorf("--filter must be JSON: %w", err)
			}
			body["filter"] = f
		}
		data, err := scopeCall(http.MethodPost, "/v1/vectorize/query", args[1], args[2], body)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "get-vectors", "delete-vectors":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive vectorize %s <namespace> <name> --ids <id> [<id>...]", verb)
		}
		path := "/v1/vectorize/get"
		if verb == "delete-vectors" {
			path = "/v1/vectorize/delete"
		}
		ids := args[3:]
		if ids[0] == "--ids" {
			ids = ids[1:]
		}
		if len(ids) == 0 {
			return fmt.Errorf("--ids is required")
		}
		data, err := scopeCall(http.MethodPost, path, args[1], args[2], map[string]any{"ids": ids})
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "list-vectors":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize list-vectors <namespace> <name> [--count N] [--cursor C]")
		}
		fs := flag.NewFlagSet("vectorize list-vectors", flag.ContinueOnError)
		count := fs.Int("count", 100, "ids per page (1..1000)")
		cursor := fs.String("cursor", "", "pagination cursor")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		data, err := scopeCall(http.MethodGet, "/v1/vectorize/list", args[1], args[2], nil,
			url.Values{"count": {strconv.Itoa(*count)}, "cursor": {*cursor}})
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "create-metadata-index", "delete-metadata-index":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize %s <namespace> <name> --property-name <p> [--type string|number|boolean]", verb)
		}
		fs := flag.NewFlagSet("vectorize "+verb, flag.ContinueOnError)
		prop := fs.String("property-name", "", "metadata property")
		typ := fs.String("type", "", "string|number|boolean")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		if *prop == "" {
			return fmt.Errorf("--property-name is required")
		}
		method := http.MethodPost
		path := "/v1/vectorize/metadata-index"
		if verb == "delete-metadata-index" {
			method = http.MethodDelete
		}
		payload := map[string]string{"namespace": args[1], "name": args[2], "property": *prop, "type": *typ}
		data, err := adminCall(method, path, payload)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "rebuild", "build-ann":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize rebuild <namespace> <name> [--buckets N] [--quantizer none|pq|opq|bq] [--codesize N] [--nprobe F]")
		}
		fs := flag.NewFlagSet("vectorize rebuild", flag.ContinueOnError)
		buckets := fs.Int("buckets", 1024, "IVF buckets (0 = exhaustive)")
		quantizer := fs.String("quantizer", "opq", "none|pq|opq|bq")
		codesize := fs.Int("codesize", 32, "quantized vector bytes (0 = default)")
		nprobe := fs.Float64("nprobe", 0.05, "buckets probed: fraction (<1) or count (>=1)")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		payload := map[string]any{
			"namespace": args[1], "name": args[2],
			"buckets": *buckets, "quantizer": *quantizer, "codesize": *codesize, "nprobe": *nprobe,
		}
		data, err := adminCall(http.MethodPost, "/v1/vectorize/rebuild", payload)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "drop-ann", "drop-index":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize drop-ann <namespace> <name>")
		}
		q := url.Values{"namespace": {args[1]}, "name": {args[2]}}
		data, err := adminCall(http.MethodDelete, "/v1/vectorize/ann?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "list-metadata-index", "list-metadata-indexes":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive vectorize %s <namespace> <name>", verb)
		}
		data, err := adminCall(http.MethodGet,
			"/v1/vectorize/metadata-indexes?"+url.Values{"namespace": {args[1]}, "name": {args[2]}}.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	default:
		return fmt.Errorf("unknown vectorize command %q (create|list|delete|info|stats|insert|upsert|query|get-vectors|delete-vectors|list-vectors|rebuild|drop-ann|create-metadata-index|list-metadata-index|delete-metadata-index)", verb)
	}
}

// scopeCall mints a scoped binding token from the platform root key and calls a
// tenant binding endpoint on the internal listener (ADR-074/158).
func scopeCall(method, path, ns, index string, body any, extra ...url.Values) (json.RawMessage, error) {
	creds := config.DeriveCredentials(config.LoadRootKey())
	if creds.Scope == "" {
		return nil, fmt.Errorf("CELLHIVE_ROOT_KEY is required to mint a scoped token")
	}
	token, err := scopedtoken.Mint([]byte(creds.Scope), scopedtoken.Claims{
		Namespace: ns, Kind: "vectorize", Name: index,
		ExpiresMs: time.Now().Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	qs := url.Values{"ns": {ns}, "index": {index}}
	if len(extra) > 0 {
		for k, v := range extra[0] {
			if len(v) > 0 && v[0] != "" {
				qs.Set(k, v[0])
			}
		}
	}
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, internalURL()+path+"?"+qs.Encode(), rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-scope-token", token)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return data, nil
}

// readVectorFile parses an ndjson vector file.
func readVectorFile(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			return nil, fmt.Errorf("ndjson line %d: %w", len(out)+1, err)
		}
		out = append(out, v)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no vectors in %s", path)
	}
	return out, nil
}

// parseVector converts comma-separated floats.
func parseVector(parts []string) ([]float32, error) {
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("bad vector component %q", p)
		}
		out = append(out, float32(f))
	}
	return out, nil
}

// cmdKind implements the per-domain resource registry + stats CLI (ADR-157):
//
//	cellhive kv namespace create|list|delete|stats <ns> <name> [flags]
//	cellhive d1 create|list|delete|info <ns> <db>
//	cellhive r2 bucket create|list|delete|stats <ns> <bucket>
//	cellhive hyperdrive create|list|delete|stats <ns> <name>
//	cellhive queue create|list|delete|stats <ns> <name>
//	cellhive workflows create|list|delete|stats <ns> <name>
//
// Data-plane verbs (kv key ..., d1 execute, r2 object ...) stay unsupported.
func cmdKind(kind string, args []string) error {
	rest := args
	// wrangler-style nouns: `kv namespace ...`, `r2 bucket ...`.
	if len(rest) > 0 && (rest[0] == "namespace" || rest[0] == "bucket") {
		rest = rest[1:]
	}
	if len(rest) == 0 {
		return fmt.Errorf("usage: cellhive %s create|list|delete|stats <namespace> [name]", kind)
	}
	verb := rest[0]
	switch verb {
	case "create":
		if len(rest) < 3 {
			return fmt.Errorf("usage: cellhive %s create <namespace> <name> [--scope <scope>] [--connection-string <url>]", kind)
		}
		fs := flag.NewFlagSet(kind+" create", flag.ContinueOnError)
		scope := fs.String("scope", "", "resource cell scope (default <ns>/__<kind>__/<name>)")
		conn := fs.String("connection-string", "", "hyperdrive origin URL (sealed server-side)")
		if err := fs.Parse(rest[3:]); err != nil {
			return err
		}
		ns, name := rest[1], rest[2]
		sc := *scope
		if sc == "" {
			sc = ns + "/__" + kind + "__/" + name
		}
		payload := map[string]string{"namespace": ns, "kind": kind, "name": name, "scope": sc}
		if kind == "hyperdrive" && *conn != "" {
			payload["connection_string"] = *conn
		}
		data, err := adminCall(http.MethodPost, "/v1/"+kind+"/resources", payload)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "list", "ls":
		if len(rest) < 2 {
			return fmt.Errorf("usage: cellhive %s list <namespace>", kind)
		}
		q := url.Values{"namespace": {rest[1]}}
		data, err := adminCall(http.MethodGet, "/v1/"+kind+"/resources?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "delete", "rm", "revoke":
		if len(rest) < 3 {
			return fmt.Errorf("usage: cellhive %s delete <namespace> <name> [--force]", kind)
		}
		fs := flag.NewFlagSet(kind+" delete", flag.ContinueOnError)
		force := fs.Bool("force", false, "revoke even while worker versions still bind the resource")
		if err := fs.Parse(rest[3:]); err != nil {
			return err
		}
		q := url.Values{"namespace": {rest[1]}, "name": {rest[2]}}
		if *force {
			q.Set("force", "1")
		}
		data, err := adminCall(http.MethodDelete, "/v1/"+kind+"/resources?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	case "stats", "info":
		if len(rest) < 3 {
			return fmt.Errorf("usage: cellhive %s stats <namespace> <name> [--prefix <p>] [--exact] [--tables] [--limit N]", kind)
		}
		fs := flag.NewFlagSet(kind+" stats", flag.ContinueOnError)
		prefix := fs.String("prefix", "", "restrict count-style fields to a key prefix")
		exact := fs.Bool("exact", false, "force exact counts (may scan)")
		tables := fs.Bool("tables", false, "include per-table dbstat usage (D1)")
		limit := fs.Int("limit", 0, "max objects to inspect (R2)")
		if err := fs.Parse(rest[3:]); err != nil {
			return err
		}
		q := url.Values{"namespace": {rest[1]}, "name": {rest[2]}}
		if *prefix != "" {
			q.Set("prefix", *prefix)
		}
		if *exact {
			q.Set("exact", "1")
		}
		if *tables {
			q.Set("tables", "1")
		}
		if *limit > 0 {
			q.Set("limit", strconv.Itoa(*limit))
		}
		if kind == "r2" {
			q.Set("bucket", rest[2])
		}
		data, err := adminCall(http.MethodGet, "/v1/"+kind+"/stats?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	default:
		return fmt.Errorf("%s %s is not supported: CellHive exposes %s to worker bindings only; use `cellhive dev` locally or a worker with a %s binding (docs/bindings.md). Registry ops: `cellhive %s create|list|delete|stats`",
			kind, verb, strings.ToUpper(kind), kind, kind)
	}
}

// cmdQueue is operator queue tooling: status and dead-letter replay (ADR-156).
func cmdQueue(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cellhive queue status <namespace> <queue> | cellhive queue replay-dlq <namespace> <queue> [--limit N]")
	}
	switch args[0] {
	case "status":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive queue status <namespace> <queue>")
		}
		q := url.Values{"namespace": {args[1]}, "queue": {args[2]}}
		data, err := adminCall(http.MethodGet, "/v1/control/queue/status?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "replay-dlq", "replay":
		if len(args) < 3 {
			return fmt.Errorf("usage: cellhive queue replay-dlq <namespace> <queue> [--limit N]")
		}
		fs := flag.NewFlagSet("queue replay-dlq", flag.ContinueOnError)
		limit := fs.Int("limit", 100, "max messages to replay")
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		q := url.Values{"namespace": {args[1]}, "queue": {args[2]}, "limit": {strconv.Itoa(*limit)}}
		data, err := adminCall(http.MethodPost, "/v1/control/queue/replay-dlq?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "create", "list", "ls", "delete", "rm", "revoke", "stats", "info":
		// Registry operations live under the per-domain surface (ADR-157).
		return cmdKind("queue", args)
	default:
		return fmt.Errorf("usage: cellhive queue create|list|delete|stats <ns> <name> | queue status|replay-dlq <ns> <queue>")
	}
	return nil
}

// cmdCompat covers wrangler command names CellHive either maps onto an existing
// command or explicitly does not support (structured rejection + alternative),
// so a migrating user gets a clear answer instead of "unknown command" (ADR-148).
func cmdCompat(args []string) error {
	cmd, rest := args[0], args[1:]
	first := ""
	if len(rest) > 0 {
		first = rest[0]
	}
	switch cmd {
	case "versions":
		switch first {
		case "list":
			if len(rest) < 3 {
				return fmt.Errorf("usage: cellhive versions list <namespace> <worker>")
			}
			return cmdReleases(rest[1:])
		case "upload":
			return fmt.Errorf("versions upload is not supported: CellHive deploys publish atomically; use `cellhive deploy` (or `cellhive promote --version` to roll back to a version)")
		case "view":
			return fmt.Errorf("versions view is not supported: use `cellhive releases <namespace> <worker>` (versions + bundle sha + crons + bindings)")
		default:
			return fmt.Errorf("usage: cellhive versions list <namespace> <worker>")
		}
	case "deployments":
		if first == "list" || first == "status" {
			if len(rest) < 3 {
				return fmt.Errorf("usage: cellhive deployments %s <namespace> <worker>", first)
			}
			return cmdReleases(rest[1:])
		}
		return fmt.Errorf("usage: cellhive deployments list|status <namespace> <worker>")
	case "triggers":
		if first == "deploy" {
			return fmt.Errorf("triggers deploy is not needed: `cellhive deploy --config wrangler.jsonc` applies `triggers.crons` atomically; use `cellhive triggers list <ns> <worker>` to inspect")
		}
		if first != "list" || len(rest) < 3 {
			return fmt.Errorf("usage: cellhive triggers list <namespace> <worker>")
		}
		return printWorkerCrons(rest[1], rest[2])
	case "workflows":
		switch first {
		case "list":
			if len(rest) < 2 {
				return fmt.Errorf("usage: cellhive workflows list <namespace>")
			}
			return cmdKind("workflow", []string{"list", rest[1]})
		case "create", "delete", "stats", "info":
			return cmdKind("workflow", rest)
		case "status", "describe", "trigger":
			return fmt.Errorf("workflows %s is not supported: instances are runtime-only; use the `env.WF` binding (create/get/sendEvent) from a Worker, or `cellhive workflows create|list|delete|stats <ns> <name>` for definitions", first)
		default:
			return fmt.Errorf("workflows %s is not supported: use `cellhive workflows create|list|delete|stats <ns> <name>` (definitions) and the runtime API (env.WF) for instances", first)
		}
	case "queues":
		switch first {
		case "list":
			if len(rest) < 2 {
				return fmt.Errorf("usage: cellhive queues list <namespace>")
			}
			return cmdKind("queue", []string{"list", rest[1]})
		case "create", "delete", "stats", "info":
			return cmdKind("queue", rest)
		case "consumer":
			return fmt.Errorf("queues consumer is not supported: consumer settings come from `queues.consumers` in wrangler.jsonc and are applied by `cellhive deploy --config`; inspect backlog with `cellhive queue status <ns> <name>`")
		default:
			return fmt.Errorf("queues %s is not supported: use `cellhive queue create|list|delete|stats <ns> <name>` (or the `cellhive resource` generic form)", first)
		}
	case "types":
		return fmt.Errorf("wrangler types is not supported: write your own Env interface (see docs/wrangler-compat.md) or use `cellhive dev` which understands the wrangler config")
	case "init":
		return fmt.Errorf("wrangler init is not supported: create wrangler.jsonc yourself (name/main/compatibility_date) and run `cellhive wrangler deploy --namespace <ns>`")
	}
	return fmt.Errorf("unknown command: %s", cmd)
}

// printWorkerCrons prints the cron expressions of a worker's active version.
func printWorkerCrons(ns, worker string) error {
	data, err := adminCall(http.MethodGet, "/v1/control/releases?namespace="+url.QueryEscape(ns)+"&worker="+url.QueryEscape(worker), nil)
	if err != nil {
		return err
	}
	var rel struct {
		Releases []struct {
			Number int      `json:"number"`
			Crons  []string `json:"crons"`
		} `json:"releases"`
	}
	if err := json.Unmarshal(data, &rel); err != nil {
		return err
	}
	out := []string{}
	for _, r := range rel.Releases {
		out = append(out, r.Crons...)
	}
	enc, err := json.Marshal(map[string]any{"namespace": ns, "worker": worker, "crons": out})
	if err != nil {
		return err
	}
	printJSON(enc)
	return nil
}

// cmdServiceACL manages the target-side allowlist for cross-namespace service
// bindings (ADR-144).
func cmdServiceACL(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cellhive service-acl add|ls|rm <namespace> [worker] [caller-namespace]")
	}
	switch args[0] {
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive service-acl add <namespace> <worker> <caller-namespace>")
		}
		data, err := adminCall(http.MethodPost, "/v1/control/service-acl", map[string]string{
			"namespace": args[1], "worker": args[2], "caller_namespace": args[3],
		})
		if err != nil {
			return err
		}
		printJSON(data)
	case "rm":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive service-acl rm <namespace> <worker> <caller-namespace>")
		}
		q := url.Values{"namespace": {args[1]}, "worker": {args[2]}, "caller_namespace": {args[3]}}
		data, err := adminCall(http.MethodDelete, "/v1/control/service-acl?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	case "ls":
		if len(args) < 2 {
			return fmt.Errorf("usage: cellhive service-acl ls <namespace>")
		}
		data, err := adminCall(http.MethodGet, "/v1/control/service-acls?namespace="+url.QueryEscape(args[1]), nil)
		if err != nil {
			return err
		}
		printJSON(data)
	default:
		return fmt.Errorf("usage: cellhive service-acl add|ls|rm ...")
	}
	return nil
}

// cmdCreds prints the role credentials derived from CELLHIVE_ROOT_KEY, so
// scripts and operators never hand-copy per-role secrets (ADR-137).
func cmdCreds(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("usage: cellhive creds [role]")
	}
	c := config.DeriveCredentials(config.LoadRootKey())
	all := map[string]string{
		"peer": c.Peer, "internal": c.Internal, "dispatch": c.Dispatch,
		"log": c.Log, "admin": c.Admin, "scope": c.Scope,
		"do-ticket": c.DoTicket, "secret-key": c.SecretKey,
	}
	if c.Internal == "" {
		return fmt.Errorf("CELLHIVE_ROOT_KEY is required (or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use)")
	}
	if len(args) == 1 {
		v, ok := all[args[0]]
		if !ok {
			return fmt.Errorf("unknown role %q (peer|internal|dispatch|log|admin|scope|do-ticket|secret-key)", args[0])
		}
		fmt.Println(v)
		return nil
	}
	for _, role := range []string{"peer", "internal", "dispatch", "log", "admin", "scope", "do-ticket", "secret-key"} {
		fmt.Printf("%s=%s\n", role, all[role])
	}
	return nil
}

func cmdTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	worker := fs.String("worker", "", "follow worker logs as <namespace>/<worker>")
	rest := args
	if len(args) > 0 && strings.HasPrefix(args[0], "-") {
		if err := fs.Parse(args); err != nil {
			return err
		}
		rest = fs.Args()
	}
	if *worker != "" {
		parts := strings.SplitN(*worker, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--worker must be <namespace>/<worker>")
		}
		return tailWorkerLogs(parts[0], parts[1])
	}
	if len(rest) < 1 {
		return fmt.Errorf("usage: cellhive tail <namespace> | cellhive tail --worker <ns>/<worker>")
	}
	ns := rest[0]
	fmt.Fprintf(os.Stderr, "tailing audit for %s (ctrl-c to stop)\n", ns)
	seen := map[string]bool{}
	for {
		q := url.Values{"namespace": {ns}, "limit": {"100"}}
		data, err := adminCall(http.MethodGet, "/v1/control/audit?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		var out struct {
			Audit []struct {
				AtMs   int64  `json:"at_ms"`
				Actor  string `json:"actor"`
				Action string `json:"action"`
				Target string `json:"target"`
			} `json:"audit"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		for _, a := range out.Audit {
			key := fmt.Sprintf("%d/%s/%s", a.AtMs, a.Actor, a.Target)
			if seen[key] {
				continue
			}
			seen[key] = true
			fmt.Printf("%s %s %s %s\n", time.UnixMilli(a.AtMs).Format(time.RFC3339), a.Actor, a.Action, a.Target)
		}
		time.Sleep(time.Second)
	}
}

// tailWorkerLogs polls the bounded per-worker log buffer (non-durable).
func tailWorkerLogs(ns, worker string) error {
	fmt.Fprintf(os.Stderr, "tailing logs for %s/%s (ctrl-c to stop)\n", ns, worker)
	var cursor uint64
	mode := ""
	for {
		// Renew the OTLP log-export subscription while this tail is running, so
		// backend export stops shortly after it exits (ADR-172).
		if data, err := adminCall(http.MethodPost, "/v1/control/logs/subscribe", map[string]any{
			"namespace": ns, "worker": worker, "ttl_ms": 60000,
		}); err == nil {
			var sub struct {
				Mode string `json:"mode"`
			}
			if json.Unmarshal(data, &sub) == nil && sub.Mode != mode {
				mode = sub.Mode
				if mode == "tail" || mode == "all" {
					fmt.Fprintf(os.Stderr, "otlp log export: %s\n", mode)
				}
			}
		}
		q := url.Values{"namespace": {ns}, "worker": {worker}, "since": {fmt.Sprint(cursor)}}
		data, err := adminCall(http.MethodGet, "/v1/control/logs?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		var out struct {
			Next    uint64 `json:"next"`
			Entries []struct {
				AtMs    int64  `json:"at_ms"`
				Level   string `json:"level"`
				Message string `json:"message"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		for _, e := range out.Entries {
			fmt.Printf("%s [%s] %s\n", time.UnixMilli(e.AtMs).Format(time.RFC3339), e.Level, e.Message)
		}
		cursor = out.Next
		time.Sleep(time.Second)
	}
}

func cmdAudit(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: cellhive audit <namespace>")
	}
	q := url.Values{"namespace": {args[0]}}
	data, err := adminCall(http.MethodGet, "/v1/control/audit?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func diagnose() error {
	req, err := http.NewRequest(http.MethodGet, internalURL()+"/v1/diagnose", nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", internalToken())
	data, err := doJSON(req)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Print(`cellhive - Cloudflare Workers compatible self-hosted runtime

Usage:
  cellhive version
  cellhive diagnose
  cellhive app create <namespace>
  cellhive app list
  cellhive app delete <namespace>
  cellhive worker delete <namespace> <worker>
  cellhive resource create <namespace> <kind> <name> [--scope <scope>] [--connection-string <url>]
    Hyperdrive: register the origin URL once (sealed server-side), then bind it with
    cellhive deploy --hyperdrive HYDR=<resource-name> (ADR-129).
  cellhive resource list <namespace> [--kind <kind>]
  cellhive resource delete <namespace> <kind> <name> [--force]   # revoke (data kept)
  cellhive deploy <namespace> <worker> (--bundle <file> | --bundle-sha <sha>) [--assets-sha <sha>] [--route <host>]
                   [--config <wrangler.jsonc>] [--env <name>]   # map a wrangler config (main/bindings/vars/crons/assets)
                   [--kv NAME=ID] [--d1 NAME=ID] [--r2 NAME=BUCKET] [--queue NAME=QUEUE]
                   [--service NAME=TARGET] [--workflow NAME=RESOURCE:CLASS] [--cron EXPR] [--consumer Q] [--migrations JSON]
  cellhive promote <namespace> <worker> --version <n>
  cellhive rollback <namespace> <worker>
  cellhive domain add <namespace> <host>   # register; routes immediately (no DNS check)
  cellhive domain ls <namespace>
  cellhive domain rm <namespace> <host>
  cellhive route add <namespace> <host> <worker> [--path <prefix>]  # prefix is always stripped (mount semantics)
  cellhive bundle put <file>
  cellhive asset put <namespace> <worker> <path> <file>
  cellhive route rm <namespace> <host>
  cellhive service-acl add <namespace> <worker> <caller-namespace>  # cross-ns service binding grant
  cellhive service-acl ls <namespace>
  cellhive service-acl rm <namespace> <worker> <caller-namespace>
  cellhive secret put <namespace> <worker> <KEY> [--value <b64>|stdin]
  cellhive secret get <namespace> <worker> <KEY>
  cellhive secret delete <namespace> <worker> <KEY>
  cellhive secret list <namespace> <worker>
  cellhive routes            # pull the routing projection (internal listener)
  cellhive releases <namespace> <worker>
  cellhive workflow create <namespace> <name>
  cellhive creds [role]                  # print root-derived role credentials
  cellhive versions list|deployments list|status <ns> <worker>   # = releases
  cellhive triggers list <namespace> <worker>
  cellhive workflows list <namespace> | cellhive queues list <namespace>
  cellhive vectorize create <namespace> <name> --dimensions <n> --metric <cosine|euclidean|dot-product>
  cellhive vectorize list|delete|info|stats <namespace> <name>
  cellhive vectorize insert|upsert <namespace> <name> --file <ndjson> [--batch-size N]
  cellhive vectorize query <namespace> <name> --vector "1,2,3" | --vector-id <id>
                     [--top-k N] [--return-values] [--return-metadata none|indexed|all] [--filter JSON]
  cellhive vectorize get-vectors|delete-vectors <namespace> <name> --ids <id>...
  cellhive vectorize list-vectors <namespace> <name> [--count N] [--cursor C]
  cellhive vectorize rebuild <namespace> <name> [--buckets N] [--quantizer opq] [--codesize N] [--nprobe F]
  cellhive vectorize drop-ann <namespace> <name>            # back to the exact flat index
  cellhive vectorize create-metadata-index <namespace> <name> --property-name <p> --type string|number|boolean
  cellhive kv namespace create|list|delete|stats <namespace> <name> [--scope] [--exact]
  cellhive d1 create|list|delete|info <namespace> <db> [--tables]
  cellhive r2 bucket create|list|delete|stats <namespace> <bucket> [--prefix] [--limit N]
  cellhive queue create|list|delete|stats <namespace> <name>
  cellhive workflows create|list|delete|stats <namespace> <name> [--exact]
  cellhive hyperdrive create|list|delete|stats <namespace> <name> [--connection-string <url>]
    # per-domain registry + cheap stats; stats fields are metadata-only (page/file
    # bytes, schema, expiry index) and opt-in exact counts (ADR-157)
  cellhive queue status <namespace> <queue>          # depth/visible/leased (+死信深度)
  cellhive queue replay-dlq <namespace> <queue> [--limit N]
  cellhive wrangler <command> ...        # wrangler-style alias (auto-discovers wrangler.jsonc):
                                         #   deploy --namespace <ns> [-c file] [--env e] [--name w]
                                         #   delete <ns> <worker> | secret put|get | tail | rollback
                                         #   versions list | deployments list | promote
  cellhive tail <namespace>              # follow the audit trail
  cellhive tail --worker <ns>/<worker>   # follow the bounded worker log buffer
  cellhive status
  cellhive capacity
  cellhive gc bundles        # delete bundles no longer referenced by a version (grace period)
  cellhive gc assets         # delete asset versions no longer referenced by a version
  cellhive audit <namespace>

Environment:
  CELLHIVE_ADMIN_URL    admin (control) endpoint  (default http://127.0.0.1:8082)
  CELLHIVE_CONTROL_URL  internal endpoint (:7001) (default http://127.0.0.1:7001)
  CELLHIVE_ROOT_KEY     platform root secret; the CLI derives the admin and
                        internal tokens from it (ADR-137)
  CELLHIVE_ADMIN_TOKEN  optional override for the admin credential
`)
}

// buildBindings assembles binding entries from the per-kind deploy flags.
func buildBindings(kv, d1, r2, q, svc, wf, hd []string) []map[string]any {
	var out []map[string]any
	simple := func(typ string, flags []string) {
		for _, s := range flags {
			name, id, ok := strings.Cut(s, "=")
			if !ok || name == "" || id == "" {
				continue
			}
			out = append(out, map[string]any{"type": typ, "name": name, "id": id})
		}
	}
	simple("kv", kv)
	simple("d1", d1)
	simple("r2", r2)
	simple("queue", q)
	simple("service", svc)
	simple("hyperdrive", hd)
	for _, s := range wf {
		name, rest, ok := strings.Cut(s, "=")
		if !ok || name == "" {
			continue
		}
		resource, class, _ := strings.Cut(rest, ":")
		if resource == "" {
			continue
		}
		out = append(out, map[string]any{"type": "workflow", "name": name, "id": resource, "class_name": class})
	}
	return out
}

// consumeSpecs parses --consumer values:
// <queue>[:maxRetries[:deadLetterQueue[:maxConcurrency]]].
func consumeSpecs(specs []string) []map[string]any {
	out := make([]map[string]any, 0, len(specs))
	for _, spec := range specs {
		parts := strings.Split(spec, ":")
		c := map[string]any{"queue": parts[0]}
		if len(parts) > 1 && parts[1] != "" {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				c["max_retries"] = n
			}
		}
		if len(parts) > 2 && parts[2] != "" {
			c["dead_letter_queue"] = parts[2]
		}
		if len(parts) > 3 && parts[3] != "" {
			if n, err := strconv.Atoi(parts[3]); err == nil {
				c["max_concurrency"] = n
			}
		}
		out = append(out, c)
	}
	return out
}
