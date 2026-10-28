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
	"errors"
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
	root := newRoot()
	root.SetArgs(normalizeLegacyLongFlags(root, os.Args[1:]))
	if err := root.Execute(); err != nil {
		if errors.Is(err, errNoCommand) {
			os.Exit(2)
		}
		if isUnknownCommand(err) {
			fmt.Fprintln(os.Stderr, err)
			_ = root.Help()
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// cmdWrangler runs a wrangler-style command against CellHive: translateWrangler
// maps the subcommand and common flags onto the native CLI, then dispatch runs
// it (ADR-138).
// uploadBundle hashes a local file, uploads it, and returns the content sha.
// registeredResources fetches the namespace's registered resources so the local
// preflight can reject unregistered bindings before deploy (no auto-provisioning).
func registeredResources(ns string) (func(kind, name string) bool, error) {
	set := map[string]bool{}
	data, err := adminCall(http.MethodGet, "/v1/control/resources?namespace="+url.QueryEscape(ns), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Resources []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode resource list: %w", err)
	}
	for _, r := range out.Resources {
		set[r.Kind+"|"+r.Name] = true
	}
	return func(kind, name string) bool { return set[kind+"|"+name] }, nil
}

// bundleFromConfig builds and uploads the config's `main` (esbuild in-process,
// --no-bundle uploads the file as-is).
func bundleFromConfig(m *wrangler.Deploy) (string, error) {
	if m.Main == "" {
		return uploadBundleBytes([]byte(`export default { fetch() { return new Response(null, { status: 404 }); } };`))
	}
	if m.NoBundle {
		return uploadBundle(m.Main)
	}
	out, err := buildBundle(m)
	if err != nil {
		return "", err
	}
	return uploadBundleBytes(out)
}

// buildBundle runs esbuild with a per-build temp OutFile so concurrent deploys
// never overwrite each other's bundle (a fixed name in os.TempDir() is shared).
func buildBundle(m *wrangler.Deploy) ([]byte, error) {
	rules := make([]bundler.Rule, 0, len(m.Rules))
	for _, r := range m.Rules {
		rules = append(rules, bundler.Rule{Type: r.Type, Globs: r.Globs})
	}
	tmpDir, err := os.MkdirTemp("", "cellhive-bundle-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	return bundler.Build(context.Background(), bundler.Options{
		EntryPoint: m.Main, OutFile: filepath.Join(tmpDir, "bundle.js"),
		Minify: m.Minify, KeepNames: m.KeepNames, NodeJSCompat: m.NodeCompat,
		Rules: rules, Define: m.Defines,
	})
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

// uploadAssetsDir uploads every file under dir under a single deterministic
// version token (sha256 of the sorted path+content list) and returns the token,
// which the version stores as assets_sha (the loader's asset token).
// assetsDirToken computes a deterministic version token over dir's files
// (sha256 of sorted "rel\0contentHash\n" lines) and returns it with the sorted
// relative paths.
func assetsDirToken(dir string) (string, []string, error) {
	type entry struct{ rel, sum string }
	var entries []entry
	patterns := readAssetsIgnore(dir)
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
		if rel == ".assetsignore" || assetIgnored(rel, patterns) {
			return nil
		}
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

func readAssetsIgnore(dir string) []string {
	raw, err := os.ReadFile(filepath.Join(dir, ".assetsignore"))
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

func assetIgnored(path string, patterns []string) bool {
	ignored := false
	for _, pattern := range patterns {
		neg := strings.HasPrefix(pattern, "!")
		if neg {
			pattern = strings.TrimPrefix(pattern, "!")
		}
		pattern = strings.TrimPrefix(strings.TrimSpace(pattern), "/")
		match := path == strings.TrimSuffix(pattern, "/") || strings.HasPrefix(path, strings.TrimSuffix(pattern, "/")+"/")
		if strings.HasSuffix(pattern, "/") {
			match = strings.HasPrefix(path, strings.TrimSuffix(pattern, "/")+"/")
		}
		if strings.ContainsAny(pattern, "*?") {
			match, _ = filepath.Match(pattern, path)
			if !match && !strings.Contains(pattern, "/") {
				match, _ = filepath.Match(pattern, filepath.Base(path))
			}
		}
		if match {
			ignored = !neg
		}
	}
	return ignored
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

// scopeCall mints a scoped binding token from the platform root key and calls a
// tenant binding endpoint on the internal listener (ADR-074/158).
func scopeCall(method, path, ns, index string, body any, extra ...url.Values) (json.RawMessage, error) {
	return scopeCallKind(method, path, ns, "vectorize", index, body, extra...)
}

// scopeCallKind is the shared data-plane client for operator commands. It
// mints a narrowly scoped token for exactly one namespace/resource pair.
func scopeCallKind(method, path, ns, kind, name string, body any, extra ...url.Values) (json.RawMessage, error) {
	creds := config.DeriveCredentials(config.LoadRootKey())
	if creds.Scope == "" {
		return nil, fmt.Errorf("CELLHIVE_ROOT_KEY is required to mint a scoped token")
	}
	token, err := scopedtoken.Mint([]byte(creds.Scope), scopedtoken.Claims{
		Namespace: ns, Kind: kind, Name: name,
		ExpiresMs: time.Now().Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	qs := url.Values{"ns": {ns}}
	if kind == "vectorize" {
		qs.Set("index", name)
	}
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
// Data-plane verbs use short-lived scoped tokens and the internal binding API.
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
	if kind == "kv" && verb == "key" {
		return dispatch(append([]string{"kv", "key"}, rest[1:]...))
	}
	if kind == "kv" && verb == "bulk" {
		return dispatch(append([]string{"kv", "bulk"}, rest[1:]...))
	}
	if kind == "d1" && (verb == "execute" || verb == "query") {
		return dispatch(append([]string{"d1", verb}, rest[1:]...))
	}
	if kind == "d1" && verb == "export" {
		return dispatch(append([]string{"d1", "export"}, rest[1:]...))
	}
	if kind == "r2" && verb == "object" {
		return dispatch(append([]string{"r2", "object"}, rest[1:]...))
	}
	if kind == "d1" && verb == "migrations" {
		return dispatch(append([]string{"d1", "migrations"}, rest[1:]...))
	}
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
			return fmt.Errorf("usage: cellhive %s stats <namespace> <name> [--prefix <p>] [--exact] [--tables] [--limit N] [--scan]", kind)
		}
		fs := flag.NewFlagSet(kind+" stats", flag.ContinueOnError)
		prefix := fs.String("prefix", "", "restrict count-style fields to a key prefix")
		exact := fs.Bool("exact", false, "force exact counts (may scan)")
		tables := fs.Bool("tables", false, "include per-table dbstat usage (D1)")
		limit := fs.Int("limit", 0, "max objects to inspect (R2)")
		scan := fs.Bool("scan", false, "scan R2 objects for diagnostic stats")
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
			if *scan {
				q.Set("scan", "1")
			}
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

func scopeRawCall(method, path, ns, kind, name string, raw []byte, q url.Values) ([]byte, error) {
	return scopeRawCallHeaders(method, path, ns, kind, name, raw, q, nil)
}

func scopeRawCallHeaders(method, path, ns, kind, name string, raw []byte, q url.Values, headers http.Header) ([]byte, error) {
	creds := config.DeriveCredentials(config.LoadRootKey())
	if creds.Scope == "" {
		return nil, fmt.Errorf("CELLHIVE_ROOT_KEY is required to mint a scoped token")
	}
	tok, err := scopedtoken.Mint([]byte(creds.Scope), scopedtoken.Claims{Namespace: ns, Kind: kind, Name: name, ExpiresMs: time.Now().Add(5 * time.Minute).UnixMilli()})
	if err != nil {
		return nil, err
	}
	if q == nil {
		q = url.Values{}
	}
	q.Set("ns", ns)
	req, err := http.NewRequest(method, internalURL()+path+"?"+q.Encode(), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-scope-token", tok)
	if method == http.MethodPost || method == http.MethodPut {
		req.Header.Set("content-type", "application/json")
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// cmdQueue is operator queue tooling: status and dead-letter replay (ADR-156).
func cmdQueue(args []string) error {
	return dispatch(append([]string{"queue"}, args...))
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
			if len(rest) < 4 {
				return fmt.Errorf("usage: cellhive versions view <namespace> <worker> <version-id>")
			}
			path := "/client/v4/accounts/" + url.PathEscape(rest[1]) + "/workers/scripts/" + url.PathEscape(rest[2]) + "/versions/" + url.PathEscape(rest[3])
			out, err := adminCall(http.MethodGet, path, nil)
			if err != nil {
				return err
			}
			printJSON(out)
			return nil
		default:
			return fmt.Errorf("usage: cellhive versions list <namespace> <worker>")
		}
	case "deployments":
		if first == "list" || first == "status" {
			if len(rest) < 3 {
				return fmt.Errorf("usage: cellhive deployments %s <namespace> <worker>", first)
			}
			if first == "status" {
				return cmdReleases(rest[1:])
			}
			path := "/client/v4/accounts/" + url.PathEscape(rest[1]) + "/workers/scripts/" + url.PathEscape(rest[2]) + "/deployments"
			out, err := adminCall(http.MethodGet, path, nil)
			if err != nil {
				return err
			}
			printJSON(out)
			return nil
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
		case "trigger":
			return cmdWorkflowTrigger(rest[1:])
		case "instances":
			return cmdWorkflowInstances(rest[1:])
		case "status", "describe":
			return fmt.Errorf("use `cellhive workflows instances describe <ns> <workflow> <id>` for instance status, or `cellhive workflows info <ns> <workflow>` for the definition")
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
			return cmdQueueConsumer(rest[1:])
		case "purge":
			return cmdQueuePurge(rest[1:])
		default:
			return fmt.Errorf("queues %s is not supported: use `cellhive queue create|list|delete|stats <ns> <name>` (or the `cellhive resource` generic form)", first)
		}
	case "types":
		return cmdTypes(rest)
	case "init":
		return cmdInit(rest)
	}
	return fmt.Errorf("unknown command: %s", cmd)
}

// workerCrons returns the cron expressions of a worker's active version (not
// the whole release history).
func workerCrons(ns, worker string) ([]string, error) {
	data, err := adminCall(http.MethodGet, "/v1/control/releases?namespace="+url.QueryEscape(ns)+"&worker="+url.QueryEscape(worker), nil)
	if err != nil {
		return nil, err
	}
	var rel struct {
		Releases []struct {
			Version int      `json:"version"`
			Active  bool     `json:"active"`
			Crons   []string `json:"crons"`
		} `json:"releases"`
	}
	if err := json.Unmarshal(data, &rel); err != nil {
		return nil, err
	}
	out := []string{}
	for _, r := range rel.Releases {
		if r.Active {
			out = append(out, r.Crons...)
			break
		}
	}
	return out, nil
}

// printWorkerCrons prints the cron expressions of a worker's active version.
func printWorkerCrons(ns, worker string) error {
	out, err := workerCrons(ns, worker)
	if err != nil {
		return err
	}
	enc, err := json.Marshal(map[string]any{"namespace": ns, "worker": worker, "crons": out})
	if err != nil {
		return err
	}
	printJSON(enc)
	return nil
}

// cmdCreds prints the role credentials derived from CELLHIVE_ROOT_KEY, so
// scripts and operators never hand-copy per-role secrets (ADR-137).
// mintScopeToken mints a scoped binding token from one place (ADR-181). keyB64
// (raw base64url) overrides root derivation so a delegated holder can mint
// without holding the root key.
func mintScopeToken(ns, kind, name, iss string, ttl time.Duration, keyB64 string) (string, error) {
	if ns == "" || kind == "" || name == "" {
		return "", fmt.Errorf("namespace, kind and name are required")
	}
	var key []byte
	if keyB64 != "" {
		b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(keyB64))
		if err != nil || len(b) == 0 {
			return "", fmt.Errorf("invalid key (want raw base64url)")
		}
		key = b
	} else {
		scope := config.DeriveCredentials(config.LoadRootKey()).Scope
		if scope == "" {
			return "", fmt.Errorf("CELLHIVE_ROOT_KEY is required (or pass --key)")
		}
		k, err := scopedtoken.IssuerKey([]byte(scope), iss)
		if err != nil {
			return "", err
		}
		key = k
	}
	var exp int64
	switch {
	case ttl > 0:
		exp = time.Now().Add(ttl).UnixMilli()
	case ttl < 0:
		return "", fmt.Errorf("ttl must be positive")
	case iss != "":
		return "", fmt.Errorf("delegated tokens (--iss) require --ttl > 0")
	}
	return scopedtoken.Mint(key, scopedtoken.Claims{
		Namespace: ns, Kind: kind, Name: name, Iss: iss, ExpiresMs: exp,
	})
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
