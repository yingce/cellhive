// Package wrangler parses a wrangler configuration file (wrangler.jsonc/json)
// and maps it to a CellHive deploy request, so `cellhive deploy --config
// wrangler.jsonc` works for a Wrangler user without translating the config into
// flags (ADR-124).
//
// TOML is not parsed here (no third-party parser dependency): use jsonc/json, or
// `cellhive dev` which reads wrangler.toml for local development.
package wrangler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cellhive/internal/control"
	"cellhive/internal/wranglercompat"
)

// Rule is one `rules` entry, used when bundling.
type Rule struct {
	Type        string
	Globs       []string
	Fallthrough bool
}

// Deploy is the mapped deploy request plus the inputs the CLI needs to bundle and
// upload. It is the neutral form between a wrangler config and the control-plane
// deploy payload.
type Deploy struct {
	// Worker is the effective worker name (top-level `name`, or the selected
	// environment's `name`).
	Worker string
	// Main is the entry module (top-level `main`).
	Main string

	CompatibilityDate  string
	CompatibilityFlags []string
	Vars               map[string]string
	Bindings           []control.Binding
	Consumers          []control.Consumer
	Crons              []string
	Assets             *control.AssetsConfig
	// AssetsDir is the local asset directory to upload (empty = no assets).
	AssetsDir string
	// AssetsBinding is the assets binding name, if declared.
	AssetsBinding string
	Migrations    []map[string]any

	// Bundling inputs.
	NoBundle   bool
	NodeCompat bool
	Minify     bool
	KeepNames  bool
	Defines    map[string]string
	Rules      []Rule

	// Env is the selected `env.<name>` ("" = top level).
	Env string
	// UnknownFields are config keys the platform does not recognise.
	UnknownFields []string
}

// knownKeys are top-level (and env-level) configuration keys the platform
// recognises. Unknown keys fail the deploy (docs/wrangler-compat.md).
var knownKeys = map[string]bool{
	"name": true, "main": true, "compatibility_date": true, "compatibility_flags": true,
	"vars": true, "define": true, "minify": true, "keep_names": true, "rules": true,
	"no_bundle": true, "find_additional_modules": true, "base_dir": true,
	"preserve_file_names": true, "tsconfig": true, "build": true,
	"kv_namespaces": true, "d1_databases": true, "r2_buckets": true, "queues": true,
	"services": true, "workflows": true, "durable_objects": true, "migrations": true,
	"triggers": true, "assets": true, "site": true,
	"workers_dev": true, "route": true, "routes": true, "custom_domain": true, "preview_urls": true,
	"observability": true, "limits": true, "logpush": true, "tail_consumers": true,
	"placement": true, "account_id": true,
	"env": true, "ai": true, "analytics_engine_datasets": true,
	"python_workers": true, "cloudchamber": true, "mtls_certificates": true,
	"vectorize": true, "hyperdrive": true, "browser": true, "images": true,
	"send_email": true, "dispatch_namespaces": true, "secrets_store_secrets": true,
	"pipelines": true, "flagship": true, "containers": true, "artifacts": true,
	"vpc_services": true, "vpc_networks": true, "cache": true, "unsafe": true,
}

// rejectedKeys are recognised but not supported by the platform (mirrors
// cli/src/validate.ts and internal/wranglercompat). `ai` and `workflows` are
// supported (BYO AI / self-built engine) and therefore absent here.
var rejectedKeys = []string{
	"images", "browser", "send_email",
	"dispatch_namespaces", "secrets_store_secrets", "containers", "pipelines",
	"flagship", "vpc_services", "vpc_networks", "cache", "analytics_engine_datasets",
	"python_workers", "mtls_certificates", "artifacts", "cloudchamber",
}

// Load parses a wrangler config and maps it, applying `env.<envName>` overrides.
func Load(path, envName string) (*Deploy, error) {
	if strings.EqualFold(filepath.Ext(path), ".toml") {
		return nil, fmt.Errorf("wrangler.toml is not parsed by this CLI; use wrangler.jsonc/json (or `cellhive dev` for local TOML)")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(stripJSONC(raw), &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	d := &Deploy{Env: envName, Vars: map[string]string{}, Defines: map[string]string{}}

	base := m
	if envName != "" {
		envs, _ := m["env"].(map[string]any)
		e, ok := envs[envName].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("env.%s not found in %s", envName, path)
		}
		// Wrangler semantics: bindings are top-level only, everything else may be
		// overridden per environment. We therefore map the top-level scalars and
		// then overlay the environment object on top of the binding fields.
		base = mergeEnv(m, e)
	}
	d.unknown(base)
	d.rejected(base)
	d.scalars(base)
	d.mapBindings(base)
	d.mapBuild(base)
	d.mapAssets(base, path)
	d.mapMigrations(base)
	d.mapCrons(base)
	// Resolve `main` relative to the config file (wrangler does the same), so the
	// CLI can bundle/upload it regardless of the current directory.
	if d.Main != "" && !filepath.IsAbs(d.Main) {
		d.Main = filepath.Join(filepath.Dir(path), d.Main)
	}
	return d, nil
}

// mergeEnv overlays env.<name> on the top level, dropping top-level bindings
// (they are not inherited) and keeping env-level ones.
func mergeEnv(top, env map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range top {
		if k == "env" || isBindingKey(k) {
			continue
		}
		out[k] = v
	}
	for k, v := range env {
		out[k] = v
	}
	return out
}

func isBindingKey(k string) bool {
	switch k {
	case "kv_namespaces", "d1_databases", "r2_buckets", "queues", "services",
		"workflows", "durable_objects", "ai", "hyperdrive", "analytics_engine_datasets",
		"vectorize", "browser", "images", "send_email",
		"dispatch_namespaces", "secrets_store_secrets", "pipelines", "flagship",
		"containers", "mtls_certificates", "artifacts", "vpc_services", "vpc_networks", "cache":
		return true
	}
	return false
}

// UnknownFields returns the unrecognised keys (sorted, stable).
func (d *Deploy) unknown(m map[string]any) {
	for k := range m {
		if !knownKeys[k] {
			d.UnknownFields = append(d.UnknownFields, k)
		}
	}
	sortStrings(d.UnknownFields)
}

func (d *Deploy) rejected(m map[string]any) {
	for _, k := range rejectedKeys {
		if _, ok := m[k]; ok {
			// Bindings are also checked by type; this covers config sections like
			// `browser` that are not shaped as a binding list.
			d.UnknownFields = append(d.UnknownFields, k)
		}
	}
	sortStrings(d.UnknownFields)
}

func (d *Deploy) scalars(m map[string]any) {
	d.Worker, _ = m["name"].(string)
	d.Main, _ = m["main"].(string)
	d.CompatibilityDate, _ = m["compatibility_date"].(string)
	d.CompatibilityFlags = strList(m["compatibility_flags"])
	if vs, ok := m["vars"].(map[string]any); ok {
		for k, v := range vs {
			if s, ok := v.(string); ok {
				d.Vars[k] = s
				continue
			}
			d.Vars[k] = fmt.Sprint(v)
		}
	}
	if defs, ok := m["define"].(map[string]any); ok {
		for k, v := range defs {
			d.Defines[k] = fmt.Sprint(v)
		}
	}
}

func (d *Deploy) mapBuild(m map[string]any) {
	d.Minify, _ = m["minify"].(bool)
	d.KeepNames, _ = m["keep_names"].(bool)
	d.NoBundle, _ = m["no_bundle"].(bool)
	if t, ok := m["tsconfig"].(string); ok && t != "" {
		// Bundling always looks for tsconfig.json next to the entry; an explicit
		// path is not wired into esbuild options yet.
		_ = t
	}
	for _, r := range list(m["rules"]) {
		rule := Rule{Type: str(r["type"]), Fallthrough: boolOf(r["fallthrough"])}
		rule.Globs = strList(r["globs"])
		if rule.Type != "" && len(rule.Globs) > 0 {
			d.Rules = append(d.Rules, rule)
		}
	}
	for _, f := range strList(m["compatibility_flags"]) {
		if f == "nodejs_compat" || f == "nodejs_compat_v2" {
			d.NodeCompat = true
		}
	}
}

func (d *Deploy) mapBindings(m map[string]any) {
	for _, b := range list(m["kv_namespaces"]) {
		d.Bindings = append(d.Bindings, control.Binding{Type: "kv", Name: str(b["binding"]), ID: str(b["id"])})
	}
	for _, b := range list(m["d1_databases"]) {
		id := str(b["database_id"])
		if id == "" {
			id = str(b["database_name"])
		}
		d.Bindings = append(d.Bindings, control.Binding{Type: "d1", Name: str(b["binding"]), ID: id})
	}
	for _, b := range list(m["r2_buckets"]) {
		id := str(b["bucket_name"])
		if id == "" {
			id = str(b["bucket"])
		}
		d.Bindings = append(d.Bindings, control.Binding{Type: "r2", Name: str(b["binding"]), ID: id})
	}
	if q, ok := m["queues"].(map[string]any); ok {
		for _, p := range list(q["producers"]) {
			d.Bindings = append(d.Bindings, control.Binding{Type: "queue", Name: str(p["binding"]), ID: str(p["queue"])})
		}
		for _, c := range list(q["consumers"]) {
			d.Consumers = append(d.Consumers, control.Consumer{
				Queue:                  str(c["queue"]),
				MaxRetries:             intOf(c["max_retries"]),
				DeadLetterQueue:        str(c["dead_letter_queue"]),
				MaxBatchSize:           intOf(c["max_batch_size"]),
				MaxConcurrency:         intOf(c["max_concurrency"]),
				MaxBatchTimeoutSeconds: intOf(c["max_batch_timeout"]),
			})
		}
	}
	for _, s := range list(m["services"]) {
		d.Bindings = append(d.Bindings, control.Binding{
			Type: "service", Name: str(s["binding"]), ID: str(s["service"]), Entrypoint: str(s["entrypoint"]),
		})
	}
	for _, w := range list(m["workflows"]) {
		d.Bindings = append(d.Bindings, control.Binding{
			Type: "workflow", Name: str(w["binding"]), ID: str(w["name"]), ClassName: str(w["class_name"]),
		})
	}
	if do, ok := m["durable_objects"].(map[string]any); ok {
		for _, b := range list(do["bindings"]) {
			d.Bindings = append(d.Bindings, control.Binding{Type: "do", Name: str(b["name"]), ID: str(b["class_name"])})
		}
	}
	for _, v := range list(m["vectorize"]) {
		d.Bindings = append(d.Bindings, control.Binding{Type: "vectorize", Name: str(v["binding"]), ID: str(v["index_name"])})
	}
	for _, h := range list(m["hyperdrive"]) {
		d.Bindings = append(d.Bindings, control.Binding{Type: "hyperdrive", Name: str(h["binding"]), ID: str(h["id"])})
	}
	if ai, ok := m["ai"].(map[string]any); ok {
		if name := str(ai["binding"]); name != "" {
			d.Bindings = append(d.Bindings, control.Binding{Type: "ai", Name: name})
		}
	}
}

func (d *Deploy) mapCrons(m map[string]any) {
	if t, ok := m["triggers"].(map[string]any); ok {
		d.Crons = append(d.Crons, strList(t["crons"])...)
	}
}

func (d *Deploy) mapMigrations(m map[string]any) {
	for _, mig := range list(m["migrations"]) {
		out := map[string]any{}
		for k, v := range mig {
			out[k] = v
		}
		if len(out) > 0 {
			d.Migrations = append(d.Migrations, out)
		}
	}
}

func (d *Deploy) mapAssets(m map[string]any, configPath string) {
	a, ok := m["assets"].(map[string]any)
	if !ok {
		// Legacy [site] bucket.
		if site, ok := m["site"].(map[string]any); ok {
			if dir := str(site["bucket"]); dir != "" {
				d.AssetsDir = resolveDir(configPath, dir)
			}
		}
		return
	}
	if dir := str(a["directory"]); dir != "" {
		d.AssetsDir = resolveDir(configPath, dir)
	}
	d.AssetsBinding = str(a["binding"])
	cfg := &control.AssetsConfig{}
	changed := false
	if nf := str(a["not_found_handling"]); nf != "" {
		cfg.NotFoundHandling = nf
		changed = true
	}
	switch rw := a["run_worker_first"].(type) {
	case bool:
		cfg.RunWorkerFirst = rw
		changed = changed || rw
	case []any:
		cfg.RunWorkerFirstPaths = strList(rw)
		changed = changed || len(cfg.RunWorkerFirstPaths) > 0
	}
	if changed {
		d.Assets = cfg
	}
}

func resolveDir(configPath, dir string) string {
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(filepath.Dir(configPath), dir)
}

// Validate runs the same compatibility gate the server applies, plus the local
// checks (migrations, crons). BundleSHA is not known yet, so it is not checked.
func (d *Deploy) Validate() []wranglercompat.Finding { return d.ValidateWith(nil) }

// ValidateWith is Validate with a registered-resource callback, so a CLI can
// catch binding_unregistered before deploying.
func (d *Deploy) ValidateWith(registered func(kind, name string) bool) []wranglercompat.Finding {
	in := wranglercompat.Input{
		BundleSHA:          "(pending)",
		CompatibilityDate:  d.CompatibilityDate,
		CompatibilityFlags: d.CompatibilityFlags,
		UnknownFields:      d.UnknownFields,
		IsRegistered:       registered,
	}
	for _, b := range d.Bindings {
		in.Bindings = append(in.Bindings, wranglercompat.Binding{
			Type: b.Type, Name: b.Name, ID: b.ID, ClassName: b.ClassName, Entrypoint: b.Entrypoint,
		})
	}
	res := wranglercompat.Validate(in)
	out := res.Errors
	out = append(out, wranglercompat.ValidateMigrations(d.Migrations)...)
	out = append(out, wranglercompat.ValidateCrons(d.Crons)...)
	return out
}

// --- small helpers ----------------------------------------------------------

func list(v any) []map[string]any {
	items, _ := v.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func strList(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}

func intOf(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

// stripJSONC removes // and /* */ comments and trailing commas, honoring string
// literals, so wrangler.jsonc parses with encoding/json.
func stripJSONC(data []byte) []byte {
	out := make([]byte, 0, len(data))
	inStr := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inStr {
			out = append(out, c)
			if c == '\\' && i+1 < len(data) {
				i++
				out = append(out, data[i])
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch {
		case c == '"':
			inStr = true
			out = append(out, c)
		case c == '/' && i+1 < len(data) && data[i+1] == '/':
			for i < len(data) && data[i] != '\n' {
				i++
			}
			out = append(out, '\n')
		case c == '/' && i+1 < len(data) && data[i+1] == '*':
			i += 2
			for i+1 < len(data) && !(data[i] == '*' && data[i+1] == '/') {
				i++
			}
			i++
		case c == ',':
			// Drop a comma that only precedes a closing brace/bracket.
			j := i + 1
			for j < len(data) && (data[j] == ' ' || data[j] == '\t' || data[j] == '\n' || data[j] == '\r') {
				j++
			}
			if j < len(data) && (data[j] == '}' || data[j] == ']') {
				continue
			}
			out = append(out, c)
		default:
			out = append(out, c)
		}
	}
	return out
}
