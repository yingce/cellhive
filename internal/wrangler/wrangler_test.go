package wrangler

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

// TestLoadJSONCAndMapping covers jsonc parsing (comments + trailing commas),
// binding mapping, build/rules, assets and migrations.
func TestLoadJSONCAndMapping(t *testing.T) {
	p := write(t, "wrangler.jsonc", `{
  // a comment
  "name": "api",
  "main": "src/index.js",
  "compatibility_date": "2026-06-22",
  "compatibility_flags": ["nodejs_compat"],
  "vars": { "MODE": "prod" },
  "kv_namespaces": [{ "binding": "KV", "id": "acme/__kv__/main" }],
  "d1_databases": [{ "binding": "DB", "database_name": "mydb" }],
  "r2_buckets": [{ "binding": "BUCKET", "bucket_name": "files" }],
  "queues": {
    "producers": [{ "binding": "JOBS", "queue": "jobs" }],
    "consumers": [{ "queue": "jobs", "max_retries": 3, "dead_letter_queue": "dlq", "max_concurrency": 4 }]
  },
  "services": [{ "binding": "SVC", "service": "other", "entrypoint": "fetch" }],
  "workflows": [{ "binding": "WF", "name": "my-wf", "class_name": "MyWorkflow" }],
  "durable_objects": { "bindings": [{ "name": "ROOMS", "class_name": "Room" }] },
  "hyperdrive": [{ "binding": "DB", "id": "postgres://u:p@origin.example.com:5432/app" }],
  "triggers": { "crons": ["*/5 * * * *"] },
  "assets": { "directory": "public", "not_found_handling": "single-page-application", "run_worker_first": ["/api/*"] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Room"] }],
  "rules": [{ "type": "Text", "globs": ["**/*.txt"] }],
  /* block comment */
  "unknown_thing": 1,
}`)
	d, err := Load(p, "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if d.Worker != "api" || filepath.Base(d.Main) != "index.js" || d.CompatibilityDate != "2026-06-22" {
		t.Fatalf("scalars = %+v", d)
	}
	if !d.NodeCompat || d.Vars["MODE"] != "prod" {
		t.Fatalf("node compat/vars = %v %v", d.NodeCompat, d.Vars)
	}
	want := map[string]string{
		"kv|KV": "acme/__kv__/main", "d1|DB": "mydb", "r2|BUCKET": "files",
		"queue|JOBS": "jobs", "service|SVC": "other", "workflow|WF": "my-wf", "do|ROOMS": "Room",
		"hyperdrive|DB": "postgres://u:p@origin.example.com:5432/app",
	}
	for _, b := range d.Bindings {
		k := b.Type + "|" + b.Name
		if got, ok := want[k]; !ok || got != b.ID {
			t.Fatalf("binding %s = %q, want %q (want map %v)", k, b.ID, want[k], want)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Fatalf("missing bindings: %v", want)
	}
	if len(d.Consumers) != 1 || d.Consumers[0].MaxRetries != 3 || d.Consumers[0].MaxConcurrency != 4 || d.Consumers[0].DeadLetterQueue != "dlq" {
		t.Fatalf("consumers = %+v", d.Consumers)
	}
	if len(d.Crons) != 1 || d.Crons[0] != "*/5 * * * *" {
		t.Fatalf("crons = %v", d.Crons)
	}
	if d.Assets == nil || d.Assets.NotFoundHandling != "single-page-application" || len(d.Assets.RunWorkerFirstPaths) != 1 {
		t.Fatalf("assets = %+v", d.Assets)
	}
	if d.AssetsDir == "" || filepath.Base(d.AssetsDir) != "public" {
		t.Fatalf("assets dir = %q", d.AssetsDir)
	}
	if len(d.Migrations) != 1 || d.Rules == nil {
		t.Fatalf("migrations/rules = %v %v", d.Migrations, d.Rules)
	}

	// hyperdrive is a supported binding (ADR-129) and must not be reported as an
	// unsupported/unknown field by the local gate.
	for _, f := range d.UnknownFields {
		if f == "hyperdrive" {
			t.Fatal("hyperdrive binding was rejected as an unknown field")
		}
	}
	// Unknown keys fail the local gate; the server re-validates too.
	findings := d.Validate()
	if len(findings) == 0 {
		t.Fatal("unknown field did not fail validation")
	}
	found := false
	for _, f := range findings {
		if f.Code == "unknown_field" {
			found = true
		}
	}
	if !found {
		t.Fatalf("findings = %+v, want unknown_field", findings)
	}
}

// TestEnvInheritance covers wrangler's env.<name> semantics: bindings are not
// inherited, other keys are.
func TestEnvInheritance(t *testing.T) {
	p := write(t, "wrangler.jsonc", `{
  "name": "api",
  "main": "src/index.js",
  "compatibility_date": "2026-06-01",
  "vars": { "MODE": "prod" },
  "kv_namespaces": [{ "binding": "TOP", "id": "top" }],
  "env": {
    "staging": {
      "name": "api-staging",
      "vars": { "MODE": "stage" },
      "kv_namespaces": [{ "binding": "STAGE", "id": "stage" }]
    }
  },
}`)
	d, err := Load(p, "staging")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if d.Worker != "api-staging" || filepath.Base(d.Main) != "index.js" || d.CompatibilityDate != "2026-06-01" {
		t.Fatalf("env scalars = %+v", d)
	}
	if d.Vars["MODE"] != "stage" {
		t.Fatalf("env vars = %v", d.Vars)
	}
	if len(d.Bindings) != 1 || d.Bindings[0].Name != "STAGE" {
		t.Fatalf("env bindings = %+v (top-level bindings must not be inherited)", d.Bindings)
	}
	if _, err := Load(p, "prod"); err == nil {
		t.Fatal("missing env did not error")
	}
}

// TestTOMLRejected: this CLI parses jsonc/json only.
func TestTOMLRejected(t *testing.T) {
	p := write(t, "wrangler.toml", "name = \"api\"\n")
	if _, err := Load(p, ""); err == nil {
		t.Fatal("toml was accepted")
	}
}

// TestRejectedBindingSection covers a recognised-but-unsupported section.
func TestRejectedBindingSection(t *testing.T) {
	p := write(t, "wrangler.jsonc", `{ "name": "api", "main": "i.js", "browser": { "binding": "B" } }`)
	d, err := Load(p, "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := false
	for _, f := range d.Validate() {
		if f.Code == "unknown_field" && f.FieldPath == "browser" {
			found = true
		}
	}
	if !found {
		t.Fatalf("browser section not rejected: %+v", d.UnknownFields)
	}
}
