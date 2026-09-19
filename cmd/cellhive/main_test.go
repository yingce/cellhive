package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildBindings(t *testing.T) {
	out := buildBindings(
		[]string{"KV=acme/__kv__/main", "bad"},
		[]string{"DB=acme/db"},
		[]string{"BUCKET=acme/files"},
		[]string{"JOBS=acme/jobs"},
		[]string{"SVC=api"},
		[]string{"WF=my-wf:MyWorkflow"},
		[]string{"DB=postgres://u:p@origin.example.com:5432/app"},
	)
	if len(out) != 7 {
		t.Fatalf("bindings = %+v", out)
	}
	find := func(typ, name string) map[string]any {
		for _, b := range out {
			if b["type"] == typ && b["name"] == name {
				return b
			}
		}
		return nil
	}
	if b := find("kv", "KV"); b == nil || b["id"] != "acme/__kv__/main" {
		t.Fatalf("kv = %+v", b)
	}
	if b := find("service", "SVC"); b == nil || b["id"] != "api" {
		t.Fatalf("service = %+v", b)
	}
	if b := find("workflow", "WF"); b == nil || b["id"] != "my-wf" || b["class_name"] != "MyWorkflow" {
		t.Fatalf("workflow = %+v", b)
	}
	if b := find("hyperdrive", "DB"); b == nil || b["id"] != "postgres://u:p@origin.example.com:5432/app" {
		t.Fatalf("hyperdrive = %+v", b)
	}
	// Malformed entries are skipped, not fatal.
	if find("kv", "bad") != nil {
		t.Fatal("malformed kv entry accepted")
	}
}

func TestAssetsDirTokenDeterministic(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(dir, "index.html"), []byte("<h1>hi</h1>"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "sub", "a.css"), []byte("body{}"), 0o644)
	tok1, rels, err := assetsDirToken(dir)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	if len(rels) != 2 || rels[0] != "index.html" || rels[1] != "sub/a.css" {
		t.Fatalf("rels = %v", rels)
	}
	tok2, _, _ := assetsDirToken(dir)
	if tok1 != tok2 || len(tok1) != 64 {
		t.Fatalf("token not deterministic: %s vs %s", tok1, tok2)
	}
	_ = os.WriteFile(filepath.Join(dir, "extra.txt"), []byte("x"), 0o644)
	tok3, _, _ := assetsDirToken(dir)
	if tok3 == tok1 {
		t.Fatal("token did not change after adding a file")
	}
}

// TestConsumeSpecs covers --consumer parsing, including the maxConcurrency
// field (ADR-112).
func TestConsumeSpecs(t *testing.T) {
	got := consumeSpecs([]string{"jobs", "jobs:3", "jobs:3:dlq", "jobs:3:dlq:4", "jobs::dlq:0"})
	if len(got) != 5 {
		t.Fatalf("specs = %+v", got)
	}
	if c := got[0]; c["queue"] != "jobs" || c["max_retries"] != nil || c["max_concurrency"] != nil {
		t.Fatalf("plain = %+v", c)
	}
	if c := got[1]; c["max_retries"] != 3 || c["max_concurrency"] != nil {
		t.Fatalf("retries = %+v", c)
	}
	if c := got[2]; c["dead_letter_queue"] != "dlq" {
		t.Fatalf("dlq = %+v", c)
	}
	if c := got[3]; c["max_concurrency"] != 4 || c["dead_letter_queue"] != "dlq" || c["max_retries"] != 3 {
		t.Fatalf("full = %+v", c)
	}
	if c := got[4]; c["max_concurrency"] != 0 {
		t.Fatalf("zero concurrency = %+v", c)
	}
}

// TestCLISubcommandArity: missing positional arguments must return a usage error,
// never panic (ADR-135).
func TestCLISubcommandArity(t *testing.T) {
	cases := []struct {
		name string
		run  func() error
	}{
		{"domain add", func() error { return cmdDomain([]string{"add", "acme"}) }},
		{"domain rm", func() error { return cmdDomain([]string{"rm", "acme"}) }},
		{"domain ls", func() error { return cmdDomain([]string{"ls"}) }},
		{"route rm", func() error { return cmdRoute([]string{"rm", "acme"}) }},
		{"route add", func() error { return cmdRoute([]string{"add", "acme", "host"}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err == nil {
				t.Fatalf("%s with missing args returned nil, want a usage error", tc.name)
			}
		})
	}
}

// TestCmdCreds covers the root-derived credential printer (ADR-137).
func TestCmdCreds(t *testing.T) {
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "")
	if err := cmdCreds([]string{"internal"}); err == nil {
		t.Fatal("creds without a root key should fail")
	}
	if err := cmdCreds([]string{"a", "b"}); err == nil {
		t.Fatal("creds with too many args should fail")
	}
	t.Setenv("CELLHIVE_ROOT_KEY", "00112233445566778899aabbccddeeff")
	if err := cmdCreds([]string{"bogus"}); err == nil {
		t.Fatal("unknown role should fail")
	}
	if err := cmdCreds([]string{"internal"}); err != nil {
		t.Fatalf("creds internal: %v", err)
	}
	if err := cmdCreds(nil); err != nil {
		t.Fatalf("creds all: %v", err)
	}
}

// TestTranslateWrangler covers the wrangler-style alias (ADR-138).
func TestTranslateWrangler(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{"deploy discovers config", []string{"deploy", "--namespace", "acme"},
			[]string{"deploy", "acme", "-", "--config", "wrangler.jsonc"}},
		{"deploy name and env", []string{"deploy", "--namespace=acme", "-n", "api", "-e", "staging"},
			[]string{"deploy", "acme", "api", "--config", "wrangler.jsonc", "--env", "staging"}},
		{"delete maps to worker delete", []string{"delete", "acme", "api"},
			[]string{"worker", "delete", "acme", "api"}},
		{"secret passes through", []string{"secret", "get", "acme", "api", "TOKEN"},
			[]string{"secret", "get", "acme", "api", "TOKEN"}},
		{"versions list goes to the compat group", []string{"versions", "list", "acme", "api"},
			[]string{"versions", "list", "acme", "api"}},
		{"deployments list goes to the compat group", []string{"deployments", "list", "acme", "api"},
			[]string{"deployments", "list", "acme", "api"}},
		{"tail passes through", []string{"tail", "--worker", "acme/api"},
			[]string{"tail", "--worker", "acme/api"}},
	} {
		got, err := translateWrangler(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if strings.Join(got, " ") != strings.Join(tc.want, " ") {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}

	for _, tc := range []struct {
		name string
		in   []string
	}{
		{"unsupported deploy flag", []string{"deploy", "--namespace", "acme", "--minify"}},
		{"unsupported deploy flag with =", []string{"deploy", "--namespace", "acme", "--minify=true"}},
		{"missing namespace", []string{"deploy"}},
		{"unsupported command", []string{"pages", "deploy"}},
		{"unsupported compat command", []string{"pages", "deploy"}},
		{"unknown command", []string{"nope"}},
	} {
		if _, err := translateWrangler(tc.in); err == nil {
			t.Errorf("%s: expected an error", tc.name)
		}
	}
}

// TestTranslateWranglerExplicitConfigSkipsDiscovery: -c wins over discovery, and
// a missing config plus no bundle is a clear error rather than a usage dump.
func TestTranslateWranglerExplicitConfig(t *testing.T) {
	t.Chdir(t.TempDir())
	got, err := translateWrangler([]string{"deploy", "--namespace", "acme", "-c", "custom.jsonc"})
	if err != nil {
		t.Fatal(err)
	}
	want := "deploy acme - --config custom.jsonc"
	if strings.Join(got, " ") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
	if _, err := translateWrangler([]string{"deploy", "--namespace", "acme"}); err == nil {
		t.Fatal("no config and no bundle should error")
	}
}

func TestDiscoverWranglerConfig(t *testing.T) {
	dir := t.TempDir()
	if got := discoverWranglerConfig(dir); got != "" {
		t.Fatalf("empty dir = %q, want empty", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "wrangler.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := discoverWranglerConfig(dir); !strings.HasSuffix(got, "wrangler.json") {
		t.Fatalf("json discovery = %q", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := discoverWranglerConfig(dir); !strings.HasSuffix(got, "wrangler.jsonc") {
		t.Fatalf("jsonc should be preferred, got %q", got)
	}
}

// TestTranslateWranglerCompat: the wrangler prefix passes the now-supported
// deploy flags through and forwards compat subcommands (ADR-148).
func TestTranslateWranglerCompat(t *testing.T) {
	t.Chdir(t.TempDir())
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	got, err := translateWrangler([]string{"deploy", "--namespace", "acme", "--dry-run", "--var", "A=1", "--var=B=2", "--secrets-file", "s.json"})
	if err != nil {
		t.Fatal(err)
	}
	want := "deploy acme - --dry-run --var A=1 --var B=2 --secrets-file s.json --config wrangler.jsonc"
	if strings.Join(got, " ") != want {
		t.Fatalf("got %v\nwant %s", got, want)
	}
	for _, tc := range []struct{ in, want string }{
		{"versions list acme api", "versions list acme api"},
		{"d1 execute acme main --command SELECT", "d1 execute acme main --command SELECT"},
		{"d1 list acme", "d1 list acme"},
		{"r2 bucket list acme", "r2 bucket list acme"},
		{"kv key list", "kv key list"},
	} {
		got, err := translateWrangler(strings.Fields(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if strings.Join(got, " ") != tc.want {
			t.Fatalf("%s -> %v, want %s", tc.in, got, tc.want)
		}
	}
}

// TestCmdCompatRejections: unsupported wrangler commands answer with a
// structured message and an alternative (ADR-148).
func TestCmdCompatRejections(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"d1", "execute"}, "not supported"},
		{[]string{"kv", "key", "list"}, "not supported"},
		{[]string{"r2", "object", "put"}, "not supported"},
		{[]string{"types"}, "not supported"},
		{[]string{"init"}, "not supported"},
		{[]string{"versions", "upload"}, "publish atomically"},
		{[]string{"versions", "view", "3"}, "use `cellhive releases"},
		{[]string{"queues", "purge", "acme", "jobs"}, "not supported"},
	} {
		// Route through dispatch: kv/d1/r2 are real per-domain commands now
		// (ADR-157) and only their data-plane verbs are rejected.
		err := dispatch(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("dispatch(%v) = %v, want an error containing %q", tc.args, err, tc.want)
		}
	}
}

// TestWranglerDataPlaneRejections: `cellhive wrangler d1|r2|kv` must reach the
// native per-domain command (ADR-157) and get a structured rejection for
// data-plane verbs, not "unknown wrangler command" (ADR-148).
func TestWranglerDataPlaneRejections(t *testing.T) {
	for _, in := range [][]string{
		{"d1", "execute", "acme", "main"},
		{"r2", "object", "put"},
		{"kv", "key", "list"},
	} {
		err := cmdWrangler(in)
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("cmdWrangler(%v) = %v, want a structured not-supported error", in, err)
		}
		if strings.Contains(fmt.Sprint(err), "unknown wrangler command") {
			t.Errorf("cmdWrangler(%v) fell through to unknown command: %v", in, err)
		}
	}
}

// TestWranglerTriggersDeploy points at the atomic deploy instead of a generic
// usage error.
func TestWranglerTriggersDeploy(t *testing.T) {
	err := cmdWrangler([]string{"triggers", "deploy"})
	if err == nil || !strings.Contains(err.Error(), "not needed") {
		t.Fatalf("triggers deploy = %v", err)
	}
}

// TestWranglerKnownCommandsHaveOutcomes: every wrangler subcommand CellHive
// knows about must either map to a native command or return a structured
// rejection — never "unknown wrangler command" (ADR-148).
func TestWranglerKnownCommandsHaveOutcomes(t *testing.T) {
	for _, sub := range []string{
		"deploy", "delete", "secret", "tail", "rollback", "promote",
		"versions", "deployments", "triggers", "workflows", "queues", "types", "init",
		"d1", "r2", "kv", "kv:key", "kv:namespace", "kv:bulk", "vectorize", "hyperdrive",
		"dev", "pages", "dispatch", "login", "logout", "whoami", "containers", "pubsub",
		"mtls-certificate", "cert", "check", "docs", "telemetry", "unstable_foo",
	} {
		_, err := translateWrangler([]string{sub})
		if err != nil && strings.Contains(err.Error(), "unknown wrangler command") {
			t.Errorf("%s fell through to unknown command: %v", sub, err)
		}
	}
}

// TestWranglerRejectionsAreActionable pins the alternatives for the commands
// we deliberately do not support.
func TestWranglerRejectionsAreActionable(t *testing.T) {
	for _, tc := range []struct{ sub, want string }{
		{"check", "deploy --dry-run"},
		{"telemetry", "no usage telemetry"},
		{"logout", "no Cloudflare account"},
		{"cert", "operator-managed"},
		{"pages", "assets"},
		{"unstable_dev", "unstable/internal"},
	} {
		_, err := translateWrangler([]string{tc.sub})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s = %v, want an error containing %q", tc.sub, err, tc.want)
		}
	}
	// Subcommand-level messages.
	if err := cmdCompat([]string{"queues", "consumer", "add"}); err == nil || !strings.Contains(err.Error(), "queues.consumers") {
		t.Errorf("queues consumer = %v", err)
	}
	if err := cmdCompat([]string{"workflows", "status", "acme", "w"}); err == nil || !strings.Contains(err.Error(), "runtime-only") {
		t.Errorf("workflows status = %v", err)
	}
}
