package wrangler

import (
	"strings"
	"testing"

	"cellhive/internal/control"
)

func TestEnvTypesIncludesBindingsAndVars(t *testing.T) {
	d := &Deploy{
		Vars: map[string]string{"GREETING": "hello", "dashed-key": "value"},
		Bindings: []control.Binding{
			{Type: "kv", Name: "CACHE"}, {Type: "d1", Name: "DB"},
			{Type: "r2", Name: "FILES"}, {Type: "queue", Name: "JOBS"},
			{Type: "workflow", Name: "WF"}, {Type: "do", Name: "ROOM"},
			{Type: "service", Name: "API"}, {Type: "vectorize", Name: "VEC"},
		}, AssetsBinding: "ASSETS",
	}
	out, err := EnvTypes(d, "WorkerEnv")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"interface WorkerEnv {", "CACHE: KVNamespace;", "DB: D1Database;",
		"FILES: R2Bucket;", "JOBS: Queue;", "WF: Workflow;",
		"ROOM: DurableObjectNamespace;", "API: Fetcher;", "VEC: VectorizeIndex;",
		"ASSETS: Fetcher;", "GREETING: string;", `"dashed-key": string;`,
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("generated Env lacks %q:\n%s", want, out)
		}
	}
	if out2, err := EnvTypes(d, "WorkerEnv"); err != nil || string(out) != string(out2) {
		t.Fatal("type generation is not deterministic")
	}
}

func TestEnvTypesRejectsDuplicateNames(t *testing.T) {
	d := &Deploy{Vars: map[string]string{"CACHE": "x"}, Bindings: []control.Binding{{Type: "kv", Name: "CACHE"}}}
	if _, err := EnvTypes(d, "Env"); err == nil {
		t.Fatal("duplicate binding and var was accepted")
	}
}

func TestEnvTypesRejectsInvalidInterfaceName(t *testing.T) {
	for _, name := range []string{"", "bad-name", "default"} {
		if _, err := EnvTypes(&Deploy{}, name); err == nil {
			t.Errorf("invalid interface name %q was accepted", name)
		}
	}
}
