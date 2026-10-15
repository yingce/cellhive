package wrangler

import (
	"encoding/json"
	"testing"
)

func TestStaticAssetsConfigPreservesRuntimeOptions(t *testing.T) {
	p := write(t, "wrangler.jsonc", `{"name":"web","compatibility_date":"2026-09-01","assets":{"directory":"dist","binding":"FILES","html_handling":"none","run_worker_first":["/api/*","!/api/docs/*"]}}`)
	d, err := Load(p, "")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(d.Assets)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["binding"] != "FILES" || cfg["html_handling"] != "none" {
		t.Fatalf("runtime options lost: %s", raw)
	}
}

func TestStaticAssetsConfigRejectsInvalidHTMLHandling(t *testing.T) {
	p := write(t, "wrangler.jsonc", `{"name":"web","assets":{"directory":"dist","html_handling":"bad"}}`)
	if _, err := Load(p, ""); err == nil {
		t.Fatal("invalid html_handling accepted")
	}
}
