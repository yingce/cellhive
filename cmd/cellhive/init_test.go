package main

import (
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/wrangler"
	"cellhive/internal/wranglercompat"
)

func TestCmdInitCreatesDeployableProject(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := cmdInit([]string{"hello-worker", "--yes"}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join("hello-worker", "wrangler.jsonc")
	d, err := wrangler.Load(config, "")
	if err != nil {
		t.Fatal(err)
	}
	if d.Worker != "hello-worker" || d.CompatibilityDate != wranglercompat.MaxCompatibilityDate {
		t.Fatalf("generated config = %+v", d)
	}
	if _, err := os.Stat(d.Main); err != nil {
		t.Fatalf("generated main: %v", err)
	}
	if err := cmdInit([]string{"hello-worker"}); !os.IsExist(err) {
		t.Fatalf("existing project must not be overwritten: %v", err)
	}
}

func TestCmdInitRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", "../other", "Upper", "trailing-", "a/b"} {
		if workerProjectName(name) {
			t.Errorf("accepted project name %q", name)
		}
	}
}
