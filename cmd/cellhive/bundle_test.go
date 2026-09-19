package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"cellhive/internal/bundler"
	"cellhive/internal/wrangler"
)

// TestBuildBundleConcurrentTempIsolation is the regression for the fixed shared
// OutFile in os.TempDir(): concurrent deploys overwrote each other's bundle.
func TestBuildBundleConcurrentTempIsolation(t *testing.T) {
	if _, err := bundler.FindEsbuild(); err != nil {
		t.Skipf("esbuild unavailable: %v", err)
	}
	dir := t.TempDir()
	write := func(name, marker string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("export default \""+marker+"\";\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	entries := []struct{ path, marker string }{
		{write("a.js", "MARKER_A"), "MARKER_A"},
		{write("b.js", "MARKER_B"), "MARKER_B"},
	}
	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n*len(entries))
	for i := 0; i < n; i++ {
		for j, e := range entries {
			wg.Add(1)
			go func(idx int, entry, marker string) {
				defer wg.Done()
				out, err := buildBundle(&wrangler.Deploy{Main: entry})
				if err != nil {
					errs[idx] = err
					return
				}
				if !strings.Contains(string(out), marker) {
					errs[idx] = fmt.Errorf("bundle missing %s", marker)
				}
			}(i*len(entries)+j, e.path, e.marker)
		}
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("build %d: %v", i, err)
		}
	}
}
