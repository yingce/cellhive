package wranglercompat

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimeBaselineADRIsCurrent(t *testing.T) {
	b, err := os.ReadFile("../../docs/decisions.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		"ADR-186", "1.20260916.1", "esbuild 0.28.2",
		"fromEnvironment", "64 MiB", "1016 KiB",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("decisions missing %q", want)
		}
	}
}
