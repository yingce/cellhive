package deploy_test

import (
	"os"
	"strings"
	"testing"
)

func TestDockerfilePinsRuntimeTools(t *testing.T) {
	b, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		"ARG WORKERD_VERSION=1.20260916.1",
		"ARG ESBUILD_VERSION=0.28.2",
		"CELLHIVE_ESBUILD=/usr/local/bin/esbuild",
		"/out/esbuild --version",
		"/usr/share/licenses/cellhive/workerd/LICENSE",
		"/usr/share/licenses/cellhive/esbuild/LICENSE",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("Dockerfile missing %q", want)
		}
	}
	for _, want := range []string{
		"sha512sum -c",
		"WORKERD_INTEGRITY_SHA512",
		"ESBUILD_INTEGRITY_SHA512",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("Dockerfile missing integrity contract %q", want)
		}
	}
}
