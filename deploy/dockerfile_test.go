package deploy_test

import (
	"os"
	"strings"
	"testing"
)

func TestDockerfileUsesWorkerdFork(t *testing.T) {
	b, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		"COPY --from=workerd-fork /workerd /usr/local/bin/workerd",
		"ARG ESBUILD_VERSION=0.28.2",
		"CELLHIVE_ESBUILD=/usr/local/bin/esbuild",
		"/out/esbuild --version",
		"/usr/share/licenses/cellhive/workerd/LICENSE",
		"/usr/share/licenses/cellhive/esbuild/LICENSE",
		"ESBUILD_INTEGRITY_SHA512",
		"sha512sum -c",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("Dockerfile missing %q", want)
		}
	}
	if strings.Contains(s, "ARG WORKERD_VERSION=") {
		t.Error("Dockerfile must not download a stock workerd tarball anymore")
	}
	b, err = os.ReadFile("build-workerd-fork.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "cellhive-v1.20260916.1-3") {
		t.Error("fork build script is not pinned to cellhive-v1.20260916.1-3")
	}
}
