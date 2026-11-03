package deploy_test

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
)

func TestLocalAdminTokenIsShortLivedJWT(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is required by local-admin-token.sh")
	}
	script, err := os.ReadFile("compose/local-admin-token.sh")
	if err != nil {
		t.Fatal(err)
	}
	root := []byte("0123456789abcdef0123456789abcdef")
	key, err := bearertoken.DeriveKey(root, "token/admin")
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range []string{hex.EncodeToString(root), base64.StdEncoding.EncodeToString(root), base64.RawURLEncoding.EncodeToString(root), "invalid-root"} {
		t.Run(encoded, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "local-admin-token.sh")
			if err := os.WriteFile(path, script, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "cellhive.local.env"), []byte("CELLHIVE_ROOT_KEY="+encoded+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("bash", path).Output()
			if encoded == "invalid-root" {
				if err == nil || len(out) != 0 {
					t.Fatal("invalid root must fail without printing a credential")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			kid, claims, err := bearertoken.Verify(key, strings.TrimSpace(string(out)), time.Now())
			if err != nil {
				t.Fatalf("helper must emit a verifiable admin JWT, not a signing key: %v", err)
			}
			if kid != "token/admin" || claims.Type != "admin" || claims.ExpiresAt-claims.IssuedAt != 300 {
				t.Fatalf("unexpected JWT identity/lifetime: %s %+v", kid, claims)
			}
		})
	}
}

func TestDeploymentRendersKeepJWTConfiguration(t *testing.T) {
	for _, command := range [][]string{
		{"helm", "template", "jwt-test", "helm/cellhive", "--set", "existingSecret=synthetic-test-secret"},
		{"helm", "template", "jwt-test", "helm/cellhive", "--set", "existingSecret=synthetic-test-secret", "--set", "doRuntime.gate=false", "--set", "telemetry.enabled=true"},
		{"kubectl", "kustomize", "k8s"},
		{"kubectl", "kustomize", "k8s/overlays/rpo0"},
	} {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			if _, err := exec.LookPath(command[0]); err != nil {
				t.Skipf("%s not installed", command[0])
			}
			out, err := exec.Command(command[0], command[1:]...).CombinedOutput()
			if err != nil {
				t.Fatalf("offline render failed: %v\n%s", err, out)
			}
			rendered := string(out)
			for _, retired := range []string{"8083", "CELLHIVE_ADMIN_TOKEN", "CELLHIVE_OIDC_", "x-cellhive-internal-token", "x-cellhive-admin-token"} {
				if strings.Contains(rendered, retired) {
					t.Errorf("render includes retired config %q", retired)
				}
			}
			if !strings.Contains(rendered, "CELLHIVE_ROOT_KEY") {
				t.Error("trusted process root-key configuration must remain")
			}
		})
	}
}
