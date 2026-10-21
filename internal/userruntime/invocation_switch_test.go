package userruntime

import (
	"os"
	"strings"
	"testing"
)

// TestRenderInvocationLoggingSwitch covers ADR-239: Queue/Cron/Workflow
// invocation records are a node-wide switch, not a per-version policy.
func TestRenderInvocationLoggingSwitch(t *testing.T) {
	for _, tc := range []struct {
		name string
		on   bool
		want string
	}{
		{"on", true, `(name = "CH_INVOCATION_LOGS", text = "1")`},
		{"off", false, `(name = "CH_INVOCATION_LOGS", text = "0")`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			internalPort, publicPort := freePort(t), freePort(t)
			dir := t.TempDir()
			path, err := Render(dir, Config{
				CellURL: "http://127.0.0.1:1", CellToken: "tok", DispatchToken: "tok",
				InternalPort: internalPort, PublicPort: publicPort,
				PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
				InvocationLogging: tc.on,
			})
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), tc.want) {
				t.Fatalf("rendered config missing %s", tc.want)
			}
		})
	}
}
