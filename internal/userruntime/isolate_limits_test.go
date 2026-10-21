package userruntime

import (
	"os"
	"strings"
	"testing"
)

// TestRenderIsolateLimits covers the deployment-configured warm-isolate
// bounds: defaults are fixed (128 / 256MiB), env-provided values override them.
func TestRenderIsolateLimits(t *testing.T) {
	base := Config{
		CellURL: "http://127.0.0.1:1", CellToken: "tok", DispatchToken: "tok",
		InternalPort: 18088, PublicPort: 18081,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	}
	dir := t.TempDir()
	path, err := Render(dir, base)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `(name = "CH_ISOLATE_MAX", text = "128")`) {
		t.Fatal("default resident max should be 128")
	}
	if !strings.Contains(text, `(name = "CH_ISOLATE_BYTES", text = "268435456")`) {
		t.Fatal("default resident bytes should be 256MiB")
	}

	dir = t.TempDir()
	path, err = Render(dir, Config{
		CellURL: "http://127.0.0.1:1", CellToken: "tok", DispatchToken: "tok",
		InternalPort: 18089, PublicPort: 18082,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		IsolateMax: 42,
	})
	if err != nil {
		t.Fatalf("render override: %v", err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text = string(raw)
	if !strings.Contains(text, `(name = "CH_ISOLATE_MAX", text = "42")`) {
		t.Fatal("resident max override was not rendered")
	}
	if !strings.Contains(text, `(name = "CH_ISOLATE_BYTES", text = "88080384")`) {
		t.Fatal("resident bytes should follow max × 2MiB when unset")
	}

	dir = t.TempDir()
	path, err = Render(dir, Config{
		CellURL: "http://127.0.0.1:1", CellToken: "tok", DispatchToken: "tok",
		InternalPort: 18090, PublicPort: 18083,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		IsolateMax: 7, IsolateBytes: 64 << 20,
	})
	if err != nil {
		t.Fatalf("render explicit bytes: %v", err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `(name = "CH_ISOLATE_BYTES", text = "67108864")`) {
		t.Fatal("explicit resident bytes were not rendered")
	}
}
