package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type skillFixture struct {
	version string
	files   map[string][]byte
	sha     map[string]string
}

func newSkillFixture(t *testing.T, version string) *skillFixture {
	t.Helper()
	f := &skillFixture{
		version: version,
		files: map[string][]byte{
			"SKILL.md":                []byte("# CellHive\n\nRead https://cellhive.example.com/ai before acting.\n"),
			"agents/openai.yaml":      []byte("name: cellhive\n"),
			"references/catalog.json": []byte(`{"version":"` + version + `"}`),
		},
		sha: map[string]string{},
	}
	for name, data := range f.files {
		sum := sha256.Sum256(data)
		f.sha[name] = hex.EncodeToString(sum[:])
	}
	return f
}

func (f *skillFixture) manifest() map[string]any {
	return map[string]any{
		"name":       "cellhive",
		"version":    f.version,
		"baseUrl":    skillPlaceholderBase,
		"entrypoint": "SKILL.md",
		"files":      []string{"manifest.json", "SKILL.md", "agents/openai.yaml", "references/catalog.json"},
		"sha256":     f.sha,
	}
}

func (f *skillFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	catalog, err := json.Marshal(map[string]any{
		"name":    "cellhive-skills",
		"version": f.version,
		"skills":  []any{map[string]any{"name": "cellhive", "manifest": "cellhive/manifest.json"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(f.manifest())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/skills/manifest.json":
			_, _ = w.Write(catalog)
		case r.URL.Path == "/skills/cellhive/manifest.json":
			_, _ = w.Write(manifest)
		case strings.HasPrefix(r.URL.Path, "/skills/cellhive/"):
			name := strings.TrimPrefix(r.URL.Path, "/skills/cellhive/")
			data, ok := f.files[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestUpdateCellhiveSkillInstallsAndRewritesBase(t *testing.T) {
	ctx := context.Background()
	f := newSkillFixture(t, "2026.11.01.01")
	srv := f.server(t)
	root := filepath.Join(t.TempDir(), "skills")
	if err := updateCellhiveSkill(ctx, root, srv.URL, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "cellhive", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), skillPlaceholderBase) {
		t.Fatalf("skill base URL was not rewritten: %s", got)
	}
	if !strings.Contains(string(got), srv.URL) {
		t.Fatalf("skill does not contain %s: %s", srv.URL, got)
	}
	version, err := os.ReadFile(filepath.Join(root, ".cellhive-skills-version"))
	if err != nil || strings.TrimSpace(string(version)) != f.version {
		t.Fatalf("version = %q, %v", version, err)
	}
}

func TestUpdateCellhiveSkillChecksumMismatchKeepsExisting(t *testing.T) {
	ctx := context.Background()
	v1 := newSkillFixture(t, "2026.11.01.01")
	srv1 := v1.server(t)
	root := filepath.Join(t.TempDir(), "skills")
	if err := updateCellhiveSkill(ctx, root, srv1.URL, false); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(root, "cellhive", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	v2 := newSkillFixture(t, "2026.11.01.02")
	srv2 := v2.server(t)
	v2.files["SKILL.md"] = []byte("tampered")
	if err := updateCellhiveSkill(ctx, root, srv2.URL, true); err == nil {
		t.Fatal("update succeeded with a bad checksum")
	}
	got, err := os.ReadFile(filepath.Join(root, "cellhive", "SKILL.md"))
	if err != nil || string(got) != string(before) {
		t.Fatalf("existing skill was not preserved: %q %v", got, err)
	}
}

func TestUpdateCellhiveSkillRestoresPreviousOnSwapFailure(t *testing.T) {
	ctx := context.Background()
	v1 := newSkillFixture(t, "2026.11.01.01")
	root := filepath.Join(t.TempDir(), "skills")
	if err := updateCellhiveSkill(ctx, root, v1.server(t).URL, false); err != nil {
		t.Fatal(err)
	}
	v2 := newSkillFixture(t, "2026.11.01.02")
	srv2 := v2.server(t)

	oldRename := renameSkill
	defer func() { renameSkill = oldRename }()
	calls := 0
	renameSkill = func(from, to string) error {
		calls++
		if calls == 2 {
			return errors.New("injected rename failure")
		}
		return os.Rename(from, to)
	}
	if err := updateCellhiveSkill(ctx, root, srv2.URL, true); err == nil {
		t.Fatal("update succeeded despite rename failure")
	}
	got, err := os.ReadFile(filepath.Join(root, "cellhive", "SKILL.md"))
	if err != nil {
		t.Fatalf("previous skill was removed: %v", err)
	}
	if !strings.Contains(string(got), v1.version) && !strings.Contains(string(got), "CellHive") {
		t.Fatalf("previous skill was not restored: %s", got)
	}
}

func TestUpdateCellhiveSkillRejectsTraversal(t *testing.T) {
	ctx := context.Background()
	f := newSkillFixture(t, "2026.11.01.01")
	f.files["SKILL.md"] = []byte("ok")
	m := f.manifest()
	m["files"] = []string{"SKILL.md", "../escape"}
	f.files["../escape"] = []byte("nope")
	manifest, _ := json.Marshal(m)
	catalog, _ := json.Marshal(map[string]any{"version": f.version, "skills": []any{map[string]any{"name": "cellhive", "manifest": "cellhive/manifest.json"}}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/skills/manifest.json":
			_, _ = w.Write(catalog)
		case "/skills/cellhive/manifest.json":
			_, _ = w.Write(manifest)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	if err := updateCellhiveSkill(ctx, filepath.Join(t.TempDir(), "skills"), srv.URL, false); err == nil {
		t.Fatal("traversal path was accepted")
	}
}

func TestUpdateCellhiveSkillEnforcesFileLimit(t *testing.T) {
	ctx := context.Background()
	f := newSkillFixture(t, "2026.11.01.01")
	f.files["SKILL.md"] = make([]byte, skillMaxFileBytes+1)
	srv := f.server(t)
	if err := updateCellhiveSkill(ctx, filepath.Join(t.TempDir(), "skills"), srv.URL, false); err == nil {
		t.Fatal("oversized skill file was accepted")
	}
}

func TestResolveSkillBase(t *testing.T) {
	t.Setenv("CELLHIVE_SKILL_BASE_URL", "")
	if _, err := resolveSkillBase(""); err == nil {
		t.Fatal("empty base URL accepted")
	}
	if _, err := resolveSkillBase("ftp://docs.example.com"); err == nil {
		t.Fatal("non-http base URL accepted")
	}
	got, err := resolveSkillBase("https://docs.example.com/")
	if err != nil || got != "https://docs.example.com" {
		t.Fatalf("base = %q, %v", got, err)
	}
}

func TestValidSkillPath(t *testing.T) {
	for _, p := range []string{"SKILL.md", "agents/openai.yaml", "references/catalog.json"} {
		if !validSkillPath(p) {
			t.Errorf("%q rejected", p)
		}
	}
	for _, p := range []string{"", ".", "..", "/abs", "../x", "a/../../b", "a\\b", "./a", "a//b"} {
		if validSkillPath(p) {
			t.Errorf("%q accepted", p)
		}
	}
}

func TestSkillFixtureManifestMatchesChecksums(t *testing.T) {
	f := newSkillFixture(t, "2026.11.01.01")
	for name, want := range f.sha {
		sum := sha256.Sum256(f.files[name])
		if got := fmt.Sprintf("%x", sum); got != want {
			t.Fatalf("%s: %s != %s", name, got, want)
		}
	}
}
