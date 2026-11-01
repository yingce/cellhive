package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	// The docs site embeds this placeholder in the checked-in Skill. The
	// installer rewrites it to the configured base URL so installed links work.
	skillPlaceholderBase = "https://cellhive.example.com"
	skillMaxFileBytes    = 1 << 20
	skillMaxTotalBytes   = 8 << 20
)

var (
	skillHTTPClient = &http.Client{Timeout: 15 * time.Second}
	renameSkill     = os.Rename // test seam for rollback
)

type cellhiveSkillBundle struct {
	Version string `json:"version"`
	Skills  []struct {
		Name     string `json:"name"`
		Manifest string `json:"manifest"`
	} `json:"skills"`
}

type cellhiveSkillManifest struct {
	Name       string            `json:"name"`
	Version    string            `json:"version"`
	BaseURL    string            `json:"baseUrl"`
	Entrypoint string            `json:"entrypoint"`
	Files      []string          `json:"files"`
	SHA256     map[string]string `json:"sha256"`
}

func cellhiveSkillRoot() string {
	if os.Getenv("CELLHIVE_AGENT") == "codex" {
		if v := os.Getenv("CODEX_HOME"); v != "" {
			return filepath.Join(v, "skills")
		}
		h, _ := os.UserHomeDir()
		return filepath.Join(h, ".codex", "skills")
	}
	if v := os.Getenv("AGENTS_HOME"); v != "" {
		return filepath.Join(v, "skills")
	}
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".agents", "skills")
}

func cellhiveSkillRootFor(dir string) string {
	if strings.TrimSpace(dir) != "" {
		return dir
	}
	return cellhiveSkillRoot()
}

// resolveSkillBase returns the docs host the Skill bundle is downloaded from.
// There is deliberately no built-in production default: a shipped placeholder
// would make `cellhive skills update` fetch dead URLs.
func resolveSkillBase(flag string) (string, error) {
	base := strings.TrimSpace(flag)
	if base == "" {
		base = strings.TrimSpace(os.Getenv("CELLHIVE_SKILL_BASE_URL"))
	}
	if base == "" {
		return "", errors.New("skill base URL is not set: pass --url or set CELLHIVE_SKILL_BASE_URL (for example https://docs.example.com)")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid skill base URL %q", base)
	}
	return strings.TrimRight(base, "/"), nil
}

func newSkillsCommand() *cobra.Command {
	var statusDir string
	skills := &cobra.Command{
		Use:   "skills",
		Short: "inspect or update the AI Skill bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("usage: cellhive skills status|update [--dir DIR] [--url URL] [--force]")
		},
	}
	status := &cobra.Command{
		Use:   "status",
		Short: "show the installed skill bundle version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return skillsStatus(cellhiveSkillRootFor(statusDir))
		},
	}
	status.Flags().StringVar(&statusDir, "dir", "", "skills directory (default: $AGENTS_HOME/skills or ~/.agents/skills)")
	skills.AddCommand(status, newSkillsUpdateCommand())
	return skills
}

func skillsStatus(root string) error {
	b, err := os.ReadFile(filepath.Join(root, ".cellhive-skills-version"))
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("not_installed")
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("version: %s\nroot: %s\n", strings.TrimSpace(string(b)), root)
	return nil
}

func newSkillsUpdateCommand() *cobra.Command {
	var force bool
	var dir, rawBase string
	cmd := &cobra.Command{
		Use:   "update",
		Short: "download/update the AI Skill bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			base, err := resolveSkillBase(rawBase)
			if err != nil {
				return err
			}
			return updateCellhiveSkill(cmd.Context(), cellhiveSkillRootFor(dir), base, force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "redownload the current version")
	cmd.Flags().StringVar(&dir, "dir", "", "skills directory (default: $AGENTS_HOME/skills or ~/.agents/skills)")
	cmd.Flags().StringVar(&rawBase, "url", "", "docs base URL (default: $CELLHIVE_SKILL_BASE_URL)")
	return cmd
}

func updateCellhiveSkill(ctx context.Context, root, base string, force bool) error {
	budget := int64(skillMaxTotalBytes)
	fetch := func(rawurl string, limit int64) ([]byte, error) {
		if budget <= 0 {
			return nil, errors.New("skill bundle exceeds total size limit")
		}
		if limit <= 0 || limit > budget {
			limit = budget
		}
		data, err := fetchSkillBytes(ctx, rawurl, limit)
		if err != nil {
			return nil, err
		}
		budget -= int64(len(data))
		return data, nil
	}
	fetchJSON := func(rawurl string, dst any) error {
		b, err := fetch(rawurl, skillMaxFileBytes)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, dst); err != nil {
			return fmt.Errorf("parse %s: %w", rawurl, err)
		}
		return nil
	}

	var catalog cellhiveSkillBundle
	if err := fetchJSON(base+"/skills/manifest.json", &catalog); err != nil {
		return err
	}
	if catalog.Version == "" || len(catalog.Skills) == 0 {
		return fmt.Errorf("invalid skill catalog")
	}
	versionPath := filepath.Join(root, ".cellhive-skills-version")
	if !force {
		if b, e := os.ReadFile(versionPath); e == nil && strings.TrimSpace(string(b)) == catalog.Version {
			fmt.Printf("CellHive skills already current at %s\n", catalog.Version)
			return nil
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(root, ".cellhive-update.")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for _, entry := range catalog.Skills {
		if entry.Name != "cellhive" {
			continue
		}
		var m cellhiveSkillManifest
		if err := fetchJSON(base+"/skills/"+entry.Manifest, &m); err != nil {
			return err
		}
		if m.Name != entry.Name || m.Version != catalog.Version || m.Entrypoint != "SKILL.md" {
			return fmt.Errorf("invalid skill manifest for %s", entry.Name)
		}
		if err := validateSkillFiles(m); err != nil {
			return err
		}
		target := filepath.Join(tmp, entry.Name)
		if err := os.MkdirAll(target, 0o700); err != nil {
			return err
		}
		for _, file := range m.Files {
			data, err := fetch(base+"/skills/"+entry.Name+"/"+file, skillMaxFileBytes)
			if err != nil {
				return err
			}
			if len(data) == 0 {
				return fmt.Errorf("empty skill file %q", file)
			}
			if file != "manifest.json" && len(m.SHA256) > 0 {
				want := strings.ToLower(strings.TrimSpace(m.SHA256[file]))
				if want == "" {
					return fmt.Errorf("skill manifest has no sha256 for %q", file)
				}
				sum := sha256.Sum256(data)
				if got := hex.EncodeToString(sum[:]); got != want {
					return fmt.Errorf("checksum mismatch for %q: got %s want %s", file, got, want)
				}
			}
			if isTextSkillFile(file) {
				data = bytes.ReplaceAll(data, []byte(skillPlaceholderBase), []byte(base))
			}
			dst := filepath.Join(target, file)
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(dst, data, 0o600); err != nil {
				return err
			}
		}
	}
	newDir := filepath.Join(tmp, "cellhive")
	if _, err := os.Stat(newDir); err != nil {
		return fmt.Errorf("skill bundle did not contain cellhive/SKILL.md")
	}
	if err := swapSkillDir(root, newDir); err != nil {
		return err
	}
	if err := os.WriteFile(versionPath, []byte(catalog.Version+"\n"), 0o600); err != nil {
		return err
	}
	fmt.Printf("updated CellHive skills to %s\n", catalog.Version)
	return nil
}

// validateSkillFiles rejects absolute/traversal paths and requires the
// entrypoint to be part of the downloadable set.
func validateSkillFiles(m cellhiveSkillManifest) error {
	seen := map[string]bool{}
	hasEntrypoint := false
	for _, file := range m.Files {
		if !validSkillPath(file) {
			return fmt.Errorf("unsafe skill path %q", file)
		}
		if seen[file] {
			return fmt.Errorf("duplicate skill file %q", file)
		}
		seen[file] = true
		if file == m.Entrypoint {
			hasEntrypoint = true
		}
	}
	if !hasEntrypoint {
		return fmt.Errorf("skill manifest does not include entrypoint %q", m.Entrypoint)
	}
	return nil
}

func validSkillPath(file string) bool {
	if file == "" || strings.HasPrefix(file, "/") || strings.Contains(file, "\\") {
		return false
	}
	clean := path.Clean(file)
	return clean == file && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func isTextSkillFile(file string) bool {
	switch strings.ToLower(path.Ext(file)) {
	case ".md", ".json", ".yaml", ".yml", ".txt":
		return true
	default:
		return false
	}
}

// swapSkillDir replaces root/cellhive atomically. The previous install is kept
// outside the updater's temp directory until the new one is in place; if the
// swap fails it is restored instead of being deleted with the temp tree.
func swapSkillDir(root, newDir string) error {
	target := filepath.Join(root, "cellhive")
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
		return renameSkill(newDir, target)
	} else if err != nil {
		return err
	}
	backupDir, err := os.MkdirTemp(root, ".cellhive-old.")
	if err != nil {
		return err
	}
	backup := filepath.Join(backupDir, "cellhive")
	if err := renameSkill(target, backup); err != nil {
		_ = os.RemoveAll(backupDir)
		return err
	}
	if err := renameSkill(newDir, target); err != nil {
		if rerr := renameSkill(backup, target); rerr != nil {
			return fmt.Errorf("install skill: %w; restore failed: %v (backup at %s)", err, rerr, backup)
		}
		_ = os.RemoveAll(backupDir)
		return err
	}
	_ = os.RemoveAll(backupDir)
	return nil
}

func fetchSkillBytes(ctx context.Context, rawurl string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return nil, err
	}
	resp, err := skillHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s: %s", rawurl, resp.Status)
	}
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("download %s: content length %d exceeds limit %d", rawurl, resp.ContentLength, limit)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("download %s: exceeds limit %d", rawurl, limit)
	}
	return data, nil
}
