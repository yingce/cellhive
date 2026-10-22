package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

type cellhiveSkillBundle struct {
	Version string `json:"version"`
	Skills  []struct {
		Name     string `json:"name"`
		Manifest string `json:"manifest"`
	} `json:"skills"`
}
type cellhiveSkillManifest struct {
	Name       string   `json:"name"`
	Version    string   `json:"version"`
	Entrypoint string   `json:"entrypoint"`
	Files      []string `json:"files"`
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
func newSkillsCommand() *cobra.Command {
	skills := &cobra.Command{
		Use:   "skills",
		Short: "inspect or update the AI Skill bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("usage: cellhive skills status|update [--force]")
		},
	}
	skills.AddCommand(
		&cobra.Command{
			Use:   "status",
			Short: "show the installed skill bundle version",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return skillsStatus()
			},
		},
		newSkillsUpdateCommand(),
	)
	return skills
}

func skillsStatus() error {
	root := cellhiveSkillRoot()
	b, err := os.ReadFile(filepath.Join(root, ".cellhive-skills-version"))
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("not_installed")
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("version: %sroot: %s\n", string(b), root)
	return nil
}

func newSkillsUpdateCommand() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "update",
		Short: "download/update the AI Skill bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return updateCellhiveSkill(cellhiveSkillRoot(), force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "redownload the current version")
	return cmd
}

// cmdSkills keeps the pre-cobra entry point for in-process callers.
func cmdSkills(args []string) error {
	return dispatch(append([]string{"skills"}, args...))
}
func updateCellhiveSkill(root string, force bool) error {
	base := strings.TrimRight(os.Getenv("CELLHIVE_SKILL_BASE_URL"), "/")
	if base == "" {
		base = "https://cellhive.example.com"
	}
	catalog, err := getSkillJSON[cellhiveSkillBundle](base + "/skills/manifest.json")
	if err != nil {
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
	if err := os.MkdirAll(root, 0700); err != nil {
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
		m, err := getSkillJSON[cellhiveSkillManifest](base + "/skills/" + entry.Manifest)
		if err != nil {
			return err
		}
		if m.Name != entry.Name || m.Version != catalog.Version || m.Entrypoint != "SKILL.md" {
			return fmt.Errorf("invalid skill manifest for %s", entry.Name)
		}
		target := filepath.Join(tmp, entry.Name)
		if err := os.MkdirAll(target, 0700); err != nil {
			return err
		}
		for _, file := range m.Files {
			if file == "" || filepath.IsAbs(file) || strings.Contains(file, "..") || strings.Contains(file, "\\") {
				return fmt.Errorf("unsafe skill path %q", file)
			}
			data, err := getSkillBytes(base + "/skills/" + entry.Name + "/" + file)
			if err != nil {
				return err
			}
			path := filepath.Join(target, file)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				return err
			}
		}
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	target := filepath.Join(root, "cellhive")
	old := filepath.Join(tmp, "old")
	if _, e := os.Stat(target); e == nil {
		if err := os.Rename(target, old); err != nil {
			return err
		}
	}
	if err := os.Rename(filepath.Join(tmp, "cellhive"), target); err != nil {
		return err
	}
	if err := os.WriteFile(versionPath, []byte(catalog.Version+"\n"), 0600); err != nil {
		return err
	}
	fmt.Printf("updated CellHive skills to %s\n", catalog.Version)
	return nil
}
func getSkillJSON[T any](url string) (T, error) {
	var out T
	b, e := getSkillBytes(url)
	if e != nil {
		return out, e
	}
	if e = json.Unmarshal(b, &out); e != nil {
		return out, fmt.Errorf("parse %s: %w", url, e)
	}
	return out, nil
}
func getSkillBytes(url string) ([]byte, error) {
	r, e := http.Get(url)
	if e != nil {
		return nil, e
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s: %s", url, r.Status)
	}
	return io.ReadAll(r.Body)
}
