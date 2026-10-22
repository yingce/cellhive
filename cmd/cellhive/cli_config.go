package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type cliProfile struct {
	AdminURL   string `json:"admin_url,omitempty"`
	AdminToken string `json:"admin_token,omitempty"`
	AdminJWT   string `json:"admin_jwt,omitempty"`
}
type cliConfig struct {
	Profiles map[string]cliProfile `json:"profiles"`
}
type projectConfig struct {
	Profile   string            `json:"profile,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Workers   map[string]string `json:"workers,omitempty"`
}

func cliConfigPath() string {
	if p := os.Getenv("CELLHIVE_CLI_CONFIG"); p != "" {
		return p
	}
	d, e := os.UserConfigDir()
	if e != nil {
		return filepath.Join(".cellhive", "config.json")
	}
	return filepath.Join(d, "cellhive", "config.json")
}
func loadCLIConfig(path string) (cliConfig, error) {
	b, e := os.ReadFile(path)
	if errors.Is(e, os.ErrNotExist) {
		return cliConfig{Profiles: map[string]cliProfile{}}, nil
	}
	if e != nil {
		return cliConfig{}, e
	}
	var c cliConfig
	if e = json.Unmarshal(b, &c); e != nil {
		return c, fmt.Errorf("parse %s: %w", path, e)
	}
	if c.Profiles == nil {
		c.Profiles = map[string]cliProfile{}
	}
	return c, nil
}
func saveCLIConfig(path string, c cliConfig) error {
	if c.Profiles == nil {
		c.Profiles = map[string]cliProfile{}
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	b, e := json.MarshalIndent(c, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func findProjectConfig(start string) (string, projectConfig, error) {
	d, e := filepath.Abs(start)
	if e != nil {
		return "", projectConfig{}, e
	}
	for {
		p := filepath.Join(d, ".cellhive", "project.json")
		b, r := os.ReadFile(p)
		if r == nil {
			var c projectConfig
			if e = json.Unmarshal(b, &c); e != nil {
				return p, c, fmt.Errorf("parse %s: %w", p, e)
			}
			return p, c, nil
		}
		if !errors.Is(r, os.ErrNotExist) {
			return p, projectConfig{}, r
		}
		q := filepath.Dir(d)
		if q == d {
			break
		}
		d = q
	}
	return "", projectConfig{}, nil
}
func activeProfileName() string {
	if p := os.Getenv("CELLHIVE_PROFILE"); p != "" {
		return p
	}
	if _, p, e := findProjectConfig("."); e == nil && p.Profile != "" {
		return p.Profile
	}
	return "default"
}
func activeProfile() cliProfile {
	c, e := loadCLIConfig(cliConfigPath())
	if e != nil {
		return cliProfile{}
	}
	return c.Profiles[activeProfileName()]
}
func projectNamespace() string {
	_, p, e := findProjectConfig(".")
	if e == nil {
		return p.Namespace
	}
	return ""
}

func projectWorkerConfig(name string) string {
	path, p, err := findProjectConfig(".")
	if err != nil || path == "" || p.Workers == nil {
		return ""
	}
	rel := p.Workers[name]
	if rel == "" {
		return ""
	}
	root := filepath.Dir(filepath.Dir(path))
	if filepath.IsAbs(rel) {
		return rel
	}
	return filepath.Join(root, rel)
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
