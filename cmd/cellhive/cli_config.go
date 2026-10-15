package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
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
func cmdLogin(args []string) error {
	f := flag.NewFlagSet("login", flag.ContinueOnError)
	profile := f.String("profile", "default", "profile name")
	admin := f.String("url", "", "CellHive admin URL")
	token := f.String("token", "", "admin token")
	jwt := f.String("jwt", "", "admin JWT")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *admin == "" {
		return fmt.Errorf("login requires --url")
	}
	if *token == "" && *jwt == "" {
		fmt.Fprint(os.Stderr, "Admin token: ")
		line, e := bufio.NewReader(os.Stdin).ReadString('\n')
		if e != nil {
			return e
		}
		*token = strings.TrimSpace(line)
	}
	p := cliConfigPath()
	c, e := loadCLIConfig(p)
	if e != nil {
		return e
	}
	c.Profiles[*profile] = cliProfile{AdminURL: strings.TrimRight(*admin, "/"), AdminToken: *token, AdminJWT: *jwt}
	if e = saveCLIConfig(p, c); e != nil {
		return e
	}
	fmt.Printf("saved profile %q in %s\n", *profile, p)
	return nil
}
func cmdAuth(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		c, e := loadCLIConfig(cliConfigPath())
		if e != nil {
			return e
		}
		for n := range c.Profiles {
			fmt.Println(n)
		}
		return nil
	}
	switch args[0] {
	case "current":
		fmt.Println(activeProfileName())
		return nil
	case "logout":
		if len(args) != 2 {
			return fmt.Errorf("usage: cellhive auth logout <profile>")
		}
		p := cliConfigPath()
		c, e := loadCLIConfig(p)
		if e != nil {
			return e
		}
		delete(c.Profiles, args[1])
		return saveCLIConfig(p, c)
	case "test":
		p := activeProfile()
		if p.AdminURL == "" {
			return fmt.Errorf("profile %q is not configured", activeProfileName())
		}
		fmt.Printf("profile: %s\nadmin_url: %s\nconfigured: yes\n", activeProfileName(), p.AdminURL)
		return nil
	default:
		return fmt.Errorf("usage: cellhive auth list|current|test|logout <profile>")
	}
}
func cmdProject(args []string) error {
	if len(args) == 0 || args[0] != "init" {
		return fmt.Errorf("usage: cellhive project init --namespace <ns> [--profile <profile>]")
	}
	f := flag.NewFlagSet("project init", flag.ContinueOnError)
	ns := f.String("namespace", "", "CellHive namespace")
	profile := f.String("profile", activeProfileName(), "profile name")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if *ns == "" {
		return fmt.Errorf("project init requires --namespace")
	}
	p := filepath.Join(".cellhive", "project.json")
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return e
	}
	b, _ := json.MarshalIndent(projectConfig{Profile: *profile, Namespace: *ns}, "", "  ")
	if e := os.WriteFile(p, append(b, '\n'), 0600); e != nil {
		return e
	}
	fmt.Printf("initialized %s\n", p)
	return nil
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

func cmdEnv() error {
	p := activeProfile()
	if p.AdminURL == "" {
		return fmt.Errorf("profile %q is not configured", activeProfileName())
	}
	ns := projectNamespace()
	if ns == "" {
		return fmt.Errorf("project namespace is not configured; run `cellhive project init --namespace <ns>`")
	}
	base := strings.TrimRight(p.AdminURL, "/")
	if !strings.HasSuffix(base, "/client/v4") {
		base += "/client/v4"
	}
	token := p.AdminToken
	if token == "" {
		token = p.AdminJWT
	}
	for _, kv := range []string{"CLOUDFLARE_API_BASE_URL=" + base, "CLOUDFLARE_API_TOKEN=" + token, "CLOUDFLARE_ACCOUNT_ID=" + ns, "WRANGLER_SEND_METRICS=false"} {
		fmt.Printf("export %s\n", shellQuote(kv))
	}
	return nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
