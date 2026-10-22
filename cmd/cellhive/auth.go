package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func newLoginCommand() *cobra.Command {
	var profile, adminURL, token, jwt string
	cmd := &cobra.Command{
		Use:   "login",
		Short: "log in to a CellHive admin endpoint and save a profile",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogin(profile, adminURL, token, jwt)
		},
	}
	cmd.Flags().StringVar(&profile, "profile", "default", "profile name")
	cmd.Flags().StringVar(&adminURL, "url", "", "CellHive admin URL")
	cmd.Flags().StringVar(&token, "token", "", "admin token")
	cmd.Flags().StringVar(&jwt, "jwt", "", "admin JWT")
	return cmd
}

func runLogin(profile, adminURL, token, jwt string) error {
	if adminURL == "" {
		return fmt.Errorf("login requires --url")
	}
	if token == "" && jwt == "" {
		fmt.Fprint(os.Stderr, "Admin token: ")
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			return err
		}
		token = strings.TrimSpace(line)
	}
	path := cliConfigPath()
	cfg, err := loadCLIConfig(path)
	if err != nil {
		return err
	}
	cfg.Profiles[profile] = cliProfile{AdminURL: strings.TrimRight(adminURL, "/"), AdminToken: token, AdminJWT: jwt}
	if err := saveCLIConfig(path, cfg); err != nil {
		return err
	}
	fmt.Printf("saved profile %q in %s\n", profile, path)
	return nil
}

func newAuthCommand() *cobra.Command {
	auth := &cobra.Command{
		Use:   "auth",
		Short: "manage CLI auth profiles (list|current|test|logout)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return authList(cmd.OutOrStdout())
		},
	}
	auth.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "list profiles",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return authList(cmd.OutOrStdout())
			},
		},
		&cobra.Command{
			Use:   "current",
			Short: "print the active profile",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				fmt.Fprintln(cmd.OutOrStdout(), activeProfileName())
				return nil
			},
		},
		&cobra.Command{
			Use:   "test",
			Short: "show the active profile endpoint",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return authTest(cmd.OutOrStdout())
			},
		},
		&cobra.Command{
			Use:   "logout <profile>",
			Short: "remove a profile",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return authLogout(args[0])
			},
		},
	)
	return auth
}

func authList(w io.Writer) error {
	cfg, err := loadCLIConfig(cliConfigPath())
	if err != nil {
		return err
	}
	for name := range cfg.Profiles {
		fmt.Fprintln(w, name)
	}
	return nil
}

func authTest(w io.Writer) error {
	p := activeProfile()
	if p.AdminURL == "" {
		return fmt.Errorf("profile %q is not configured", activeProfileName())
	}
	fmt.Fprintf(w, "profile: %s\nadmin_url: %s\nconfigured: yes\n", activeProfileName(), p.AdminURL)
	return nil
}

func authLogout(profile string) error {
	path := cliConfigPath()
	cfg, err := loadCLIConfig(path)
	if err != nil {
		return err
	}
	delete(cfg.Profiles, profile)
	return saveCLIConfig(path, cfg)
}

func newProjectCommand() *cobra.Command {
	project := &cobra.Command{
		Use:   "project",
		Short: "manage project namespace/profile config",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("usage: cellhive project init --namespace <ns> [--profile <profile>]")
		},
	}
	var namespace, profile string
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "write .cellhive/project.json",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if profile == "" {
				profile = activeProfileName()
			}
			if namespace == "" {
				return fmt.Errorf("project init requires --namespace")
			}
			path := filepath.Join(".cellhive", "project.json")
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			body, _ := json.MarshalIndent(projectConfig{Profile: profile, Namespace: namespace}, "", "  ")
			if err := os.WriteFile(path, append(body, '\n'), 0600); err != nil {
				return err
			}
			fmt.Printf("initialized %s\n", path)
			return nil
		},
	}
	initCmd.Flags().StringVar(&namespace, "namespace", "", "CellHive namespace")
	initCmd.Flags().StringVar(&profile, "profile", "", "profile name (default: active profile)")
	project.AddCommand(initCmd)
	return project
}

func newEnvCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "print Wrangler-compatible environment exports",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEnv(cmd.OutOrStdout())
		},
	}
}

func runEnv(w io.Writer) error {
	p := activeProfile()
	if p.AdminURL == "" {
		return fmt.Errorf("profile %q is not configured", activeProfileName())
	}
	namespace := projectNamespace()
	if namespace == "" {
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
	for _, kv := range []string{"CLOUDFLARE_API_BASE_URL=" + base, "CLOUDFLARE_API_TOKEN=" + token, "CLOUDFLARE_ACCOUNT_ID=" + namespace, "WRANGLER_SEND_METRICS=false"} {
		fmt.Fprintf(w, "export %s\n", shellQuote(kv))
	}
	return nil
}
