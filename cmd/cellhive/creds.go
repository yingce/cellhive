package main

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"cellhive/internal/config"
	"cellhive/internal/scopedtoken"
)

func newCredsCommand() *cobra.Command {
	creds := &cobra.Command{
		Use:   "creds [role]",
		Short: "print root-derived role credentials",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			role := ""
			if len(args) == 1 {
				role = args[0]
			}
			return credsPrint(role)
		},
	}
	creds.AddCommand(&cobra.Command{
		Use:   "issuer <name>",
		Short: "print a delegated issuer signing key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return credsIssuer(args[0])
		},
	})
	return creds
}

func credsPrint(role string) error {
	c := config.DeriveCredentials(config.LoadRootKey())
	all := map[string]string{
		"peer": c.Peer, "internal": c.Internal, "dispatch": c.Dispatch,
		"log": c.Log, "admin": c.Admin, "scope": c.Scope,
		"do-ticket": c.DoTicket, "secret-key": c.SecretKey,
	}
	if c.Internal == "" {
		return fmt.Errorf("CELLHIVE_ROOT_KEY is required (or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use)")
	}
	if role != "" {
		v, ok := all[role]
		if !ok {
			return fmt.Errorf("unknown role %q (peer|internal|dispatch|log|admin|scope|do-ticket|secret-key)", role)
		}
		fmt.Println(v)
		return nil
	}
	for _, name := range []string{"peer", "internal", "dispatch", "log", "admin", "scope", "do-ticket", "secret-key"} {
		fmt.Printf("%s=%s\n", name, all[name])
	}
	return nil
}

func credsIssuer(name string) error {
	c := config.DeriveCredentials(config.LoadRootKey())
	if c.Scope == "" {
		return fmt.Errorf("CELLHIVE_ROOT_KEY is required (or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use)")
	}
	key, err := scopedtoken.IssuerKey([]byte(c.Scope), name)
	if err != nil {
		return err
	}
	fmt.Println(base64.RawURLEncoding.EncodeToString(key))
	return nil
}

func newTokenCommand() *cobra.Command {
	var namespace, kind, name, issuer, keyB64 string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "token",
		Short: "mint a scoped binding token",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := mintScopeToken(namespace, kind, name, issuer, ttl, keyB64)
			if err != nil {
				return fmt.Errorf("usage: cellhive token --ns <ns> --kind <kind> --name <name|glob> [--iss <issuer>] [--ttl 5m] [--key <b64>]: %w", err)
			}
			fmt.Println(tok)
			return nil
		},
	}
	cmd.Flags().StringVar(&namespace, "ns", "", "namespace (exact)")
	cmd.Flags().StringVar(&kind, "kind", "", "binding kind (may be * or pre*)")
	cmd.Flags().StringVar(&name, "name", "", "resource name (may be * or pre*)")
	cmd.Flags().StringVar(&issuer, "iss", "", "delegated issuer name (empty = platform scope key)")
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "lifetime; delegated tokens require > 0")
	cmd.Flags().StringVar(&keyB64, "key", "", "issuer signing key (raw base64url); skips root derivation")
	return cmd
}

// cmdCreds/cmdToken keep the pre-cobra entry points used by in-process tests.
func cmdCreds(args []string) error {
	return dispatch(append([]string{"creds"}, args...))
}

func cmdToken(args []string) error {
	return dispatch(append([]string{"token"}, args...))
}
