package main

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
)

func newCredsCommand() *cobra.Command {
	creds := &cobra.Command{
		Use:   "creds [role]",
		Short: "print root-derived role signing keys (not Bearer tokens)",
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
		Use:   "issuer <namespace-pattern>",
		Short: "derive an issuer signing key locally from the root key (no server required)",
		Long:  "Derive a signing key for an exact namespace, trailing-prefix* pattern, or *. No registration or persisted issuer state is used. Quote wildcard patterns in your shell.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := bearertoken.ValidatePattern(args[0]); err != nil {
				return fmt.Errorf("invalid issuer namespace pattern: %w", err)
			}
			key, err := (config.Config{RootKey: config.LoadRootKey()}).JWTKey("issuer/" + args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), base64.RawURLEncoding.EncodeToString(key))
			return err
		},
	})
	return creds
}

func credsPrint(role string) error {
	c := config.DeriveCredentials(config.LoadRootKey())
	all := map[string]string{
		"peer": c.Peer, "internal": c.Internal, "dispatch": c.Dispatch,
		"log": c.Log, "admin": c.Admin, "scope": c.Binding,
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

func newTokenCommand() *cobra.Command {
	var namespace, kind, name, issuer, keyB64 string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "token",
		Short: "mint a namespace or binding JWT locally using an issuer pattern and key",
		Long:  "Mint locally without a root key or server. The issuer namespace pattern is the signing identity and ceiling; requested namespaces must be a subset. Maximum lifetime is 86400 seconds. Send the JWT only as Authorization: Bearer <token>.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := mintScopeToken(namespace, kind, name, issuer, ttl, keyB64)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), tok)
			return nil
		},
	}
	cmd.Flags().StringVar(&namespace, "ns", "", "comma-separated namespace patterns; one exact namespace for binding tokens")
	cmd.Flags().StringVar(&kind, "kind", "", "exact binding kind; omit for a namespace token")
	cmd.Flags().StringVar(&name, "name", "", "exact binding resource name; required with --kind")
	cmd.Flags().StringVar(&issuer, "issuer", "", "namespace ceiling: exact name, prefix*, or * (required; quote wildcards)")
	cmd.Flags().DurationVar(&ttl, "ttl", 5*time.Minute, "positive lifetime in whole seconds, at most 24h (86400 seconds)")
	cmd.Flags().StringVar(&keyB64, "key", "", "canonical raw base64url 32-byte issuer key (required)")
	return cmd
}

// cmdCreds keeps the pre-cobra entry point used by in-process tests.
func cmdCreds(args []string) error {
	return dispatch(append([]string{"creds"}, args...))
}
