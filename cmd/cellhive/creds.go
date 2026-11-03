package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

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
		Use:   "issuer <name>",
		Short: "removed: use issuer create to provision an authoritative issuer",
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

func credsIssuer(name string) error {
	return fmt.Errorf("name-derived issuer keys are removed; use cellhive issuer create %q --ns <patterns> --max-token-ttl <duration> to provision an immutable issuer ID and key", name)
}

func newTokenCommand() *cobra.Command {
	var namespace, kind, name, issuer, keyB64 string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "token",
		Short: "mint a namespace or binding JWT locally using an issuer ID and key",
		Long:  "Mint locally without querying issuer state. The server enforces the active issuer record, namespace ceiling and maximum TTL on every use.",
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
	cmd.Flags().StringVar(&issuer, "issuer", "", "immutable 32-character lowercase hex issuer ID (required)")
	cmd.Flags().DurationVar(&ttl, "ttl", 5*time.Minute, "positive lifetime in whole seconds; must not exceed server issuer maximum")
	cmd.Flags().StringVar(&keyB64, "key", "", "canonical raw base64url 32-byte issuer key (required)")
	return cmd
}

// cmdCreds keeps the pre-cobra entry point used by in-process tests.
func cmdCreds(args []string) error {
	return dispatch(append([]string{"creds"}, args...))
}
