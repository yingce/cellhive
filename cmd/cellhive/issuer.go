package main

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/bearertoken"
	"github.com/spf13/cobra"
)

func validIssuerID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}

func newIssuerCommand() *cobra.Command {
	root := &cobra.Command{Use: "issuer", Short: "manage authoritative JWT issuers"}
	var namespaces []string
	var ttl time.Duration
	create := &cobra.Command{Use: "create <name>", Args: cobra.ExactArgs(1), Short: "provision issuer and signing key (store the key securely)", RunE: func(cmd *cobra.Command, args []string) error {
		if len(namespaces) == 0 || ttl < time.Second || ttl%time.Second != 0 {
			return fmt.Errorf("--ns and positive whole-second --max-token-ttl are required")
		}
		for _, ns := range namespaces {
			if err := bearertoken.ValidatePattern(ns); err != nil {
				return err
			}
		}
		out, err := adminCall(http.MethodPost, "/v1/control/issuers", map[string]any{"name": args[0], "namespaces": namespaces, "max_token_ttl_seconds": int64(ttl / time.Second)})
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(out))
		return nil
	}}
	create.Flags().StringSliceVar(&namespaces, "ns", nil, "namespace ceiling patterns (comma-separated or repeated)")
	create.Flags().DurationVar(&ttl, "max-token-ttl", 0, "maximum token lifetime, e.g. 1h (required)")
	root.AddCommand(create)
	for _, verb := range []string{"list", "get", "disable", "delete"} {
		c := &cobra.Command{Use: verb + " <id>", Short: verb + " issuers", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			path, method := "/v1/control/issuers", http.MethodGet
			var body any
			if verb != "list" {
				if !validIssuerID(args[0]) {
					return fmt.Errorf("issuer ID must be 32 lowercase hex characters")
				}
				path += "/" + args[0]
			}
			switch verb {
			case "disable":
				method, body = http.MethodPatch, map[string]bool{"enabled": false}
			case "delete":
				method = http.MethodDelete
			}
			out, err := adminCall(method, path, body)
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(out)) != "" {
				fmt.Fprintln(cmd.OutOrStdout(), string(out))
			}
			return nil
		}}
		if verb == "list" {
			c.Use, c.Args = "list", cobra.NoArgs
		}
		root.AddCommand(c)
	}
	return root
}
