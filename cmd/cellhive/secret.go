package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newSecretCommand() *cobra.Command {
	secret := &cobra.Command{
		Use:   "secret",
		Short: "manage worker secrets",
	}

	var value string
	put := &cobra.Command{
		Use:   "put <namespace> <worker> <KEY>",
		Short: "create or replace a secret",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return secretPut(args[0], args[1], args[2], value)
		},
	}
	put.Flags().StringVar(&value, "value", "", "base64 value (default: read stdin)")

	secret.AddCommand(
		put,
		&cobra.Command{
			Use:   "get <namespace> <worker> <KEY>",
			Short: "show a secret",
			Args:  cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return secretGet(args[0], args[1], args[2])
			},
		},
		&cobra.Command{
			Use:   "delete <namespace> <worker> <KEY>",
			Short: "delete a secret",
			Args:  cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return secretDelete(args[0], args[1], args[2])
			},
		},
		&cobra.Command{
			Use:   "list <namespace> <worker>",
			Short: "list secret names",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				return secretList(args[0], args[1])
			},
		},
		&cobra.Command{
			Use:   "bulk <namespace> <worker> <json-or-env-file>",
			Short: "upload or delete up to 100 secrets",
			Args:  cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return secretBulk(args[0], args[1], args[2])
			},
		},
	)
	return secret
}

func secretPut(namespace, worker, key, value string) error {
	raw := value
	if raw == "" {
		in, _ := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		raw = base64.StdEncoding.EncodeToString(in)
	}
	data, err := adminCall(http.MethodPost, "/v1/control/secret", map[string]string{
		"namespace": namespace, "worker": worker, "key": key, "value": raw,
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func secretGet(namespace, worker, key string) error {
	q := url.Values{"namespace": {namespace}, "worker": {worker}, "key": {key}}
	data, err := adminCall(http.MethodGet, "/v1/control/secret?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func secretDelete(namespace, worker, key string) error {
	q := url.Values{"namespace": {namespace}, "worker": {worker}, "key": {key}}
	data, err := adminCall(http.MethodDelete, "/v1/control/secret?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func secretList(namespace, worker string) error {
	q := url.Values{"namespace": {namespace}, "worker": {worker}}
	data, err := adminCall(http.MethodGet, "/v1/control/secrets?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func secretBulk(namespace, worker, file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return fmt.Errorf("secret bulk file exceeds 1 MiB")
	}
	changes := map[string]*string{}
	if strings.HasSuffix(strings.ToLower(file), ".env") {
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok || strings.TrimSpace(key) == "" {
				return fmt.Errorf("invalid .env secret line")
			}
			value = strings.TrimSuffix(value, "\r")
			changes[strings.TrimSpace(key)] = &value
		}
	} else if err := json.Unmarshal(raw, &changes); err != nil {
		return fmt.Errorf("secret bulk JSON: %w", err)
	}
	if len(changes) == 0 || len(changes) > 100 {
		return fmt.Errorf("secret bulk requires 1 to 100 entries")
	}
	secrets := map[string]any{}
	for key, value := range changes {
		if key == "" {
			return fmt.Errorf("secret bulk key is required")
		}
		if value == nil {
			secrets[key] = nil
		} else {
			secrets[key] = map[string]string{"name": key, "text": *value, "type": "secret_text"}
		}
	}
	path := "/client/v4/accounts/" + url.PathEscape(namespace) + "/workers/scripts/" + url.PathEscape(worker) + "/secrets-bulk"
	out, err := adminCall(http.MethodPatch, path, map[string]any{"secrets": secrets})
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}
