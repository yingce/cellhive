package main

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

// newResourceCommand exposes the native resource registry group with typed
// flags. The HTTP calls are shared by the compatibility shim below.
func newResourceCommand() *cobra.Command {
	resource := &cobra.Command{
		Use:   "resource",
		Short: "manage registered resources",
	}

	create := &cobra.Command{
		Use:   "create <namespace> <kind> <name>",
		Short: "register a resource",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, _ := cmd.Flags().GetString("scope")
			conn, _ := cmd.Flags().GetString("connection-string")
			return resourceCreate(args[0], args[1], args[2], scope, conn)
		},
	}
	create.Flags().String("scope", "", "resource cell scope (default <ns>/__<kind>__/<name>)")
	create.Flags().String("connection-string", "", "hyperdrive origin URL (sealed server-side; kind=hyperdrive only)")

	list := &cobra.Command{
		Use:   "list <namespace>",
		Short: "list registered resources",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kind, _ := cmd.Flags().GetString("kind")
			return resourceList(args[0], kind)
		},
	}
	list.Flags().String("kind", "", "filter by kind")

	del := &cobra.Command{
		Use:     "delete <namespace> <kind> <name>",
		Aliases: []string{"rm", "revoke"},
		Short:   "revoke a resource (data kept)",
		Args:    cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			force, _ := cmd.Flags().GetBool("force")
			return resourceDelete(args[0], args[1], args[2], force)
		},
	}
	del.Flags().Bool("force", false, "revoke even while worker versions still bind the resource")

	resource.AddCommand(create, list, del)
	return resource
}

func resourceCreate(namespace, kind, name, scope, connectionString string) error {
	if scope == "" {
		scope = fmt.Sprintf("%s/__%s__/%s", namespace, kind, name)
	}
	payload := map[string]string{
		"namespace": namespace, "kind": kind, "name": name, "scope": scope,
	}
	if connectionString != "" {
		payload["connection_string"] = connectionString
	}
	data, err := adminCall(http.MethodPost, "/v1/control/resource", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func resourceList(namespace, kind string) error {
	q := url.Values{"namespace": {namespace}}
	if kind != "" {
		q.Set("kind", kind)
	}
	data, err := adminCall(http.MethodGet, "/v1/control/resources?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func resourceDelete(namespace, kind, name string, force bool) error {
	q := url.Values{"namespace": {namespace}, "kind": {kind}, "name": {name}}
	if force {
		q.Set("force", "1")
	}
	data, err := adminCall(http.MethodDelete, "/v1/control/resource?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdResource keeps the pre-cobra entry point used by in-process e2e tests.
func cmdResource(args []string) error {
	return dispatch(append([]string{"resource"}, args...))
}
