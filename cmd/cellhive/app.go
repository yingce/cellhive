package main

import (
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

// newAppCommand exposes the native application (namespace) group. The HTTP
// calls below are the single implementation; `cmdApp` remains only as a
// compatibility shim for in-process callers/e2e tests.
func newAppCommand() *cobra.Command {
	app := &cobra.Command{
		Use:   "app",
		Short: "manage applications (namespaces)",
	}
	app.AddCommand(
		&cobra.Command{
			Use:   "create <namespace>",
			Short: "create an application",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return appCreate(args[0])
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "list applications",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return appList()
			},
		},
		&cobra.Command{
			Use:   "delete <namespace>",
			Short: "delete an application",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return appDelete(args[0])
			},
		},
	)
	return app
}

func appCreate(namespace string) error {
	data, err := adminCall(http.MethodPost, "/v1/control/app", map[string]string{"namespace": namespace})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func appList() error {
	data, err := adminCall(http.MethodGet, "/v1/control/apps", nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func appDelete(namespace string) error {
	q := url.Values{"namespace": {namespace}}
	data, err := adminCall(http.MethodDelete, "/v1/control/app?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdApp keeps the pre-cobra entry point used by in-process e2e tests. New
// callers should use the cobra tree (P2, ADR-265).
func cmdApp(args []string) error {
	return dispatch(append([]string{"app"}, args...))
}
