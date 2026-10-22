package main

import (
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func newRouteCommand() *cobra.Command {
	route := &cobra.Command{
		Use:   "route",
		Short: "manage host routes",
	}

	var addPath string
	add := &cobra.Command{
		Use:   "add <namespace> <host> <worker>",
		Short: "add or replace a host route",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return routeAdd(args[0], args[1], args[2], addPath)
		},
	}
	add.Flags().StringVar(&addPath, "path", "", "path prefix (always stripped before the worker sees the request)")

	var rmPath string
	rm := &cobra.Command{
		Use:   "rm <namespace> <host>",
		Short: "delete a host route",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Preserve the old semantics: path is only sent when the flag was
			// explicitly provided, so `--path ""` still deletes that exact path.
			return routeRemove(args[0], args[1], rmPath, cmd.Flags().Changed("path"))
		},
	}
	rm.Flags().StringVar(&rmPath, "path", "", "delete only this exact path (omit to delete all routes for the host)")

	route.AddCommand(add, rm)
	return route
}

func routeAdd(namespace, host, worker, path string) error {
	data, err := adminCall(http.MethodPost, "/v1/control/route", map[string]string{
		"namespace": namespace, "host": host, "worker": worker, "path": path,
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func routeRemove(namespace, host, path string, pathSet bool) error {
	q := url.Values{"namespace": {namespace}, "host": {host}}
	if pathSet {
		q.Set("path", path)
	}
	data, err := adminCall(http.MethodDelete, "/v1/control/route?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdRoute keeps the pre-cobra entry point used by in-process tests.
func cmdRoute(args []string) error {
	return dispatch(append([]string{"route"}, args...))
}
