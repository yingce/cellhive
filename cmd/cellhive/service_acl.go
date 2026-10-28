package main

import (
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func newServiceACLCommand() *cobra.Command {
	acl := &cobra.Command{
		Use:   "service-acl",
		Short: "manage cross-namespace service binding grants",
	}
	acl.AddCommand(
		&cobra.Command{
			Use:   "add <namespace> <worker> <caller-namespace>",
			Short: "grant a caller namespace access to a service",
			Args:  cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return serviceACLAdd(args[0], args[1], args[2])
			},
		},
		&cobra.Command{
			Use:     "ls <namespace>",
			Aliases: []string{"list"},
			Short:   "list service ACL grants",
			Args:    cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return serviceACLList(args[0])
			},
		},
		&cobra.Command{
			Use:     "rm <namespace> <worker> <caller-namespace>",
			Aliases: []string{"delete"},
			Short:   "revoke a service ACL grant",
			Args:    cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return serviceACLRemove(args[0], args[1], args[2])
			},
		},
	)
	return acl
}

func serviceACLAdd(namespace, worker, caller string) error {
	data, err := adminCall(http.MethodPost, "/v1/control/service-acl", map[string]string{
		"namespace": namespace, "worker": worker, "caller_namespace": caller,
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func serviceACLList(namespace string) error {
	data, err := adminCall(http.MethodGet, "/v1/control/service-acls?namespace="+url.QueryEscape(namespace), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func serviceACLRemove(namespace, worker, caller string) error {
	q := url.Values{"namespace": {namespace}, "worker": {worker}, "caller_namespace": {caller}}
	data, err := adminCall(http.MethodDelete, "/v1/control/service-acl?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}
