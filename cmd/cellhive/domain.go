package main

import (
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func newDomainCommand() *cobra.Command {
	domain := &cobra.Command{
		Use:   "domain",
		Short: "manage custom domains",
	}
	domain.AddCommand(
		&cobra.Command{
			Use:   "add <namespace> <host>",
			Short: "register a custom domain",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				return domainAdd(args[0], args[1])
			},
		},
		&cobra.Command{
			Use:     "ls <namespace>",
			Aliases: []string{"list"},
			Short:   "list custom domains",
			Args:    cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return domainList(args[0])
			},
		},
		&cobra.Command{
			Use:     "rm <namespace> <host>",
			Aliases: []string{"delete"},
			Short:   "remove a custom domain",
			Args:    cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				return domainRemove(args[0], args[1])
			},
		},
	)
	return domain
}

func domainAdd(namespace, host string) error {
	data, err := adminCall(http.MethodPost, "/v1/control/domain", map[string]string{
		"namespace": namespace, "host": host,
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func domainList(namespace string) error {
	q := url.Values{"namespace": {namespace}}
	data, err := adminCall(http.MethodGet, "/v1/control/domains?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func domainRemove(namespace, host string) error {
	q := url.Values{"namespace": {namespace}, "host": {host}}
	data, err := adminCall(http.MethodDelete, "/v1/control/domain?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// cmdDomain keeps the pre-cobra entry point used by in-process tests.
func cmdDomain(args []string) error {
	return dispatch(append([]string{"domain"}, args...))
}
