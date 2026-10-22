package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

func newWorkerCommand() *cobra.Command {
	worker := &cobra.Command{
		Use:   "worker",
		Short: "worker operations",
	}
	worker.AddCommand(&cobra.Command{
		Use:   "delete <namespace> <worker>",
		Short: "delete a worker",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return workerDelete(args[0], args[1])
		},
	})
	return worker
}

func workerDelete(namespace, worker string) error {
	q := url.Values{"namespace": {namespace}, "worker": {worker}}
	data, err := adminCall(http.MethodDelete, "/v1/control/worker?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func newWorkflowCommand() *cobra.Command {
	workflow := &cobra.Command{
		Use:   "workflow",
		Short: "manage workflow resources",
	}
	workflow.AddCommand(&cobra.Command{
		Use:   "create <namespace> <name>",
		Short: "create a workflow definition resource",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return workflowCreate(args[0], args[1])
		},
	})
	return workflow
}

func workflowCreate(namespace, name string) error {
	data, err := adminCall(http.MethodPost, "/v1/control/resource", map[string]string{
		"namespace": namespace, "kind": "workflow", "name": name,
		"scope": fmt.Sprintf("%s/__workflow__/%s", namespace, name),
	})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "print the control-plane overview",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := adminCall(http.MethodGet, "/v1/control/status", nil)
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
}

func newCapacityCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "capacity",
		Short: "print the autoscaler capacity recommendation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := adminCall(http.MethodGet, "/v1/control/capacity", nil)
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
}

func newRoutesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "routes",
		Short: "pull the routing projection",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := internalGet("/v1/control/routes")
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
}

func newGCCommand() *cobra.Command {
	gc := &cobra.Command{
		Use:   "gc",
		Short: "garbage-collect unreferenced bundles or assets",
	}
	for _, target := range []string{"bundles", "assets"} {
		target := target
		gc.AddCommand(&cobra.Command{
			Use:   target,
			Short: "delete unreferenced " + target,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				data, err := adminCall(http.MethodPost, "/v1/control/gc/"+target, nil)
				if err != nil {
					return err
				}
				printJSON(data)
				return nil
			},
		})
	}
	return gc
}

func newReleasesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "releases <namespace> <worker>",
		Short: "show a worker's release log",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := adminCall(http.MethodGet, "/v1/control/releases?namespace="+url.QueryEscape(args[0])+"&worker="+url.QueryEscape(args[1]), nil)
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
}

// cmdReleases keeps the compatibility helper used by cmdCompat's
// versions/deployments aliases.
func cmdReleases(args []string) error {
	return dispatch(append([]string{"releases"}, args...))
}

func newAuditCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "audit <namespace>",
		Short: "show the audit log",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := url.Values{"namespace": {args[0]}}
			data, err := adminCall(http.MethodGet, "/v1/control/audit?"+q.Encode(), nil)
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
}

func newBundleCommand() *cobra.Command {
	bundle := &cobra.Command{
		Use:   "bundle",
		Short: "bundle operations (put|build)",
	}
	bundle.AddCommand(
		&cobra.Command{
			Use:   "put <file>",
			Short: "upload a prebuilt bundle",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				data, err := os.ReadFile(args[0])
				if err != nil {
					return err
				}
				out, err := adminCallRaw("/v1/control/bundle", data)
				if err != nil {
					return err
				}
				printJSON(out)
				return nil
			},
		},
		newLegacyCommand("build <entry> --out <file>", "build a worker bundle with esbuild", cmdBundleBuild),
	)
	return bundle
}

func newAssetCommand() *cobra.Command {
	asset := &cobra.Command{
		Use:   "asset",
		Short: "static asset operations",
	}
	asset.AddCommand(&cobra.Command{
		Use:   "put <namespace> <worker> <path> <file>",
		Short: "upload one static asset",
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[3])
			if err != nil {
				return err
			}
			q := url.Values{"ns": {args[0]}, "worker": {args[1]}, "path": {args[2]}}
			out, err := adminCallRaw("/v1/control/asset?"+q.Encode(), data)
			if err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	})
	return asset
}
