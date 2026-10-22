package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/spf13/cobra"
)

// newKindCommand builds the per-domain registry/stat surface used by kv, d1,
// r2 and hyperdrive. The HTTP paths and payloads mirror the legacy cmdKind
// router, but flags are typed and the command tree is discoverable.
func newKindCommand(kind string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   kind,
		Short: kindShort(kind),
	}
	cmd.AddCommand(newKindRegistryLeaves(kind)...)
	return cmd
}

// newKindRegistryGroup builds the Wrangler-style noun group, e.g.
// `cellhive kv namespace ...` or `cellhive r2 bucket ...`.
func newKindRegistryGroup(kind, use, short string) *cobra.Command {
	cmd := &cobra.Command{Use: use, Short: short}
	cmd.AddCommand(newKindRegistryLeaves(kind)...)
	return cmd
}

func newKindRegistryLeaves(kind string) []*cobra.Command {
	create := &cobra.Command{
		Use:   "create <namespace> <name>",
		Short: "create " + kindLabel(kind),
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, _ := cmd.Flags().GetString("scope")
			conn, _ := cmd.Flags().GetString("connection-string")
			return kindCreate(kind, args[0], args[1], scope, conn)
		},
	}
	create.Flags().String("scope", "", "resource cell scope (default <ns>/__<kind>__/<name>)")
	create.Flags().String("connection-string", "", "hyperdrive origin URL (sealed server-side; kind=hyperdrive only)")

	list := &cobra.Command{
		Use:     "list <namespace>",
		Aliases: []string{"ls"},
		Short:   "list " + kindPlural(kind),
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return kindList(kind, args[0])
		},
	}

	del := &cobra.Command{
		Use:     "delete <namespace> <name>",
		Aliases: []string{"rm", "revoke"},
		Short:   "delete " + kindLabel(kind) + " (data kept)",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			force, _ := cmd.Flags().GetBool("force")
			return kindDelete(kind, args[0], args[1], force)
		},
	}
	del.Flags().Bool("force", false, "revoke even while worker versions still bind the resource")

	stats := &cobra.Command{
		Use:     "stats <namespace> <name>",
		Aliases: []string{"info"},
		Short:   "show " + kindLabel(kind) + " metadata/stats",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			prefix, _ := cmd.Flags().GetString("prefix")
			exact, _ := cmd.Flags().GetBool("exact")
			tables, _ := cmd.Flags().GetBool("tables")
			limit, _ := cmd.Flags().GetInt("limit")
			scan, _ := cmd.Flags().GetBool("scan")
			return kindStats(kind, args[0], args[1], kindStatsOptions{
				prefix: prefix, exact: exact, tables: tables, limit: limit, scan: scan,
			})
		},
	}
	stats.Flags().String("prefix", "", "restrict count-style fields to a key prefix")
	stats.Flags().Bool("exact", false, "force exact counts (may scan)")
	stats.Flags().Bool("tables", false, "include per-table dbstat usage (D1)")
	stats.Flags().Int("limit", 0, "max objects to inspect (R2)")
	stats.Flags().Bool("scan", false, "scan R2 objects for diagnostic stats")

	return []*cobra.Command{create, list, del, stats}
}

func kindShort(kind string) string {
	switch kind {
	case "kv":
		return "manage KV namespaces and keys"
	case "d1":
		return "manage D1 databases"
	case "r2":
		return "manage R2 buckets and objects"
	case "hyperdrive":
		return "manage Hyperdrive origin resources"
	case "queue":
		return "manage queues"
	default:
		return "manage " + kind + " resources"
	}
}

func kindLabel(kind string) string {
	switch kind {
	case "kv":
		return "KV namespace"
	case "d1":
		return "D1 database"
	case "r2":
		return "R2 bucket"
	case "hyperdrive":
		return "Hyperdrive resource"
	case "queue":
		return "queue"
	default:
		return kind + " resource"
	}
}

func kindPlural(kind string) string {
	switch kind {
	case "kv":
		return "KV namespaces"
	case "d1":
		return "D1 databases"
	case "r2":
		return "R2 buckets"
	case "hyperdrive":
		return "Hyperdrive resources"
	case "queue":
		return "queues"
	default:
		return kind + " resources"
	}
}

func kindCreate(kind, namespace, name, scope, connectionString string) error {
	if scope == "" {
		scope = fmt.Sprintf("%s/__%s__/%s", namespace, kind, name)
	}
	payload := map[string]string{"namespace": namespace, "kind": kind, "name": name, "scope": scope}
	if kind == "hyperdrive" && connectionString != "" {
		payload["connection_string"] = connectionString
	}
	data, err := adminCall(http.MethodPost, "/v1/"+kind+"/resources", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func kindList(kind, namespace string) error {
	q := url.Values{"namespace": {namespace}}
	data, err := adminCall(http.MethodGet, "/v1/"+kind+"/resources?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func kindDelete(kind, namespace, name string, force bool) error {
	q := url.Values{"namespace": {namespace}, "name": {name}}
	if force {
		q.Set("force", "1")
	}
	data, err := adminCall(http.MethodDelete, "/v1/"+kind+"/resources?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

type kindStatsOptions struct {
	prefix string
	exact  bool
	tables bool
	limit  int
	scan   bool
}

func kindStats(kind, namespace, name string, o kindStatsOptions) error {
	q := url.Values{"namespace": {namespace}, "name": {name}}
	if o.prefix != "" {
		q.Set("prefix", o.prefix)
	}
	if o.exact {
		q.Set("exact", "1")
	}
	if o.tables {
		q.Set("tables", "1")
	}
	if o.limit > 0 {
		q.Set("limit", strconv.Itoa(o.limit))
	}
	if kind == "r2" {
		q.Set("bucket", name)
		if o.scan {
			q.Set("scan", "1")
		}
	}
	data, err := adminCall(http.MethodGet, "/v1/"+kind+"/stats?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

// newKVCommand exposes namespace/key/bulk groups while keeping the direct
// `cellhive kv create|list|delete|stats` forms working.
func newKVCommand() *cobra.Command {
	kv := &cobra.Command{Use: "kv", Short: kindShort("kv")}
	kv.AddCommand(newKindRegistryGroup("kv", "namespace", "manage KV namespaces"))
	kv.AddCommand(newKindRegistryLeaves("kv")...)

	key := &cobra.Command{Use: "key", Short: "manage KV keys"}
	key.AddCommand(
		newKVKeyPutCommand(),
		newKVKeyReadCommand("get"),
		newKVKeyReadCommand("delete"),
		newKVKeyListCommand(),
	)
	bulk := &cobra.Command{Use: "bulk", Short: "bulk KV operations"}
	bulk.AddCommand(newKVBulkCommand("put"), newKVBulkCommand("delete"))
	kv.AddCommand(key, bulk)
	return kv
}

// newD1Command exposes registry CRUD/stats plus execute/query/export/migrations.
func newD1Command() *cobra.Command {
	d1 := &cobra.Command{Use: "d1", Short: kindShort("d1")}
	d1.AddCommand(newKindRegistryLeaves("d1")...)

	d1.AddCommand(
		newD1DataCommand("execute"),
		newD1DataCommand("query"),
		newD1ExportCommand(),
	)
	migrations := &cobra.Command{Use: "migrations", Short: "manage D1 migrations"}
	migrations.AddCommand(
		newD1MigrationsCommand("create"),
		newD1MigrationsCommand("list"),
		newD1MigrationsCommand("apply"),
	)
	d1.AddCommand(migrations)
	return d1
}

// newR2Command exposes bucket CRUD/stats plus object put/get/delete/list.
func newR2Command() *cobra.Command {
	r2 := &cobra.Command{Use: "r2", Short: kindShort("r2")}
	r2.AddCommand(newKindRegistryGroup("r2", "bucket", "manage R2 buckets"))
	r2.AddCommand(newKindRegistryLeaves("r2")...)

	object := &cobra.Command{Use: "object", Short: "manage R2 objects"}
	object.AddCommand(
		newR2ObjectCommand("put"),
		newR2ObjectCommand("get"),
		newR2ObjectCommand("delete"),
		newR2ObjectCommand("list"),
	)
	r2.AddCommand(object)
	return r2
}
