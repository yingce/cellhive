package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Data-plane commands parse flags with pflag and call the typed core functions
// in kv_core.go/r2_core.go/d1_core.go; those cores remain the single HTTP
// implementation for both the cobra tree and compatibility shims.

func newKVKeyPutCommand() *cobra.Command {
	var path, ttl, expiration, metadata string
	var remote bool
	cmd := &cobra.Command{
		Use:   "put <namespace> <binding> <key> [value]",
		Short: "put a KV key",
		Args:  legacyArgs("usage: cellhive kv key put <namespace> <binding> <key> [value] [--path <file>|--ttl <seconds>|--expiration <unix>|--metadata <json>]", 3, 4),
		RunE: func(cmd *cobra.Command, args []string) error {
			value := ""
			if len(args) == 4 {
				value = args[3]
			}
			return kvPutCore(args[0], args[1], args[2], kvPutOptions{
				value: value, path: path, ttl: ttl, expiration: expiration, metadata: metadata,
			})
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "read the value from a file")
	cmd.Flags().StringVar(&ttl, "ttl", "", "expiration TTL in seconds")
	cmd.Flags().StringVar(&expiration, "expiration", "", "absolute expiration (unix seconds)")
	cmd.Flags().StringVar(&metadata, "metadata", "", "JSON metadata")
	cmd.Flags().BoolVar(&remote, "remote", false, "accepted for Wrangler compatibility (CellHive is remote)")
	_ = remote
	return cmd
}

func newKVKeyReadCommand(verb string) *cobra.Command {
	var remote bool
	cmd := &cobra.Command{
		Use:   verb + " <namespace> <binding> <key>",
		Short: "KV key " + verb,
		Args:  legacyArgs("usage: cellhive kv key "+verb+" <namespace> <binding> <key>", 3, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			if verb == "get" {
				return kvGetCore(args[0], args[1], args[2])
			}
			return kvDeleteCore(args[0], args[1], args[2])
		},
	}
	cmd.Flags().BoolVar(&remote, "remote", false, "accepted for Wrangler compatibility (CellHive is remote)")
	_ = remote
	return cmd
}

func newKVKeyListCommand() *cobra.Command {
	var prefix string
	var remote bool
	cmd := &cobra.Command{
		Use:   "list <namespace> <binding> [prefix]",
		Short: "list KV keys",
		Args:  legacyArgs("usage: cellhive kv key list <namespace> <binding> [prefix]", 2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			p := prefix
			if p == "" && len(args) == 3 {
				p = args[2]
			}
			return kvListCore(args[0], args[1], p)
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "key prefix")
	cmd.Flags().BoolVar(&remote, "remote", false, "accepted for Wrangler compatibility (CellHive is remote)")
	_ = remote
	return cmd
}

func newKVBulkCommand(verb string) *cobra.Command {
	return &cobra.Command{
		Use:   verb + " <namespace> <binding> <json-file>",
		Short: "KV bulk " + verb,
		Args:  legacyArgs("usage: cellhive kv bulk "+verb+" <namespace> <binding> <json-file>", 3, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return kvBulkCore(verb, args[0], args[1], args[2])
		},
	}
}

func newD1DataCommand(verb string) *cobra.Command {
	var commands []string
	var file string
	var remote, local bool
	cmd := &cobra.Command{
		Use:   verb + " <namespace> <database> [sql]",
		Short: "run SQL against a D1 database (" + verb + ")",
		Args:  legacyArgs("usage: cellhive d1 "+verb+" <namespace> <database> <sql>|--command <sql>|--file <path> [--remote]", 2, -1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if local {
				return fmt.Errorf("local D1 uses `cellhive dev`; this command targets the CellHive database")
			}
			return d1DataCore(verb, args[0], args[1], args[2:], commands, file)
		},
	}
	cmd.Flags().StringArrayVar(&commands, "command", nil, "SQL statement (repeatable)")
	cmd.Flags().StringVar(&file, "file", "", "read SQL from a file")
	cmd.Flags().BoolVar(&remote, "remote", false, "accepted for Wrangler compatibility (CellHive is remote)")
	cmd.Flags().BoolVar(&local, "local", false, "local mode is not supported by this binary")
	return cmd
}

func newD1ExportCommand() *cobra.Command {
	var output string
	var noSchema, noData bool
	var tables []string
	cmd := &cobra.Command{
		Use:   "export <namespace> <database>",
		Short: "export a D1 database to SQL",
		Args:  legacyArgs("usage: cellhive d1 export <namespace> <database> --output <file> [--table <name>] [--no-schema|--no-data]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return d1ExportCore(args[0], args[1], output, noSchema, noData, tables)
		},
	}
	cmd.Flags().StringVar(&output, "output", "", "SQL output file")
	cmd.Flags().BoolVar(&noSchema, "no-schema", false, "export only data")
	cmd.Flags().BoolVar(&noData, "no-data", false, "export only schema")
	cmd.Flags().StringArrayVar(&tables, "table", nil, "table to include (repeatable)")
	return cmd
}

func newD1MigrationsCommand(verb string) *cobra.Command {
	var directory, table string
	var remote, local, applied bool
	use := verb + " <namespace> <database>"
	args := legacyArgs("usage: cellhive d1 migrations "+verb+" <namespace> <database> [message] [--directory dir]", 2, 2)
	if verb == "create" {
		use += " <message>"
		args = legacyArgs("usage: cellhive d1 migrations create <namespace> <database> <message> [--directory dir]", 3, 3)
	}
	cmd := &cobra.Command{
		Use:   use,
		Short: "D1 migrations " + verb,
		Args:  args,
		RunE: func(cmd *cobra.Command, args []string) error {
			message := ""
			if verb == "create" {
				message = args[2]
			}
			return d1MigrationsCore(verb, args[0], args[1], message, d1MigrationsOptions{
				directory: directory, table: table, remote: remote, local: local, applied: applied,
			})
		},
	}
	cmd.Flags().StringVar(&directory, "directory", "", "migration directory (default migrations)")
	cmd.Flags().StringVar(&table, "table", "", "migration tracking table (default d1_migrations)")
	cmd.Flags().BoolVar(&remote, "remote", true, "use the CellHive database")
	cmd.Flags().BoolVar(&local, "local", false, "local mode is not supported by this binary")
	if verb == "list" {
		cmd.Flags().BoolVar(&applied, "applied", false, "list applied records instead of pending files")
	}
	return cmd
}

func newR2ObjectCommand(verb string) *cobra.Command {
	var file, prefix string
	var remote bool
	var meta r2Metadata
	use := verb + " <namespace> <bucket> <key>"
	args := legacyArgs("usage: cellhive r2 object "+verb+" <namespace> <bucket> <key>", 3, 3)
	if verb == "put" {
		use += " [file]"
		args = legacyArgs("usage: cellhive r2 object put <namespace> <bucket> <key> [file] [--file <path>]", 3, 4)
	}
	if verb == "list" {
		use += " [prefix]"
		args = legacyArgs("usage: cellhive r2 object list <namespace> <bucket> [prefix] [--prefix <prefix>]", 3, 4)
	}
	cmd := &cobra.Command{
		Use:   use,
		Short: "R2 object " + verb,
		Args:  args,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch verb {
			case "put":
				return r2ObjectPutCore(args[0], args[1], args[2], file, meta)
			case "get":
				return r2ObjectGetCore(args[0], args[1], args[2], file)
			case "delete":
				return r2ObjectDeleteCore(args[0], args[1], args[2])
			case "list":
				p := prefix
				if p == "" && len(args) == 4 {
					p = args[3]
				}
				return r2ObjectListCore(args[0], args[1], p)
			default:
				return fmt.Errorf("unknown r2 object verb %q (put|get|delete|list)", verb)
			}
		},
	}
	if verb == "put" || verb == "get" {
		cmd.Flags().StringVar(&file, "file", "", "local file (put: source, get: destination)")
	}
	if verb == "list" {
		cmd.Flags().StringVar(&prefix, "prefix", "", "list prefix")
	}
	cmd.Flags().BoolVar(&remote, "remote", false, "accepted for Wrangler compatibility (CellHive is remote)")
	if verb == "put" {
		cmd.Flags().StringVar(&meta.contentType, "content-type", "", "HTTP content type")
		cmd.Flags().StringVar(&meta.contentDisposition, "content-disposition", "", "HTTP content disposition")
		cmd.Flags().StringVar(&meta.contentEncoding, "content-encoding", "", "HTTP content encoding")
		cmd.Flags().StringVar(&meta.contentLanguage, "content-language", "", "HTTP content language")
		cmd.Flags().StringVar(&meta.cacheControl, "cache-control", "", "HTTP cache control")
		cmd.Flags().StringVar(&meta.expires, "expires", "", "HTTP expiry")
	}
	_ = remote
	return cmd
}
