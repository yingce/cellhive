package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newVectorizeCommand exposes the Cloudflare-compatible vectorize surface.
// Each leaf parses typed flags and calls the core implementation directly.
func newVectorizeCommand() *cobra.Command {
	vectorize := &cobra.Command{
		Use:   "vectorize",
		Short: "manage Vectorize indexes",
	}
	vectorize.AddCommand(
		newVectorizeCreateCommand(),
		newVectorizeSimpleCommand("list", 1),
		newVectorizeSimpleCommand("delete", 2),
		newVectorizeSimpleCommand("info", 2),
		newVectorizeSimpleCommand("stats", 2),
		newVectorizeInsertCommand("insert"),
		newVectorizeInsertCommand("upsert"),
		newVectorizeQueryCommand(),
		newVectorizeIDsCommand("get-vectors"),
		newVectorizeIDsCommand("delete-vectors"),
		newVectorizeListVectorsCommand(),
		newVectorizeMetadataIndexCommand("create-metadata-index"),
		newVectorizeMetadataIndexCommand("delete-metadata-index"),
		newVectorizeSimpleCommand("list-metadata-index", 2),
		newVectorizeRebuildCommand(),
		newVectorizeSimpleCommand("drop-ann", 2),
	)
	return vectorize
}

func newVectorizeCreateCommand() *cobra.Command {
	var dimensions int
	var metric, description string
	cmd := &cobra.Command{
		Use:   "create <namespace> <name>",
		Short: "create a Vectorize index",
		Args:  legacyArgs("usage: cellhive vectorize create <namespace> <name> --dimensions <n> --metric <cosine|euclidean|dot-product> [--description <text>]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeCreateCore(args[0], args[1], dimensions, metric, description)
		},
	}
	cmd.Flags().IntVar(&dimensions, "dimensions", 0, "vector dimensions (1..1536)")
	cmd.Flags().StringVar(&metric, "metric", "", "cosine|euclidean|dot-product")
	cmd.Flags().StringVar(&description, "description", "", "index description")
	return cmd
}

func newVectorizeSimpleCommand(verb string, nargs int) *cobra.Command {
	use := verb + " <namespace>"
	if nargs == 2 {
		use += " <name>"
	}
	var force bool
	cmd := &cobra.Command{
		Use:   use,
		Short: "vectorize " + verb,
		Args:  legacyArgs("usage: cellhive vectorize "+use, nargs, nargs),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch verb {
			case "list":
				return vectorizeListCore(args[0])
			case "delete":
				return vectorizeDeleteCore(args[0], args[1], force)
			case "info", "stats":
				return vectorizeInfoCore(args[0], args[1])
			case "list-metadata-index":
				return vectorizeListMetadataIndexCore(args[0], args[1])
			case "drop-ann":
				return vectorizeDropANNCore(args[0], args[1])
			default:
				return fmt.Errorf("unknown vectorize command %q", verb)
			}
		},
	}
	switch verb {
	case "delete":
		cmd.Aliases = []string{"rm"}
		cmd.Flags().BoolVar(&force, "force", false, "revoke even while worker versions still bind the index")
	case "list":
		cmd.Aliases = []string{"ls"}
	case "drop-ann":
		cmd.Aliases = []string{"drop-index"}
	case "list-metadata-index":
		cmd.Aliases = []string{"list-metadata-indexes"}
	}
	return cmd
}

func newVectorizeInsertCommand(verb string) *cobra.Command {
	var file string
	var batchSize int
	cmd := &cobra.Command{
		Use:   verb + " <namespace> <name>",
		Short: "vectorize " + verb,
		Args:  legacyArgs("usage: cellhive vectorize "+verb+" <namespace> <name> --file <ndjson> [--batch-size N]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeInsertCore(verb, args[0], args[1], file, batchSize)
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "ndjson file with {id, values, metadata?, namespace?} per line")
	cmd.Flags().IntVar(&batchSize, "batch-size", 1000, "vectors per request (max 1000)")
	return cmd
}

func newVectorizeQueryCommand() *cobra.Command {
	var vector, vectorID, returnMetadata, namespace, filter string
	var topK int
	var returnValues bool
	cmd := &cobra.Command{
		Use:   "query <namespace> <name>",
		Short: "query a Vectorize index",
		Args:  legacyArgs("usage: cellhive vectorize query <namespace> <name> (--vector '1,2,3' | --vector-id <id>) [--top-k N] [--return-values] [--return-metadata none|indexed|all] [--namespace <ns>] [--filter JSON]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeQueryCore(args[0], args[1], vectorQueryOptions{
				vector: vector, vectorID: vectorID, topK: topK, returnValues: returnValues,
				returnMetadata: returnMetadata, namespace: namespace, filter: filter,
			})
		},
	}
	cmd.Flags().StringVar(&vector, "vector", "", "comma-separated query vector")
	cmd.Flags().StringVar(&vectorID, "vector-id", "", "query using a stored vector id")
	cmd.Flags().IntVar(&topK, "top-k", 5, "number of matches")
	cmd.Flags().BoolVar(&returnValues, "return-values", false, "include vector values")
	cmd.Flags().StringVar(&returnMetadata, "return-metadata", "none", "none|indexed|all")
	cmd.Flags().StringVar(&namespace, "namespace", "", "namespace partition to search")
	cmd.Flags().StringVar(&filter, "filter", "", "metadata filter JSON")
	return cmd
}

func newVectorizeIDsCommand(verb string) *cobra.Command {
	var ids []string
	cmd := &cobra.Command{
		Use:   verb + " <namespace> <name>",
		Short: "vectorize " + verb,
		Args:  legacyArgs("usage: cellhive vectorize "+verb+" <namespace> <name> --ids <id> [<id>...]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeIDsCore(verb, args[0], args[1], ids)
		},
	}
	cmd.Flags().StringArrayVar(&ids, "ids", nil, "vector ids (repeatable)")
	return cmd
}

func newVectorizeListVectorsCommand() *cobra.Command {
	var count int
	var cursor string
	cmd := &cobra.Command{
		Use:   "list-vectors <namespace> <name>",
		Short: "list vector ids",
		Args:  legacyArgs("usage: cellhive vectorize list-vectors <namespace> <name> [--count N] [--cursor C]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeListVectorsCore(args[0], args[1], count, cursor)
		},
	}
	cmd.Flags().IntVar(&count, "count", 100, "ids per page (1..1000)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	return cmd
}

func newVectorizeMetadataIndexCommand(verb string) *cobra.Command {
	var property, typ string
	cmd := &cobra.Command{
		Use:   verb + " <namespace> <name>",
		Short: "vectorize " + verb,
		Args:  legacyArgs("usage: cellhive vectorize "+verb+" <namespace> <name> --property-name <p> [--type string|number|boolean]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeMetadataIndexCore(verb, args[0], args[1], property, typ)
		},
	}
	cmd.Flags().StringVar(&property, "property-name", "", "metadata property")
	cmd.Flags().StringVar(&typ, "type", "", "string|number|boolean")
	return cmd
}

func newVectorizeRebuildCommand() *cobra.Command {
	var buckets, codesize int
	var quantizer string
	var nprobe float64
	cmd := &cobra.Command{
		Use:     "rebuild <namespace> <name>",
		Aliases: []string{"build-ann"},
		Short:   "rebuild the ANN index",
		Args:    legacyArgs("usage: cellhive vectorize rebuild <namespace> <name> [--buckets N] [--quantizer none|pq|opq|bq] [--codesize N] [--nprobe F]", 2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return vectorizeRebuildCore(args[0], args[1], buckets, quantizer, codesize, nprobe)
		},
	}
	cmd.Flags().IntVar(&buckets, "buckets", 1024, "IVF buckets (0 = exhaustive)")
	cmd.Flags().StringVar(&quantizer, "quantizer", "opq", "none|pq|opq|bq")
	cmd.Flags().IntVar(&codesize, "codesize", 32, "quantized vector bytes (0 = default)")
	cmd.Flags().Float64Var(&nprobe, "nprobe", 0.05, "buckets probed: fraction (<1) or count (>=1)")
	return cmd
}
