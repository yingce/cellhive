package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// errNoCommand marks the "no subcommand" case so main can keep the historical
// exit code 2 without printing an extra error prefix.
var errNoCommand = errors.New("no command")

// newRoot builds the cobra command tree. During the migration the leaves are
// compatibility wrappers around the existing cmdXxx handlers: cobra owns
// routing/help/completion, while each handler keeps its stdlib flag parsing
// until its domain is migrated to pflag (ADR-265).
func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "cellhive",
		Short:         "CellHive CLI: control plane, data plane, and Wrangler-compatible aliases",
		Long:          "CellHive CLI. Native commands are the canonical surface; `cellhive wrangler ...` is a compatibility translator over them (ADR-138/148/265).",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				_ = cmd.Help()
				return errNoCommand
			}
			return fmt.Errorf("unknown command %q", args[0])
		},
	}
	root.SetVersionTemplate("cellhive {{.Version}}\n")

	root.AddCommand(
		newVersionCommand(),
		newDiagnoseCommand(),
		newLoginCommand(),
		newAuthCommand(),
		newProjectCommand(),
		newEnvCommand(),
		newSkillsCommand(),

		newAppCommand(),
		newResourceCommand(),
		newDomainCommand(),
		newDeployCommand(),
		newPromoteCommand(),
		newRollbackCommand(),
		newRouteCommand(),
		newSecretCommand(),
		newBundleCommand(),
		newAssetCommand(),
		newWorkerCommand(),
		newWorkflowCommand(),
		newReleasesCommand(),
		newTailCommand(),

		newCredsCommand(),
		newTokenCommand(),
		newStatusCommand(),
		newCapacityCommand(),
		newGCCommand(),
		newRoutesCommand(),
		newAuditCommand(),
		newServiceACLCommand(),

		newQueueCommand(),
		newKVCommand(),
		newD1Command(),
		newR2Command(),
		newKindCommand("hyperdrive"),
		newVectorizeCommand(),

		// Native implementations that also happen to be Wrangler command names.
		newTypesCommand(),
		newInitCommand(),

		// Wrangler compatibility surface: translated and re-entered, never a
		// second implementation (ADR-138/148/265).
		newLegacyCommand("wrangler", "run a Wrangler-compatible command alias", cmdWrangler),

		// Compatibility aliases are kept working but hidden from primary help.
		newHiddenLegacyCommand("versions", "Wrangler-compatible version aliases", compat("versions")),
		newHiddenLegacyCommand("deployments", "Wrangler-compatible deployment aliases", compat("deployments")),
		newHiddenLegacyCommand("triggers", "Wrangler-compatible trigger aliases", compat("triggers")),
		newHiddenLegacyCommand("queues", "Wrangler-compatible queue aliases", compat("queues")),
		newHiddenLegacyCommand("workflows", "Wrangler-compatible workflow aliases", compat("workflows")),
	)

	return root
}

// legacyArgs preserves the pre-cobra usage error for migrated leaves that still
// rely on their handler for detailed validation.
func legacyArgs(usage string, min, max int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) < min || (max >= 0 && len(args) > max) {
			return errors.New(usage)
		}
		return nil
	}
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "print the CellHive CLI version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("cellhive", version)
			return nil
		},
	}
}

// newLegacyCommand wraps an existing handler. DisableFlagParsing keeps the
// handler's stdlib flag semantics (including -c/--config and repeated flags).
// A lone -h/--help renders the cobra help; help after positional arguments is
// left to the handler so its stdlib flag listing is still available.
func newLegacyCommand(use, short string, run func([]string) error) *cobra.Command {
	return &cobra.Command{
		Use:                use,
		Short:              short,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
				return cmd.Help()
			}
			return run(args)
		},
	}
}

// newHiddenLegacyCommand is newLegacyCommand without a primary-help entry.
func newHiddenLegacyCommand(use, short string, run func([]string) error) *cobra.Command {
	c := newLegacyCommand(use, short, run)
	c.Hidden = true
	return c
}

// compat binds a hidden alias (versions/queues/...) back to cmdCompat, which
// expects the alias name as args[0].
func compat(name string) func([]string) error {
	return func(args []string) error {
		return cmdCompat(append([]string{name}, args...))
	}
}

// dispatch runs a top-level command. It is separate from main so that the
// `wrangler` namespace can translate its arguments and re-enter it (ADR-138).
func dispatch(args []string) error {
	root := newRoot()
	root.SetArgs(normalizeLegacyLongFlags(root, args))
	return root.Execute()
}

// normalizeLegacyLongFlags restores the stdlib-flag convenience of single-dash
// long flags (`-config`, `-force`) for cobra commands that define the long name.
// Shorthands (`-c`, `-v`, `-h`) and `--` terminator semantics are untouched.
func normalizeLegacyLongFlags(root *cobra.Command, args []string) []string {
	cmd, _, err := root.Find(args)
	if err != nil {
		cmd = root
	}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			out = append(out, args[i:]...)
			return out
		}
		if len(a) > 2 && a[0] == '-' && a[1] != '-' {
			name := a[1:]
			value := ""
			hasValue := false
			if k, v, ok := strings.Cut(name, "="); ok {
				name, value, hasValue = k, v, true
			}
			if cmd.Flags().Lookup(name) != nil || cmd.InheritedFlags().Lookup(name) != nil {
				a = "--" + name
				if hasValue {
					a += "=" + value
				}
			}
		}
		out = append(out, a)
	}
	return out
}

// isUnknownCommand reports whether err came from an unknown command name, which
// historically exits 2. Flag and handler errors keep exit code 1.
func isUnknownCommand(err error) bool {
	return strings.HasPrefix(err.Error(), "unknown command")
}
