package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"cellhive/internal/wranglercompat"
)

// newInitCommand creates a small project that can be deployed by either
// CellHive or official Wrangler. It never calls a remote API or installs
// dependencies.
func newInitCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "init <worker-name>",
		Short: "create a minimal deployable Worker project",
		Args:  legacyArgs("usage: cellhive init <worker-name> [--yes] (lowercase letters, digits and hyphens only)", 1, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !workerProjectName(args[0]) {
				return fmt.Errorf("usage: cellhive init <worker-name> [--yes] (lowercase letters, digits and hyphens only)")
			}
			return initCore(args[0], yes)
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "create without prompting")
	return cmd
}

func initCore(name string, yes bool) error {
	if err := os.Mkdir(name, 0755); err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(name)
		}
	}()
	if err := os.Mkdir(filepath.Join(name, "src"), 0755); err != nil {
		return err
	}
	config, err := json.MarshalIndent(map[string]string{
		"name": name, "main": "src/index.js", "compatibility_date": wranglercompat.MaxCompatibilityDate,
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(name, "wrangler.jsonc"), append(config, '\n'), 0644); err != nil {
		return err
	}
	source := "export default {\n  async fetch(request, env, ctx) {\n    return new Response(\"Hello from " + name + "!\");\n  },\n};\n"
	if err := os.WriteFile(filepath.Join(name, "src", "index.js"), []byte(source), 0644); err != nil {
		return err
	}
	complete = true
	fmt.Printf("Created %s/ (wrangler.jsonc, src/index.js)\n", name)
	return nil
}

func workerProjectName(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// cmdInit keeps the pre-cobra entry point used by in-process tests.
func cmdInit(args []string) error {
	return dispatch(append([]string{"init"}, args...))
}
