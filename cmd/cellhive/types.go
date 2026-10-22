package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"cellhive/internal/wrangler"
)

// newTypesCommand generates the Env interface from the same parsed configuration
// as deploy. The native command intentionally leaves runtime declarations to the
// project's Workers types package.
func newTypesCommand() *cobra.Command {
	var configPath, envName, interfaceName string
	var check bool
	cmd := &cobra.Command{
		Use:   "types [output.d.ts]",
		Short: "generate the Wrangler Env TypeScript interface",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			output := "worker-configuration.d.ts"
			if len(args) == 1 {
				output = args[0]
			}
			return typesCore(output, configPath, envName, interfaceName, check)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Wrangler JSONC/JSON configuration")
	cmd.Flags().StringVarP(&envName, "env", "e", "", "Wrangler environment")
	cmd.Flags().StringVar(&interfaceName, "env-interface", "Env", "generated interface name")
	cmd.Flags().BoolVar(&check, "check", false, "check that the generated file is current")
	return cmd
}

func typesCore(output, configPath, envName, interfaceName string, check bool) error {
	if configPath == "" {
		for _, name := range []string{"wrangler.jsonc", "wrangler.json"} {
			if _, err := os.Stat(name); err == nil {
				configPath = name
				break
			}
		}
		if configPath == "" {
			return fmt.Errorf("no wrangler.jsonc/json found; pass --config (wrangler.toml is not parsed by the Go CLI)")
		}
	}
	d, err := wrangler.Load(configPath, envName)
	if err != nil {
		return err
	}
	generated, err := wrangler.EnvTypes(d, interfaceName)
	if err != nil {
		return err
	}
	if check {
		current, err := os.ReadFile(output)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, generated) {
			return fmt.Errorf("%s is stale; rerun `cellhive types`", output)
		}
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(output), ".cellhive-types-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(generated); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), output)
}

// cmdTypes keeps the pre-cobra entry point used by in-process tests.
func cmdTypes(args []string) error {
	return dispatch(append([]string{"types"}, args...))
}
