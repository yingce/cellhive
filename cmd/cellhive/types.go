package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"cellhive/internal/wrangler"
)

// cmdTypes generates the Env interface from the same parsed configuration as
// deploy. The native command intentionally leaves runtime declarations to the
// project's Workers types package.
func cmdTypes(args []string) error {
	output := "worker-configuration.d.ts"
	if len(args) != 0 && len(args[0]) != 0 && args[0][0] != '-' {
		output, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("types", flag.ContinueOnError)
	configPath := fs.String("config", "", "Wrangler JSONC/JSON configuration")
	fs.StringVar(configPath, "c", "", "Wrangler JSONC/JSON configuration")
	envName := fs.String("env", "", "Wrangler environment")
	fs.StringVar(envName, "e", "", "Wrangler environment")
	interfaceName := fs.String("env-interface", "Env", "generated interface name")
	check := fs.Bool("check", false, "check that the generated file is current")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("usage: cellhive types [output.d.ts] [--config wrangler.jsonc] [--env name] [--env-interface Env] [--check]")
	}
	if *configPath == "" {
		for _, name := range []string{"wrangler.jsonc", "wrangler.json"} {
			if _, err := os.Stat(name); err == nil {
				*configPath = name
				break
			}
		}
		if *configPath == "" {
			return fmt.Errorf("no wrangler.jsonc/json found; pass --config (wrangler.toml is not parsed by the Go CLI)")
		}
	}
	d, err := wrangler.Load(*configPath, *envName)
	if err != nil {
		return err
	}
	generated, err := wrangler.EnvTypes(d, *interfaceName)
	if err != nil {
		return err
	}
	if *check {
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
