package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"cellhive/internal/wrangler"
)

type deployOptions struct {
	bundleFile    string
	bundleSHA     string
	assetsSHA     string
	assetsDir     string
	route         string
	configPath    string
	envName       string
	consumers     []string
	crons         []string
	migrations    string
	idempotency   string
	kv            []string
	d1            []string
	r2            []string
	queue         []string
	service       []string
	workflow      []string
	hyperdrive    []string
	assets404     string
	runWorker     bool
	runWorkerPath []string
	dryRun        bool
	vars          []string
	secretsFile   string
}

func newDeployCommand() *cobra.Command {
	var o deployOptions
	cmd := &cobra.Command{
		Use:   "deploy <namespace> <worker>",
		Short: "deploy a worker version",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deployCore(args[0], args[1], o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.bundleFile, "bundle", "", "local bundle file: hash, upload, then deploy")
	f.StringVar(&o.bundleSHA, "bundle-sha", "", "content-addressed bundle sha256")
	f.StringVar(&o.assetsSHA, "assets-sha", "", "assets sha256")
	f.StringVar(&o.assetsDir, "assets-dir", "", "local assets directory: upload all files under one version token, then deploy")
	f.StringVar(&o.route, "route", "", "optional host route to set for this worker")
	f.StringVar(&o.configPath, "config", "", "wrangler.jsonc/json to map (instead of explicit flags)")
	f.StringVar(&o.envName, "env", "", "environment within --config")
	f.StringArrayVar(&o.consumers, "consumer", nil, "queue consumer: <queue>[:maxRetries[:deadLetterQueue[:maxConcurrency]]] (repeatable)")
	f.StringArrayVar(&o.crons, "cron", nil, "cron expression this worker handles via scheduled() (repeatable)")
	f.StringVar(&o.migrations, "migrations", "", "DO migrations JSON array (e.g. '[{\"tag\":\"v1\",\"new_sqlite_classes\":[\"Room\"]}]')")
	f.StringVar(&o.idempotency, "idempotency-key", "", "de-duplicate a repeated deploy (returns the first version)")
	f.StringArrayVar(&o.kv, "kv", nil, "KV binding NAME=ID (repeatable)")
	f.StringArrayVar(&o.d1, "d1", nil, "D1 binding NAME=ID (repeatable)")
	f.StringArrayVar(&o.r2, "r2", nil, "R2 binding NAME=BUCKET (repeatable)")
	f.StringArrayVar(&o.queue, "queue", nil, "Queue binding NAME=QUEUE (repeatable)")
	f.StringArrayVar(&o.service, "service", nil, "Service binding NAME=TARGET_WORKER (repeatable)")
	f.StringArrayVar(&o.workflow, "workflow", nil, "Workflow binding NAME=RESOURCE:CLASS (repeatable)")
	f.StringArrayVar(&o.hyperdrive, "hyperdrive", nil, "Hyperdrive binding NAME=<registered-resource|[postgres|mysql]://url> (repeatable)")
	f.StringVar(&o.assets404, "assets-not-found", "", "assets not_found_handling: none|404-page|single-page-application")
	f.BoolVar(&o.runWorker, "run-worker-first", false, "run the worker before assets")
	f.StringArrayVar(&o.runWorkerPath, "run-worker-first-path", nil, "path prefix where the worker runs before assets (repeatable)")
	f.BoolVar(&o.dryRun, "dry-run", false, "validate only: compatibility gate + bundle + ACL, without deploying")
	f.StringArrayVar(&o.vars, "var", nil, "var override NAME=VALUE (repeatable; merges over config vars)")
	f.StringVar(&o.secretsFile, "secrets-file", "", "JSON object {\"KEY\":\"value\"} uploaded as secrets before deploying")
	return cmd
}

func deployCore(ns, worker string, o deployOptions) error {
	if o.bundleFile != "" {
		sha, err := uploadBundle(o.bundleFile)
		if err != nil {
			return err
		}
		o.bundleSHA = sha
	}

	// --config maps a wrangler.jsonc/json to the deploy request (ADR-124): the
	// worker name may come from the config, and main/assets are bundled/uploaded
	// unless the caller passed explicit overrides.
	var mapped *wrangler.Deploy
	if o.configPath != "" {
		if o.bundleFile != "" || o.bundleSHA != "" || len(o.kv)+len(o.d1)+len(o.r2)+len(o.queue)+len(o.service)+len(o.workflow)+len(o.hyperdrive) > 0 ||
			len(o.consumers) > 0 || len(o.crons) > 0 || o.assetsSHA != "" || o.assetsDir != "" || o.migrations != "" {
			return fmt.Errorf("--config cannot be combined with explicit binding/bundle/assets flags")
		}
		m, err := wrangler.Load(o.configPath, o.envName)
		if err != nil {
			return err
		}
		mapped = m
		if worker == "" || worker == "-" {
			worker = m.Worker
		}
		if worker == "" {
			return fmt.Errorf("worker name is required (positional or config name)")
		}
		// Local preflight with the registered-resource set, then the server
		// re-validates authoritatively.
		registered, err := registeredResources(ns)
		if err != nil {
			return fmt.Errorf("read registered resources before deploy: %w", err)
		}
		if findings := m.ValidateWith(registered); len(findings) > 0 {
			for _, f := range findings {
				fmt.Fprintf(os.Stderr, "%s: %s: %s\n", f.Code, f.FieldPath, f.Message)
			}
			return fmt.Errorf("config rejected by the compatibility gate")
		}
		if o.bundleSHA == "" {
			sha, err := bundleFromConfig(m)
			if err != nil {
				return err
			}
			o.bundleSHA = sha
		}
		if o.assetsSHA == "" && m.AssetsDir != "" && !o.dryRun {
			tok, err := uploadAssetsDir(ns, worker, m.AssetsDir)
			if err != nil {
				return err
			}
			o.assetsSHA = tok
		}
	}
	if o.bundleSHA == "" {
		return fmt.Errorf("--bundle, --bundle-sha or --config with a main is required")
	}
	if o.assetsDir != "" && o.assetsSHA == "" && !o.dryRun {
		tok, err := uploadAssetsDir(ns, worker, o.assetsDir)
		if err != nil {
			return err
		}
		o.assetsSHA = tok
	}
	// --secrets-file uploads secrets before the deploy so the first request
	// already sees them (wrangler semantics). Skipped for --dry-run.
	if o.secretsFile != "" && !o.dryRun {
		if err := putSecretsFile(ns, worker, o.secretsFile); err != nil {
			return err
		}
	}
	payload := map[string]any{
		"namespace": ns, "worker": worker, "bundle_sha": o.bundleSHA, "assets_sha": o.assetsSHA,
		"consumers": consumeSpecs(o.consumers),
		"crons":     []string(o.crons),
	}
	if mapped != nil {
		if mapped.Limits != nil {
			payload["limits"] = mapped.Limits
		}
		if mapped.CompatibilityDate != "" {
			payload["compatibility_date"] = mapped.CompatibilityDate
		}
		if len(mapped.CompatibilityFlags) > 0 {
			payload["compatibility_flags"] = mapped.CompatibilityFlags
		}
		if len(mapped.Vars) > 0 {
			payload["vars"] = mapped.Vars
		}
		if len(mapped.Bindings) > 0 {
			payload["bindings"] = mapped.Bindings
		}
		if len(mapped.Migrations) > 0 {
			payload["migrations"] = mapped.Migrations
		}
		if mapped.Assets != nil {
			payload["assets"] = mapped.Assets
		}
		if len(mapped.Consumers) > 0 {
			payload["consumers"] = mapped.Consumers
		}
		if len(mapped.Crons) > 0 {
			payload["crons"] = mapped.Crons
		}
	}
	// --var overrides win over config vars.
	if len(o.vars) > 0 {
		merged := map[string]string{}
		if m, ok := payload["vars"].(map[string]string); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
		for _, kv := range o.vars {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return fmt.Errorf("--var must be NAME=VALUE, got %q", kv)
			}
			merged[k] = v
		}
		payload["vars"] = merged
	}
	if o.dryRun {
		payload["dry_run"] = true
	}
	if o.idempotency != "" {
		payload["idempotency_key"] = o.idempotency
	}
	if bs := buildBindings(o.kv, o.d1, o.r2, o.queue, o.service, o.workflow, o.hyperdrive); len(bs) > 0 {
		payload["bindings"] = bs
	}
	if o.migrations != "" {
		var migs []map[string]any
		if err := json.Unmarshal([]byte(o.migrations), &migs); err != nil {
			return fmt.Errorf("--migrations must be a JSON array: %w", err)
		}
		payload["migrations"] = migs
	}
	if o.assets404 != "" || o.runWorker || len(o.runWorkerPath) > 0 {
		payload["assets"] = map[string]any{
			"not_found_handling":     o.assets404,
			"run_worker_first":       o.runWorker,
			"run_worker_first_paths": []string(o.runWorkerPath),
		}
	}
	data, err := adminCall(http.MethodPost, "/v1/control/deploy", payload)
	if err != nil {
		return err
	}
	if o.route != "" && !o.dryRun {
		if _, err := adminCall(http.MethodPost, "/v1/control/route", map[string]string{
			"namespace": ns, "host": o.route, "worker": worker,
		}); err != nil {
			return err
		}
	}
	printJSON(data)
	return nil
}

func newPromoteCommand() *cobra.Command {
	var version int
	cmd := &cobra.Command{
		Use:   "promote <namespace> <worker>",
		Short: "promote a worker version",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if version == 0 {
				return fmt.Errorf("--version is required")
			}
			data, err := adminCall(http.MethodPost, "/v1/control/promote", map[string]any{
				"namespace": args[0], "worker": args[1], "version": version,
			})
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "version number to make active")
	return cmd
}

func newRollbackCommand() *cobra.Command {
	var idempotencyKey string
	cmd := &cobra.Command{
		Use:   "rollback <namespace> <worker>",
		Short: "roll back a worker to its previous version",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := adminCall(http.MethodPost, "/v1/control/rollback", map[string]string{
				"namespace": args[0], "worker": args[1], "idempotency_key": idempotencyKey,
			})
			if err != nil {
				return err
			}
			printJSON(data)
			return nil
		},
	}
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "return the first rollback result on repeated requests with this key")
	return cmd
}

// cmdDeploy keeps the pre-cobra entry point used by in-process e2e tests.
func cmdDeploy(args []string) error {
	return dispatch(append([]string{"deploy"}, args...))
}
