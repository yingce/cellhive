// Command do-runtime launches the CellHive do-runtime workerd process
// (docs/durable-objects.md): a native Durable Object host actor with local-disk
// SQLite storage (ADR-077).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cellhive/internal/config"
	"cellhive/internal/doruntime"
)

func main() {
	renderOnly := flag.Bool("render-only", false, "render the workerd config, print its path and exit (for do-supervisor -config)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	for _, w := range config.LegacyEnvWarnings() {
		log.Warn(w)
	}

	cellURL := getenv("CELLHIVE_CELL_URL", "http://127.0.0.1:7001")
	// Role credentials are HKDF-derived from CELLHIVE_ROOT_KEY (ADR-137).
	creds := config.DeriveCredentials(config.LoadRootKey())
	if creds.Internal == "" {
		log.Error("CELLHIVE_ROOT_KEY is required (or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use)")
		os.Exit(1)
	}
	cellToken := creds.Internal
	addr := getenv("CELLHIVE_DO_ADDR", "*:8788")
	nodeID := getenv("CELLHIVE_DO_NODE", hostname())
	advertise := getenv("CELLHIVE_DO_ADVERTISE", localURL(addr))
	preventEviction, err := boolEnv("CELLHIVE_DO_PREVENT_EVICTION", true)
	if err != nil {
		log.Error("invalid CELLHIVE_DO_PREVENT_EVICTION", "err", err)
		os.Exit(1)
	}
	dataDir := getenv("CELLHIVE_DATA_DIR", "./.cellhive/data")
	runtimeDir := filepath.Join(runtimeRoot(), "do-runtime")
	// Local DO storage is a disposable working copy; the bucket/cell-agent keep
	// the authority (ADR-077). Always under DATA_DIR.
	diskDir := filepath.Join(dataDir, "do")
	platformJS := getenv("CELLHIVE_DO_RUNTIME_JS", "workerd/do-runtime")
	gateURL := getenv("CELLHIVE_DO_GATE_URL", "")

	capnpPath, err := doruntime.Render(runtimeDir, doruntime.Config{
		CellURL:         cellURL,
		LogToken:        creds.Log,
		DoTicketSecret:  creds.DoTicket,
		DoLeaseS:        doLeaseSeconds(),
		DOObjectIndex:   envTrue("CELLHIVE_DO_OBJECT_INDEX"),
		AIURL:           getenv("CELLHIVE_AI_URL", ""),
		AIKey:           getenv("CELLHIVE_AI_KEY", ""),
		CellToken:       cellToken,
		Addr:            addr,
		DiskDir:         diskDir,
		PlatformJS:      platformJS,
		NodeID:          nodeID,
		Advertise:       advertise,
		PreventEviction: preventEviction,
		GateURL:         gateURL,
		OutboundAllow:   outboundAllow(),
	})
	if err != nil {
		log.Error("render config", "err", err)
		os.Exit(1)
	}
	if *renderOnly {
		// Print the rendered config path so a supervisor can spawn workerd under
		// the ADR-083 output gate.
		fmt.Println(capnpPath)
		return
	}
	workerd, err := doruntime.FindWorkerd()
	if err != nil {
		log.Error("find workerd", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("do-runtime starting", "workerd", workerd, "config", capnpPath, "addr", addr,
		"disk", diskDir, "cell", cellURL, "node", nodeID, "resident", preventEviction)

	// Lease renewal loop (local 127.0.0.1; never a service alias, ADR-078).
	go doruntime.RenewLoop(ctx, localURL(addr), cellToken, doruntime.RenewEvery,
		func(err error) { log.Warn("do renew failed", "err", err) })
	// On shutdown, drain (stop new work, wait in-flight, release leases) then
	// exit — killing workerd directly rather than relying on a graceful window.
	go func() {
		<-ctx.Done()
		if err := doruntime.Drain(localURL(addr), cellToken); err != nil {
			log.Warn("do drain failed; relying on lease expiry", "err", err)
			return
		}
		log.Info("do-runtime drained")
	}()

	if err := doruntime.Run(ctx, workerd, capnpPath); err != nil {
		log.Error("do-runtime exited", "err", err)
		os.Exit(1)
	}
}

// localURL turns a workerd listen address like "*:8788" into a local base URL.
func localURL(addr string) string {
	host, port, ok := strings.Cut(addr, ":")
	if !ok {
		return "http://127.0.0.1:8788"
	}
	if host == "" || host == "*" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return "http://" + host + ":" + port
}

func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "do-runtime"
}

func boolEnv(k string, def bool) (bool, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("%s must be exactly true or false, got %q", k, v)
}

// envTrue parses a boolean-ish env var the same way config.envBool does for
// cell-agent (true/1), so both sides agree on CELLHIVE_DO_OBJECT_INDEX.
func envTrue(k string) bool {
	v := os.Getenv(k)
	return v == "true" || v == "1"
}

// runtimeRoot is the base directory for disposable runtime scratch (capnp
// configs, workerd state). Defaults to $TMPDIR/cellhive; one knob for every
// runtime instead of a var per service (ADR-137).
func runtimeRoot() string {
	return getenv("CELLHIVE_RUNTIME_DIR", filepath.Join(os.TempDir(), "cellhive"))
}

// doLeaseSeconds renders CELLHIVE_DO_LEASE (Go duration, default 30s) as the
// whole-second string the DO host actor expects.
func doLeaseSeconds() string {
	d := envDuration("CELLHIVE_DO_LEASE", 30*time.Second)
	s := int64(d / time.Second)
	if s < 1 {
		s = 1
	}
	return strconv.FormatInt(s, 10)
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// outboundAllow parses CELLHIVE_TENANT_OUTBOUND (comma-separated workerd network
// categories: public|private|local). Empty = public only (ADR-130).
func outboundAllow() []string {
	raw := strings.TrimSpace(os.Getenv("CELLHIVE_TENANT_OUTBOUND"))
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
