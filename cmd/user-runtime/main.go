// Command user-runtime launches the CellHive user-runtime workerd process
// (docs/architecture.md): public loader :8081 + internal privileged dispatch
// :8088 that runs tenant queue()/scheduled() handlers.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cellhive/internal/config"
	"cellhive/internal/userruntime"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	for _, w := range config.LegacyEnvWarnings() {
		log.Warn(w)
	}

	cellURL := getenv("CELLHIVE_CELL_URL", "http://127.0.0.1:7001")
	// All role credentials are HKDF-derived from CELLHIVE_ROOT_KEY (ADR-137),
	// so one secret configured everywhere keeps the roles in sync.
	creds := config.DeriveCredentials(config.LoadRootKey())
	if creds.Internal == "" {
		log.Error("CELLHIVE_ROOT_KEY is required (or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use)")
		os.Exit(1)
	}
	cellToken := creds.Internal
	scopeSecret := creds.Scope
	dispatchToken := creds.Dispatch
	internalPort := envInt("CELLHIVE_USER_RUNTIME_INTERNAL_PORT", 8088)
	publicPort := envInt("CELLHIVE_USER_RUNTIME_PORT", 8081)
	dataDir := filepath.Join(runtimeRoot(), "user-runtime")

	platformJS := getenv("CELLHIVE_USER_RUNTIME_JS", "workerd/user-runtime")
	facadesJS := getenv("CELLHIVE_FACADES_JS", "workerd/platform/facades.js")

	runtimeCfg := userruntime.Config{
		CellURL:          cellURL,
		CellToken:        cellToken,
		AIURL:            getenv("CELLHIVE_AI_URL", ""),
		AIKey:            getenv("CELLHIVE_AI_KEY", ""),
		InternalPort:     internalPort,
		PublicPort:       publicPort,
		PlatformJS:       platformJS,
		FacadesJS:        facadesJS,
		ScopeSecret:      scopeSecret,
		DispatchToken:    dispatchToken,
		ServiceNative:    getenv("CELLHIVE_SERVICE_NATIVE", ""),
		TraceSampleRatio: traceRatio(),
		DoDirect:         getenv("CELLHIVE_DO_DIRECT", ""),
		OutboundAllow:    outboundAllow(),
		EgressAllow:      egressAllow(),
	}
	capnpPath, err := userruntime.Render(dataDir, runtimeCfg)
	if err != nil {
		log.Error("render config", "err", err)
		os.Exit(1)
	}
	childEnv, err := userruntime.WorkerdEnv(runtimeCfg)
	if err != nil {
		log.Error("build workerd environment", "err", err)
		os.Exit(1)
	}
	workerd, err := userruntime.FindWorkerd()
	if err != nil {
		log.Error("find workerd", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("user-runtime starting", "workerd", workerd, "config", capnpPath,
		"public", fmt.Sprintf(":%d", publicPort), "internal", fmt.Sprintf(":%d", internalPort), "cell", cellURL)
	// Graceful shutdown (ADR-156): on SIGTERM mark the loader not-ready so the
	// edge stops routing to this instance, give in-flight requests a moment, then
	// stop workerd.
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() {
		<-ctx.Done()
		if err := drainLocal(publicPort, cellToken); err != nil {
			log.Warn("user-runtime drain probe failed; relying on readiness removal", "err", err)
		}
		select {
		case <-time.After(drainGrace):
		case <-runCtx.Done():
		}
		cancelRun()
	}()
	if err := userruntime.Run(runCtx, workerd, capnpPath, childEnv); err != nil {
		log.Error("user-runtime exited", "err", err)
		os.Exit(1)
	}
}

// runtimeRoot mirrors do-runtime's CELLHIVE_RUNTIME_DIR: one scratch root for
// every runtime (ADR-137).
func runtimeRoot() string {
	return getenv("CELLHIVE_RUNTIME_DIR", filepath.Join(os.TempDir(), "cellhive"))
}

// drainGrace is how long to keep serving after readiness turns 503 so the edge
// stops sending new requests before workerd exits.
const drainGrace = 3 * time.Second

// drainLocal flips the local loader's readiness to 503 (internal token).
func drainLocal(port int, token string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/drain", port), nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", token)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("drain: %s", resp.Status)
	}
	return nil
}

// traceRatio parses CELLHIVE_TRACES_SAMPLE_RATIO (default 0.01).
func traceRatio() float64 {
	if v := strings.TrimSpace(os.Getenv("CELLHIVE_TRACES_SAMPLE_RATIO")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0.01
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// outboundAllow parses CELLHIVE_TENANT_OUTBOUND (comma-separated workerd network
// categories: public|private|local). Empty = public only (I-09, ADR-130).
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

// egressAllow parses CELLHIVE_CAP_EGRESS (comma-separated CIDR blocks or
// workerd categories) into the loader PLATFORM binding's capability-egress
// policy. Empty keeps the legacy permissive posture (public+private) until
// the deployment configures the runtime-services range.
// envList parses a comma-separated environment variable into a list.
func envList(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
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

func egressAllow() []string {
	return envList("CELLHIVE_CAP_EGRESS")
}
