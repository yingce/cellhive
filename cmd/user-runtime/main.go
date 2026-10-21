// Command user-runtime launches the CellHive user-runtime workerd process
// (docs/architecture.md): public loader :8081 + internal privileged dispatch
// :8088 that runs tenant queue()/scheduled() handlers.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"cellhive/internal/config"
	"cellhive/internal/ingress"
	"cellhive/internal/plangen"
	"cellhive/internal/runtimelog"
	"cellhive/internal/staticgen"
	"cellhive/internal/userruntime"
	"cellhive/internal/userruntimepool"
	"cellhive/internal/workerdpool"
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
	logToken := creds.Log
	internalPort := envInt("CELLHIVE_USER_RUNTIME_INTERNAL_PORT", 8088)
	publicPort := envInt("CELLHIVE_USER_RUNTIME_PORT", 8081)
	dataDir := filepath.Join(runtimeRoot(), "user-runtime")

	platformJS := getenv("CELLHIVE_USER_RUNTIME_JS", "workerd/user-runtime")
	facadesJS := getenv("CELLHIVE_FACADES_JS", "workerd/platform/facades.js")

	tenantLogCapture, err := tenantLogCapture()
	if err != nil {
		log.Error("invalid CELLHIVE_TENANT_LOG_CAPTURE", "err", err)
		os.Exit(1)
	}
	invocationLogging, err := invocationLogging()
	if err != nil {
		log.Error("invalid CELLHIVE_INVOCATION_LOGS", "err", err)
		os.Exit(1)
	}
	runtimeCfg := userruntime.Config{
		CellURL:           cellURL,
		CellToken:         cellToken,
		AIURL:             getenv("CELLHIVE_AI_URL", ""),
		AIKey:             getenv("CELLHIVE_AI_KEY", ""),
		InternalPort:      internalPort,
		PublicPort:        publicPort,
		PlatformJS:        platformJS,
		FacadesJS:         facadesJS,
		ScopeSecret:       scopeSecret,
		DispatchToken:     dispatchToken,
		LogToken:          logToken,
		DisableTenantLogs: !tenantLogCapture,
		InvocationLogging: invocationLogging,
		ServiceNative:     getenv("CELLHIVE_SERVICE_NATIVE", ""),
		TraceSampleRatio:  traceRatio(),
		DoDirect:          getenv("CELLHIVE_DO_DIRECT", ""),
		OutboundAllow:     outboundAllow(),
		EgressAllow:       egressAllow(),
	}
	if v := getenv("CELLHIVE_ISOLATE_MAX", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Error("invalid CELLHIVE_ISOLATE_MAX", "value", v)
			os.Exit(1)
		}
		runtimeCfg.IsolateMax = n
	}
	if v := getenv("CELLHIVE_ISOLATE_BYTES", ""); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1<<20 {
			log.Error("invalid CELLHIVE_ISOLATE_BYTES", "value", v)
			os.Exit(1)
		}
		runtimeCfg.IsolateBytes = n
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
	if getenv("CELLHIVE_POOL_MODE", "") == "adaptive" {
		if err := runAdaptivePool(runtimeCfg, workerd, dataDir, log); err != nil {
			log.Error("adaptive user-runtime pool exited", "err", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var frontServer *ingress.Server
	if ingressPort := envInt("CELLHIVE_INGRESS_PORT", 0); ingressPort > 0 {
		upstream := getenv("CELLHIVE_INGRESS_UPSTREAM", fmt.Sprintf("127.0.0.1:%d", publicPort))
		var table *ingress.Table
		if path := getenv("CELLHIVE_INGRESS_TABLE", ""); path != "" {
			loaded, err := ingress.LoadTable(path)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Error("load ingress table", "path", path, "err", err)
					os.Exit(1)
				}
				log.Info("ingress table not present yet; starting dynamic-only", "path", path)
			} else {
				table = loaded
				if table.Default == "" {
					table.Default = upstream
				}
			}
		}
		front, err := ingress.New(fmt.Sprintf(":%d", ingressPort), table, upstream, log)
		if err != nil {
			log.Error("start ingress", "err", err)
			os.Exit(1)
		}
		frontServer = front
		go func() {
			if err := front.Serve(ctx); err != nil {
				log.Error("ingress exited", "err", err)
			}
		}()
		if path := getenv("CELLHIVE_INGRESS_TABLE", ""); path != "" {
			go front.WatchTable(ctx, path, time.Second)
		}
		log.Info("ingress listening", "port", ingressPort, "upstream", upstream, "table", getenv("CELLHIVE_INGRESS_TABLE", ""))
	}
	planPath := getenv("CELLHIVE_STATIC_PLAN", "")
	staticHosts := splitCSV(getenv("CELLHIVE_STATIC_HOSTS", ""))
	hotMax := envInt("CELLHIVE_HOT_MAX", 0)
	var staticMgr *staticgen.Manager
	if planPath != "" || len(staticHosts) > 0 || hotMax > 0 {
		staticMgr, err = staticgen.NewManager(staticgen.Config{
			WorkerdBin: workerd,
			Root:       filepath.Join(dataDir, "staticgen"),
			PortBase:   envInt("CELLHIVE_STATIC_PORT_BASE", 19000),
			DrainGrace: drainGrace,
			TablePath:  getenv("CELLHIVE_INGRESS_TABLE", ""),
			Logger: func(format string, args ...any) {
				log.Info(fmt.Sprintf(format, args...))
			},
		})
		if err != nil {
			log.Error("start staticgen", "err", err)
			os.Exit(1)
		}
		defer staticMgr.Close()
		if adminPort := envInt("CELLHIVE_INGRESS_ADMIN_PORT", 0); adminPort > 0 && frontServer != nil {
			startIngressAdmin(ctx, adminPort, frontServer, staticMgr, log)
		}
		switch {
		case planPath != "":
			go staticMgr.Watch(ctx, planPath, time.Second)
			log.Info("static generation manager watching plan", "plan", planPath, "port_base", envInt("CELLHIVE_STATIC_PORT_BASE", 19000))
		case hotMax > 0:
			if frontServer == nil {
				log.Error("CELLHIVE_HOT_MAX requires CELLHIVE_INGRESS_PORT")
				os.Exit(1)
			}
			go runHotLoop(ctx, frontServer, staticMgr, cellURL, cellToken, logToken, log)
			log.Info("hot-set static generation enabled", "max", hotMax, "port_base", envInt("CELLHIVE_STATIC_PORT_BASE", 19000))
		default:
			go runStaticHostsLoop(ctx, staticMgr, cellURL, cellToken, logToken, staticHosts, log)
			log.Info("static generation manager building plans", "hosts", strings.Join(staticHosts, ","), "port_base", envInt("CELLHIVE_STATIC_PORT_BASE", 19000))
		}
	}
	advertise := getenv("CELLHIVE_USER_RUNTIME_ADVERTISE", "")
	if advertise == "" {
		advertise = getenv("POD_IP", "127.0.0.1")
	}
	go runtimelog.RegisterLoopFunc(ctx, getenv("CELLHIVE_LOG_REGISTER_URLS", cellURL), cellToken, func() []string {
		urls := []string{
			fmt.Sprintf("http://%s:%d", advertise, publicPort),
			fmt.Sprintf("http://%s:%d", advertise, internalPort),
		}
		if staticMgr != nil {
			if tailURL := staticMgr.TailURL(advertise); tailURL != "" {
				urls = append(urls, tailURL)
			}
		}
		return urls
	})
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

func runAdaptivePool(cfg userruntime.Config, workerd, dataDir string, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	manager, err := userruntimepool.NewManager(context.Background(), workerd, filepath.Join(dataDir, "pool"), cfg, workerdpool.Config{
		MemoryUtilization: envFloat("CELLHIVE_POOL_MEMORY_UTILIZATION", .70),
		MaxSlots:          envInt("CELLHIVE_POOL_MAX_SLOTS", 16),
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = manager.Close(closeCtx)
	}()
	router := userruntimepool.NewRouter(cfg.CellURL, cfg.CellToken, cfg.DispatchToken)
	defer router.Close()
	proxy := workerdpool.NewProxy(manager.Supervisor, router.Public, manager.Acquire)
	defer proxy.Close()
	internal := workerdpool.NewProxy(manager.Supervisor, router.Internal, manager.Acquire)
	defer internal.Close()
	var draining atomic.Bool
	public := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		case "/ready":
			if draining.Load() || !manager.Ready() {
				http.Error(w, "draining", http.StatusServiceUnavailable)
			} else {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			}
			return
		case "/drain":
			if r.Method == http.MethodPost {
				if cfg.CellToken == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("x-cellhive-internal-token")), []byte(cfg.CellToken)) != 1 {
					http.Error(w, "unauthorized", 401)
					return
				}
				draining.Store(true)
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		if draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	})
	lnPublic, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PublicPort))
	if err != nil {
		return err
	}
	lnInternal, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.InternalPort))
	if err != nil {
		lnPublic.Close()
		return err
	}
	sp := &http.Server{Handler: public}
	si := &http.Server{Handler: internal}
	defer sp.Close()
	defer si.Close()
	errCh := make(chan error, 2)
	go func() { errCh <- sp.Serve(lnPublic) }()
	go func() { errCh <- si.Serve(lnInternal) }()
	log.Info("adaptive user-runtime pool started", "public", cfg.PublicPort, "internal", cfg.InternalPort, "max_slots", manager.Supervisor.Pool.Config.MaxSlots)
	select {
	case <-ctx.Done():
		draining.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sp.Shutdown(shutdownCtx)
		_ = si.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// runtimeRoot mirrors do-runtime's CELLHIVE_RUNTIME_DIR: one scratch root for
// every runtime (ADR-137).
func runtimeRoot() string {
	return getenv("CELLHIVE_RUNTIME_DIR", filepath.Join(os.TempDir(), "cellhive"))
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
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

// invocationLogging is the node-wide switch for durable Queue/Cron/Workflow
// invocation records (ADR-239); it is no longer a per-version policy.
func invocationLogging() (bool, error) {
	switch strings.ToLower(strings.TrimSpace(getenv("CELLHIVE_INVOCATION_LOGS", "on"))) {
	case "on":
		return true, nil
	case "off":
		return false, nil
	default:
		return false, fmt.Errorf("must be exactly on or off")
	}
}

func tenantLogCapture() (bool, error) {
	switch strings.ToLower(strings.TrimSpace(getenv("CELLHIVE_TENANT_LOG_CAPTURE", "on"))) {
	case "on":
		return true, nil
	case "off":
		return false, nil
	default:
		return false, fmt.Errorf("must be exactly on or off")
	}
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

func splitCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// newPlanBuilder wires the platform module paths and AI settings needed to
// render workers that use platform bindings.
func newPlanBuilder(cellURL, cellToken, logToken string) *plangen.Builder {
	return plangen.New(plangen.Config{
		CellURL: cellURL, CellToken: cellToken,
		PlatformJS:   getenv("CELLHIVE_USER_RUNTIME_JS", "workerd/user-runtime"),
		FacadesJS:    getenv("CELLHIVE_FACADES_JS", "workerd/platform/facades.js"),
		AIURL:        getenv("CELLHIVE_AI_URL", ""),
		AIKey:        getenv("CELLHIVE_AI_KEY", ""),
		LogToken:     logToken,
		MinimalMode:  staticMinimalMode(getenv("CELLHIVE_STATIC_MINIMAL", "")),
		TenantEgress: outboundAllow(),
	})
}

// runStaticHostsLoop periodically rebuilds the static plan for the configured
// hosts from the cell-agent projection and applies it when it changes. Hosts
// that need bindings/assets/path mounting are skipped and stay on the dynamic
// loader (the ingress falls back per Host).
func runStaticHostsLoop(ctx context.Context, mgr *staticgen.Manager, cellURL, cellToken, logToken string, hosts []string, log *slog.Logger) {
	builder := newPlanBuilder(cellURL, cellToken, logToken)
	var last []byte
	apply := func() {
		plan, skipped, err := builder.Build(ctx, hosts)
		if err != nil {
			log.Warn("static plan build failed", "err", err)
			return
		}
		for _, s := range skipped {
			log.Info("static plan skipped host", "host", s.Host, "reason", s.Reason)
		}
		data, err := json.Marshal(plan)
		if err != nil {
			log.Warn("static plan encode failed", "err", err)
			return
		}
		if bytes.Equal(data, last) {
			return
		}
		if err := mgr.Apply(ctx, plan); err != nil {
			log.Warn("static plan apply failed", "err", err)
			return
		}
		last = data
	}
	apply()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			apply()
		}
	}
}

func staticMinimalMode(v string) string {
	switch v {
	case "1", "on", "true":
		return "on"
	case "0", "off", "false":
		return "off"
	default:
		return "auto"
	}
}
