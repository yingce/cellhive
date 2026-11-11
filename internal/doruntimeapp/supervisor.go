// The integrated DO supervisor captures a do-runtime's on-disk SQLite files (host actor
// + facet files) as LTX and gates do-runtime responses on cell-agent durability
// (ADR-083). It discovers facets by parsing workerd's <hosthash>.facets table and
// can cold-restore a captured shard through cell-agent, holding no bucket
// credentials itself (ADR-084).
package doruntimeapp

import (
	"cellhive/internal/bucket"
	cellcfg "cellhive/internal/config"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cellhive/internal/doruntime"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/runtimelog"
	"cellhive/internal/sqlcapture"
	"cellhive/internal/telemetry"
)

type supervisorOptions struct {
	Listen, ScopePrefix, Followers, RestoreObject string
	Epoch                                         uint64
	Restore, RestoreIfPresent                     bool
}

func runSupervisor(runtimeCfg doruntime.Config, configPath string, opts supervisorOptions) {
	listen, dir, scopePrefix, epoch := &opts.Listen, &runtimeCfg.DiskDir, &opts.ScopePrefix, &opts.Epoch
	owner, token := &runtimeCfg.CellURL, &runtimeCfg.CellToken
	follower := &opts.Followers
	restore, restoreIfPresent, restoreObj := &opts.Restore, &opts.RestoreIfPresent, &opts.RestoreObject
	workerdValue := os.Getenv("CELLHIVE_WORKERD")
	workerdBin, config := &workerdValue, &configPath
	doURLValue := localURL(runtimeCfg.Addr)
	doURL := &doURLValue

	if *dir == "" {
		log.Fatal("runtime disk directory is required")
	}
	logger := log.New(os.Stderr, "do-runtime ", log.LstdFlags)
	agent := &dosupervisor.HTTPStore{Base: *owner, Token: *token}

	if *restore || *restoreIfPresent {
		s := &dosupervisor.Supervisor{
			Dir: *dir, Epoch: *epoch, ScopePrefix: *scopePrefix,
			Segments: agent, Blobs: agent, Log: logger,
		}
		if *restoreObj != "" {
			parts := strings.SplitN(*restoreObj, "/", 3)
			if len(parts) != 3 {
				log.Fatal("-restore-object must be storage_id/class/name")
			}
			rel, err := s.RestoreObject(context.Background(), *dir, parts[0], parts[1], parts[2])
			if err != nil {
				log.Fatalf("restore object: %v", err)
			}
			log.Printf("restored object %s -> %s", *restoreObj, rel)
			return
		}
		n, err := s.RestoreAll(context.Background(), *dir)
		if err = optionalRestoreError(err, *restoreIfPresent); err != nil {
			log.Fatalf("restore: %v", err)
		}
		if n == 0 && *restoreIfPresent {
			log.Printf("no capture manifest; starting with empty runtime state")
		} else {
			log.Printf("restored %d files from %s into %s", n, *owner, *dir)
		}
		return
	}

	var followers []string
	for _, f := range strings.Split(*follower, ",") {
		if f = strings.TrimSpace(f); f != "" {
			followers = append(followers, f)
		}
	}
	committer := sqlcapture.NewHTTPCommitter(*owner, *token, followers, nil)
	committer.RequireDOOwner = true // Claimed hosts recover follower tails before cold restore.
	s := &dosupervisor.Supervisor{
		Dir:         *dir,
		Committer:   committer,
		Epoch:       *epoch,
		ScopePrefix: *scopePrefix,
		Segments:    agent,
		Blobs:       agent,
		Log:         logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *config != "" {
		advertise := os.Getenv("CELLHIVE_DO_ADVERTISE")
		if advertise == "" {
			advertise = strings.TrimPrefix(*doURL, "http://")
		}
		if !strings.HasPrefix(advertise, "http://") && !strings.HasPrefix(advertise, "https://") {
			advertise = "http://" + advertise
		}
		registerURLs := os.Getenv("CELLHIVE_LOG_REGISTER_URLS")
		if registerURLs == "" {
			registerURLs = *owner
		}
		go runtimelog.RegisterLoop(ctx, registerURLs, *token, []string{advertise})
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	var drain func() error

	// OTLP trace export (ADR-167); empty endpoint = no-op.
	acfg := cellcfg.FromEnv()
	svc := acfg.ServiceName
	if os.Getenv("CELLHIVE_SERVICE_NAME") == "" {
		svc = "cellhive-do-supervisor" // Preserve the existing telemetry service identity.
	}
	tel, terr := telemetry.New(ctx, telemetry.Config{
		Endpoint: acfg.OTLPEndpoint, Headers: acfg.OTLPHeaders,
		Service: svc, NodeID: acfg.NodeID, Ratio: acfg.TraceSampleRatio,
	})
	if terr != nil {
		log.Fatalf("telemetry init: %v", terr)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tel.Shutdown(shutCtx)
	}()

	if *config != "" {
		bin := *workerdBin
		if bin == "" {
			var err error
			if bin, err = doruntime.FindWorkerd(); err != nil {
				log.Fatalf("find workerd: %v", err)
			}
		}
		// Never expose sparse databases to a binary that silently ignores the
		// paging environment. The fork advertises this explicit protocol probe.
		probeCtx, probeCancel := context.WithTimeout(ctx, 10*time.Second)
		probe := exec.CommandContext(probeCtx, bin, "cellhive-build-info")
		output, probeErr := probe.Output()
		probeCancel()
		if probeErr != nil || !strings.Contains("\n"+string(output), "\ndo-paging/1\n") ||
			!strings.Contains("\n"+string(output), "\ndo-paging-fail-stop/1\n") ||
			!strings.Contains("\n"+string(output), "\ndo-paging-lifecycle/1\n") {
			log.Fatalf("workerd lacks required DO paging/fail-stop protocol v1: %v", probeErr)
		}
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			log.Fatalf("paging capability: %v", err)
		}
		pageToken := hex.EncodeToString(secret)
		paging, err := dosupervisor.NewPaging(*dir, pageToken)
		if err != nil {
			log.Fatalf("paging registry: %v", err)
		}
		defer paging.Close()
		s.Paging = paging
		s.ScopedRecovery = strings.Contains("\n"+string(output), "\ndo-paging-local-close/1\n")

		pageListener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			log.Fatalf("paging listener: %v", err)
		}
		pageServer := &http.Server{Handler: s.PagingHandler(), ReadHeaderTimeout: 5 * time.Second}
		defer pageServer.Close()
		go func() {
			if err := pageServer.Serve(pageListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("paging service failed: %v", err)
				stop()
			}
		}()
		logDir := os.Getenv("CELLHIVE_LOG_DIR")
		if logDir == "" {
			logDir = filepath.Join(runtimeRoot(), "logs")
		}
		spool, err := runtimelog.OpenSpool(runtimelog.SpoolConfig{Dir: logDir, Stream: "do-runtime", Endpoint: os.Getenv("CELLHIVE_OTLP_ENDPOINT")})
		if err != nil {
			log.Fatalf("open log spool: %v", err)
		}
		defer func() {
			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = spool.Close(c)
		}()
		runtimeCfg.LogSpool = spool
		runtimeCfg.PagingLifecycle = true
		childEnv, err := doruntime.WorkerdEnv(runtimeCfg)
		if err != nil {
			log.Fatalf("build workerd environment: %v", err)
		}
		childEnv = append(childEnv, "CELLHIVE_DO_PAGING_URL=http://"+pageListener.Addr().String(), "CELLHIVE_DO_PAGING_TOKEN="+pageToken, "CELLHIVE_DO_PAGING_FAIL_STOP=1")
		// Renewal has its own lifetime: canceling the child requests termination,
		// but only Run returning proves it has stopped accessing the database.
		s.OwnerLeases = dosupervisor.NewOwnerLeases(context.Background(), agent.RenewDOOwner)
		defer s.OwnerLeases.Close()
		childExited := make(chan struct{})
		defer func() {
			cancelRun()
			<-childExited // join before paging.Close or any remaining lease teardown
		}()
		go func() {
			err := waitForChild(func() error { return doruntime.Run(runtimelog.WithSpool(runCtx, spool), bin, *config, childEnv) },
				s.OwnerLeases.Close, childExited)
			if runCtx.Err() == nil {
				log.Printf("workerd exited: %v", err)
				stop()
			}
		}()
		log.Printf("supervising workerd %s config=%s", bin, *config)
		// When we own the workerd process we also own its lease lifecycle:
		// renew the DO owner leases and drain on shutdown (ADR-078/083).
		// Gated hosts negotiate independent Go renewal at restore/claim time.
		// Ungated runtimes retain the local workerd lifecycle endpoint.
		if runtimeCfg.GateURL == "" {
			go doruntime.RenewLoop(ctx, *doURL, *token, doruntime.RenewEvery,
				func(err error) { log.Printf("do renew failed: %v", err) })
		}
		drain = func() error {
			return drainLiveChild(childExited, func() error { return doruntime.Drain(*doURL, *token) })
		}
	}

	srv := &http.Server{Addr: *listen, Handler: s.Handler()}
	go func() {
		err := gatedShutdownAfter(ctx, drain, cancelRun)
		if err != nil {
			log.Printf("do drain failed; relying on lease expiry: %v", err)
		} else if drain != nil {
			log.Printf("do-runtime drained")
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	log.Printf("do-runtime gate listening %s dir=%s owner=%s", *listen, *dir, *owner)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// shutdownAfter keeps workerd alive while the host actor drains in-flight work
// and releases leases. cancelRun stops it only after drain finishes or times out.
func gatedShutdownAfter(signalCtx context.Context, drain func() error, cancelRun context.CancelFunc) error {
	<-signalCtx.Done()
	var err error
	if drain != nil {
		err = drain()
	}
	cancelRun()
	return err
}

func optionalRestoreError(err error, optional bool) error {
	if optional && errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	return err
}

// A dead child cannot prove a successful drain. Keep the owner record fenced
// until its lease expires instead of releasing it using ambiguous local state.
func drainLiveChild(childExited <-chan struct{}, drain func() error) error {
	select {
	case <-childExited:
		return errors.New("workerd already exited; retaining ownership until lease expiry")
	default:
		return drain()
	}
}

// run must wait for the OS process to exit. Renewal ends only after that proof,
// before shutdown is notified; no release RPC or certificate reset is involved.
func waitForChild(run func() error, stopRenewal func(), exited chan<- struct{}) error {
	err := run()
	stopRenewal()
	close(exited)
	return err
}
