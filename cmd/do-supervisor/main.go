// Command do-supervisor captures a do-runtime's on-disk SQLite files (host actor
// + facet files) as LTX and gates do-runtime responses on cell-agent durability
// (ADR-083). It discovers facets by parsing workerd's <hosthash>.facets table and
// can cold-restore a captured shard through cell-agent, holding no bucket
// credentials itself (ADR-084).
package main

import (
	cellcfg "cellhive/internal/config"
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cellhive/internal/doruntime"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/sqlcapture"
	"cellhive/internal/telemetry"
)

func main() {
	listen := flag.String("listen", ":18901", "HTTP listen address for /sync-all")
	dir := flag.String("dir", "", "do-runtime data dir (contains the host-actor sqlite files)")
	scopePrefix := flag.String("scope-prefix", "workerd/__do__", "scope namespace/class for derived capture scopes")
	epoch := flag.Uint64("epoch", 1, "epoch")
	owner := flag.String("owner", "http://127.0.0.1:7001", "owner cell-agent URL")
	follower := flag.String("follower", "", "comma-separated follower URLs")
	token := flag.String("token", cellcfg.DeriveCredentials(cellcfg.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	restore := flag.Bool("restore", false, "cold-restore captured shard files into -dir via cell-agent, then exit")
	restoreObj := flag.String("restore-object", "", "with -restore: restore only one object as storage_id/class/name (per-object cold start)")
	workerdBin := flag.String("workerd", "", "workerd binary to spawn/supervise (empty: auto-detect)")
	config := flag.String("config", "", "workerd capnp config to spawn/supervise (empty: do not spawn)")
	doURL := flag.String("do-url", "http://127.0.0.1:8788", "local do-runtime URL for lease renew/drain while supervising workerd")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-dir is required")
	}
	logger := log.New(os.Stderr, "do-supervisor ", log.LstdFlags)
	agent := &dosupervisor.HTTPStore{Base: *owner, Token: *token}

	if *restore {
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
		if err != nil {
			log.Fatalf("restore: %v", err)
		}
		log.Printf("restored %d files from %s into %s", n, *owner, *dir)
		return
	}

	var followers []string
	for _, f := range strings.Split(*follower, ",") {
		if f = strings.TrimSpace(f); f != "" {
			followers = append(followers, f)
		}
	}
	committer := sqlcapture.NewHTTPCommitter(*owner, *token, followers, nil)
	s := &dosupervisor.Supervisor{
		Dir:         *dir,
		Committer:   committer,
		Epoch:       *epoch,
		ScopePrefix: *scopePrefix,
		Blobs:       agent,
		Log:         logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTLP trace export (ADR-167); empty endpoint = no-op.
	acfg := cellcfg.FromEnv()
	svc := acfg.ServiceName
	if os.Getenv("CELLHIVE_SERVICE_NAME") == "" {
		svc = "cellhive-do-supervisor"
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
		creds := cellcfg.DeriveCredentials(cellcfg.LoadRootKey())
		childEnv, err := doruntime.WorkerdEnv(doruntime.Config{
			CellURL:        *owner,
			CellToken:      *token,
			DoTicketSecret: creds.DoTicket,
			AIURL:          os.Getenv("CELLHIVE_AI_URL"),
			AIKey:          os.Getenv("CELLHIVE_AI_KEY"),
			GateURL:        os.Getenv("CELLHIVE_DO_GATE_URL"),
		})
		if err != nil {
			log.Fatalf("build workerd environment: %v", err)
		}
		go func() {
			if err := doruntime.Run(ctx, bin, *config, childEnv); err != nil && ctx.Err() == nil {
				log.Printf("workerd exited: %v", err)
				stop()
			}
		}()
		log.Printf("supervising workerd %s config=%s", bin, *config)
		// When we own the workerd process we also own its lease lifecycle:
		// renew the DO owner leases and drain on shutdown (ADR-078/083).
		go doruntime.RenewLoop(ctx, *doURL, *token, doruntime.RenewEvery,
			func(err error) { log.Printf("do renew failed: %v", err) })
		go func() {
			<-ctx.Done()
			if err := doruntime.Drain(*doURL, *token); err != nil {
				log.Printf("do drain failed; relying on lease expiry: %v", err)
				return
			}
			log.Printf("do-runtime drained")
		}()
	}

	srv := &http.Server{Addr: *listen, Handler: s.Handler()}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	log.Printf("do-supervisor listening %s dir=%s owner=%s", *listen, *dir, *owner)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
