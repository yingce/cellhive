// Command cell-agent is the CellHive state/control node (P0 skeleton).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cellhive/internal/admission"
	"cellhive/internal/autoscaler"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/coordination"
	"cellhive/internal/cron"
	"cellhive/internal/d1"
	"cellhive/internal/dispatch"
	"cellhive/internal/drain"
	"cellhive/internal/instance"
	"cellhive/internal/lease"
	"cellhive/internal/logbuf"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/peer"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/rebalance"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/scheduleindex"
	"cellhive/internal/server"
	"cellhive/internal/telemetry"
	"cellhive/internal/timer"
	"cellhive/internal/upload"
	"cellhive/internal/vectorize"
	"cellhive/internal/wake"
	"cellhive/internal/waker"
	"cellhive/internal/workflow"
)

func main() {
	level := slog.LevelInfo
	if value := os.Getenv("CELLHIVE_LOG_LEVEL"); value != "" {
		if err := level.UnmarshalText([]byte(value)); err != nil {
			fmt.Fprintln(os.Stderr, "invalid CELLHIVE_LOG_LEVEL")
			os.Exit(2)
		}
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(2)
	}
	var shared *scheduleindex.Index
	if cfg.ScheduleMode == "shared" {
		index, err := scheduleindex.New(cfg.ScheduleRedisURL, cfg.ScheduleRegion, log)
		if err != nil {
			log.Error("invalid shared scheduling configuration", "err", err)
			os.Exit(2)
		}
		shared = index
		defer shared.Close()
	}
	inst, err := instance.Acquire(cfg.DataDir)
	if err != nil {
		log.Error("acquire data directory", "err", err)
		os.Exit(1)
	}
	defer inst.Close()
	if err := cellstore.CleanupSnapshotCopies(cfg.DataDir); err != nil {
		log.Error("cleanup temporary snapshot copies", "err", err)
		os.Exit(1)
	}
	for _, w := range config.LegacyEnvWarnings() {
		log.Warn(w)
	}

	b, err := openBucket(context.Background(), cfg)
	if err != nil {
		log.Error("open bucket", "err", err)
		os.Exit(1)
	}

	if len(os.Args) > 1 && os.Args[1] == "coordination-init" {
		if len(os.Args) != 3 || os.Args[2] != "--offline" || cfg.CoordinationBackend != "redis" {
			log.Error("coordination-init requires --offline, stopped writers and redis backend")
			os.Exit(2)
		}
		if err := coordination.Initialize(context.Background(), b, cfg.CoordinationRedisURL, cfg.CoordinationRegion); err != nil {
			log.Error("coordination initialization failed", "err", err)
			os.Exit(1)
		}
		log.Info("coordination initialized; start all participants with redis backend")
		return
	}
	var coordinationBucket *coordination.Bucket
	if cfg.CoordinationBackend == "redis" {
		coordinationBucket, err = coordination.Open(context.Background(), b, cfg.CoordinationRedisURL, cfg.CoordinationRegion)
		if err != nil {
			log.Error("open coordination authority failed", "err", err)
			os.Exit(1)
		}
		defer coordinationBucket.Close()
		b = coordinationBucket
	} else if err = coordination.CheckObjectMode(context.Background(), b); err != nil {
		log.Error("coordination binding check failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OpenTelemetry OTLP/HTTP export (ADR-167). Empty endpoint = no-op.
	tel, err := telemetry.New(ctx, telemetry.Config{
		Endpoint: cfg.OTLPEndpoint,
		Headers:  cfg.OTLPHeaders,
		Service:  cfg.ServiceName,
		Version:  cell.ProtoVersion,
		NodeID:   cfg.NodeID,
		Ratio:    cfg.TraceSampleRatio,
		Logs:     cfg.OTLPLogs,
	})
	if err != nil {
		log.Error("telemetry init", "err", err)
		os.Exit(1)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tel.Shutdown(shutCtx)
	}()
	if tel.Enabled() {
		log.Info("otel export enabled", "ratio", cfg.TraceSampleRatio)
	}

	// The bucket is the authority for owner election and epoch fencing. Serving
	// after a failed conformance probe could admit two owners, so fail closed.
	if err := bucket.Diagnose(ctx, b); err != nil {
		log.Error("storage diagnose failed", "err", err)
		os.Exit(1)
	}
	log.Info("storage diagnose ok", "authority_mode", cfg.AuthorityMode)

	lm := lease.NewManager(b, cfg.NodeID, cfg.SessionID, cfg.Advertise, cfg.PeerURL, cfg.LeaseTTL)
	lm.Instance = inst.ID
	if prev, lerr := lm.Get(ctx, cfg.NodeID); lerr == nil && prev.Live(time.Now()) && prev.Session != cfg.SessionID {
		if prev.Instance == "" || prev.Instance != inst.ID {
			log.Error("node id is live in another instance", "node", cfg.NodeID, "lease_session", prev.Session, "lease_instance", prev.Instance, "instance", inst.ID)
			os.Exit(1)
		}
	} else if lerr != nil && !errors.Is(lerr, bucket.ErrNotFound) {
		log.Error("read previous node lease failed", "err", lerr)
		os.Exit(1)
	}
	lm.AZ = cfg.PlacementAZ
	// ADR-273/276/285: owner, resource-fence and recovery protocols require a full
	// replacement. Never start if we cannot establish the live peer set.
	if peers, serr := lm.Sample(ctx); serr != nil {
		log.Error("cannot verify peer protocol; refusing to start", "err", serr)
		os.Exit(1)
	} else if legacy := lease.LegacyPeers(peers, cfg.NodeID, time.Now()); len(legacy) > 0 {
		names := make([]string, 0, len(legacy))
		for _, l := range legacy {
			names = append(names, l.Node)
		}
		log.Error("live node(s) use an incompatible owner or resource-fence protocol; stop them before starting this version", "nodes", names)
		os.Exit(1)
	}
	adminAdvertise := ""
	if host, _, err := net.SplitHostPort(cfg.Advertise); err == nil {
		port := "8082"
		if _, p, perr := net.SplitHostPort(cfg.AdminAddr); perr == nil && p != "" {
			port = p
		}
		adminAdvertise = net.JoinHostPort(host, port)
	}
	om := &owner.Manager{
		B:              b,
		NodeID:         cfg.NodeID,
		Session:        cfg.SessionID,
		Advertise:      cfg.Advertise,
		AdminAdvertise: adminAdvertise,
		Role:           cell.RoleCellAgent,
		OwnerTTL:       cfg.LeaseTTL,
	}
	om.LeaseView = lm

	spool, err := peer.NewSpool(cfg.PeerSpoolDir)
	if err != nil {
		log.Error("open peer spool", "err", err)
		os.Exit(1)
	}
	peerTransport := peer.NewHTTPTransport(cfg.TokenPeer, nil).WithSession(cfg.SessionID)
	targets := &peer.Targets{}
	peerTransport.Targets = targets
	peerMgr := peer.NewManager(peerTransport)
	nodeLog := nodelog.New(b, cfg.NodeID)

	rep := replica.New(b)
	if cleanup, err := rep.EnableLocalLTXCache(cfg.DataDir); err != nil {
		log.Warn("local LTX cache disabled", "err", err)
	} else {
		defer cleanup()
	}
	rep.RestoreParallel = cfg.RestoreParallel
	// Replay the previous process's acknowledged local uploads before recovery
	// can inspect/cache a bucket cut or invalidate its original SQLite file.
	startupSpool, err := upload.OpenSpool(filepath.Join(cfg.DataDir, "upload-spool"), 200000)
	if err != nil {
		log.Error("open startup upload spool", "err", err)
		os.Exit(1)
	}
	startupUploads := upload.New(rep, log, 64, 1<<20, time.Millisecond)
	startupUploads.SetSpool(startupSpool)
	if err := startupUploads.ReplaySpool(ctx); err != nil {
		log.Error("startup upload replay stopped", "err", err)
		os.Exit(1)
	}
	takeoverRecovery := recovery.New(rep, peer.NewHTTPTransport(cfg.TokenPeer, nil))
	takeoverRecovery.Lease = lm
	takeoverRecovery.LocalInstanceLocked = true
	om.BeforeTakeover = func(tctx context.Context, predecessor cell.Owner) error {
		_, err := takeoverRecovery.Recover(tctx, nodeLog, predecessor.Node, predecessor.Session)
		return err
	}
	recoveryServer := server.New(server.Deps{Cfg: cfg, Lease: lm, Spool: spool, Log: log})
	startupListener, err := server.StartRecoveryListener(cfg.RESTAddr, recoveryServer)
	if err != nil {
		log.Error("bind recovery listener", "err", err)
		os.Exit(1)
	}
	defer startupListener.Close()
	if err := lm.PublishRecoveryAddress(ctx); err != nil {
		log.Error("publish recovery address", "err", err)
		os.Exit(1)
	}
	if err := takeoverRecovery.RecoverLocal(ctx, nodeLog, cfg.NodeID, cfg.SessionID, log); err != nil {
		log.Error("startup recovery stopped", "err", err)
		if ctx.Err() == nil {
			os.Exit(1)
		}
		return
	}

	placementWeight := cfg.PlacementWeight
	if placementWeight <= 0 {
		placementWeight = runtime.NumCPU()
	}
	leaseCtx, stopLease := context.WithCancel(context.Background())
	defer stopLease()
	var loadStore atomic.Pointer[cellstore.Store]
	loadSampler := newNodeLoadSampler()
	publishLoad := func() error {
		start := time.Now()
		log.Debug("node lease publication started", "session", cfg.SessionID, "remaining", lm.Remaining())
		load := loadSampler.Sample()
		load.Weight = placementWeight
		load.OwnedCells = len(om.OwnedScopes())
		if coordinationBucket != nil {
			count, cerr := coordinationBucket.Store.OwnedCount(leaseCtx, cfg.NodeID, cfg.SessionID)
			if cerr == nil {
				load.OwnedCells = count
				load.OwnedComplete = true
			}
		}
		if st := loadStore.Load(); st != nil {
			load.ResidentCells, _, _ = st.Stats()
			for _, owned := range om.OwnedScopes() {
				if st.IsOpen(owned.Scope) {
					load.OwnedResidentCells++
				}
			}
		}
		if load.OwnedComplete {
			load.DormantCells = max(0, load.OwnedCells-load.OwnedResidentCells)
		}
		if err := lm.Publish(bucket.WithReadSource(leaseCtx, bucket.LeaseRead), load); err != nil {
			log.Warn("lease publish failed", "err", err, "elapsed", time.Since(start), "remaining", lm.Remaining(), "session", cfg.SessionID, "lease", lm.Diagnostics())
			return err
		}
		log.Debug("node lease publication confirmed", "session", cfg.SessionID, "elapsed", time.Since(start), "remaining", lm.Remaining(), "lease", lm.Diagnostics())
		return nil
	}
	if err := publishLoad(); err != nil {
		log.Error("initial node lease failed", "err", err)
		os.Exit(1)
	}
	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()
	go watchNodeLease(watchCtx, lm, func() {
		log.Error("node lease permanently fenced; exiting without handoff", "session", cfg.SessionID, "remaining", lm.Remaining(), "lease", lm.Diagnostics())
		os.Exit(1)
	})

	// Keep known peer leases current independently of full fleet discovery.
	knownCtx, stopKnown := context.WithCancel(ctx)
	knownDone := make(chan struct{})
	go func() {
		defer close(knownDone)
		lm.RunKnownRefresh(bucket.WithReadSource(knownCtx, bucket.LeaseRead))
	}()
	defer func() { stopKnown(); <-knownDone }()

	// Keep renewing through shutdown drains; the watchdog remains active.
	go runNodeLeaseRenewal(leaseCtx, cfg.LeaseTTL, publishLoad)

	// Keep the upload/ship loops alive through graceful handoff: the signal
	// context is already cancelled when performHandoff runs, and a cancelled
	// uploader would leave follower-acked segments in the local spool while the
	// owner record is released.
	upCtx, upCancel := context.WithCancel(context.Background())
	shards := cfg.UploadShards
	if shards <= 0 {
		shards = runtime.NumCPU()
		if shards > 8 {
			shards = 8
		}
	}
	// Owner uploads, including a single node and a write with no follower,
	// coalesce on this interval after the local spool fsync. Follower fsync
	// keeps its own 2ms window below.
	uploadSegs, uploadEvery := uploadSegmentCap(cfg), cfg.UploadInterval
	if uploadEvery <= 0 {
		uploadEvery = 30 * time.Second
	}
	uploader := upload.NewSharded(rep, log, uploadSegs, 1<<20, uploadEvery, shards)
	compactWake := newCompactionWake()
	uploader.SetOnDurable(func(sc cell.Scope, epoch uint64) {
		compactWake.Nudge(sc, epoch)
	})
	// Durable queue for the asynchronous bucket copy: a failed upload (or a
	// restart) defers the segment instead of dropping it (ADR-143).
	if sp, serr := upload.OpenSpool(filepath.Join(cfg.DataDir, "upload-spool"), 200000); serr != nil {
		log.Warn("upload spool disabled; async bucket uploads are best-effort", "err", serr)
	} else {
		uploader.SetSpool(sp)
	}
	uploader.Start(bucket.WithReadSource(upCtx, bucket.UploadRead))
	// Follower-side group commit: coalesce replicated appends into one spool
	// fsync per batch (short window; the owner's ack waits on it).
	peerUploader := upload.New(spool, log, 64, 1<<20, 2*time.Millisecond)
	peerUploader.Start(bucket.WithReadSource(upCtx, bucket.UploadRead))
	// Fleet log group commit: coalesce concurrent commits into one framed batch
	// per follower set (1ms window, up to 256 segments), mirroring celld's
	// CELLD_LOG_GROUP_COMMIT_MS + CELLD_LOG_PIPELINE=4. StreamTransport uses
	// four scope-hashed HTTP 101 binary lanes per follower, with four batches
	// allowed in flight per lane.
	streamTransport := peer.NewSessionStreamTransport(cfg.TokenPeer, cfg.SessionID, 4, 4)
	streamTransport.Targets = targets
	defer streamTransport.Close()
	var shipTransport peer.Transport = streamTransport
	if cfg.PeerLatency > 0 {
		log.Info("peer latency injection enabled", "one_way", cfg.PeerLatency)
		shipTransport = peer.NewLatencyTransport(shipTransport, cfg.PeerLatency)
	}
	shipper := peer.NewShipBatcher(shipTransport, 256, 4<<20, time.Millisecond, 4, cfg.PeerPipeline, cfg.PeerHedgeMS, cfg.PeerHedgeMaxMS)
	shipper.Start(bucket.WithReadSource(upCtx, bucket.UploadRead))

	// Background promotion stays off the write-ack path. A durable L0 upload
	// nudges L1 when the object count or stored-byte bar is met.
	go runCompactionScheduler(bucket.WithReadSource(ctx, bucket.CompactionRead), log, om, rep, cfg, compactWake)

	store, err := cellstore.New(cfg.DataDir)
	if err != nil {
		log.Error("open cell store", "err", err)
		os.Exit(1)
	}
	loadStore.Store(store)
	// Drop the tally only after this process no longer holds the epoch. A fold
	// that times out must not erase the count when the release itself fails.
	om.AfterRelease = func(sc cell.Scope, epoch uint64) {
		rep.LoseL0(sc, epoch)
	}
	// Claim callbacks must not wait on a request that is itself waiting for
	// the per-scope claim gate. Retry a busy replica without publishing owner.
	resetLocal := func(rctx context.Context, sc cell.Scope) error {
		ok, err := store.TryInvalidate(rctx, sc, nil)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("stale local cell is busy; retry ownership acquisition")
		}
		return nil
	}
	store.OnEvicted = func(sc cell.Scope) { om.Forget(sc, 0) }
	store.OnForget = func(sc cell.Scope) { om.Forget(sc, 0) }
	// The control cell is the hottest cell in the fleet. Keep it resident:
	// evicting it re-runs schema migration and forces capture to rebuild its
	// baseline, which is both a cold-path cost and a migration/close race.
	store.Pin = func(sc cell.Scope) bool { return sc == control.Scope() }
	store.HydrateParallel = cfg.HydrateParallel
	store.HydrateInteractiveParallel = cfg.HydrateInteractiveParallel
	store.HydrateTimeout = cfg.HydrateTimeout
	// Paged cold restore (ADR-160): when the cell's compacted restore chain is
	// large enough, open the main database over a sparse local file and fault
	// pages from the replica on first use instead of downloading the whole
	// chain. Small or uncompacted cells fall through to the full hydrate below.
	if cfg.PagedRestore {
		store.Paged = func(pctx context.Context, sc cell.Scope, dest string) (pagedvfs.Source, bool, error) {
			pctx, span := telemetry.Tracer().Start(pctx, "cell.restore_paged_probe")
			defer span.End()
			ep, ok, perr := rep.LatestEpoch(pctx, sc, 0)
			if perr != nil {
				return nil, false, perr
			}
			if !ok {
				return nil, false, nil
			}
			// The index-only early-out avoids downloading an L0 tail for a cell
			// whose compacted image is already below the paged threshold. It is
			// deliberately conservative: any L0 object disables it because L0
			// decoded commits are not in the index and may grow the image.
			pf, usePaged, perr := rep.NewPageFetcherMinBytes(pctx, sc, ep, cfg.PagedMinBytes)
			if errors.Is(perr, replica.ErrNoPageIndex) {
				// No compacted L1 page index yet: clone the chain whole.
				return nil, false, nil
			}
			if perr != nil {
				return nil, false, perr
			}
			if !usePaged {
				_ = pf.Close()
				return nil, false, nil
			}
			imageBytes := int64(pf.PageSize()) * int64(pf.Commit())
			if imageBytes < cfg.PagedMinBytes {
				_ = pf.Close()
				return nil, false, nil
			}
			log.Info("paged restore", "scope", sc.String(), "epoch", ep,
				"bytes", imageBytes, "pages", pf.Commit())
			return &pagedSource{pf: pf}, true, nil
		}
		store.PagedHydrateMBPS = cfg.PagedHydrateMBPS
		if cfg.PagedReadAheadBytes > 0 {
			pagedvfs.ReadAheadBytes = cfg.PagedReadAheadBytes
		}
		if cfg.PagedPrefetchWorkers > 0 {
			pagedvfs.PrefetchWorkers = cfg.PagedPrefetchWorkers
		}
	}

	// Existing local files need a cold activation watermark floor, including
	// read-only requests that never start capture. Epoch zero plans all epochs.
	store.LocalDurable = func(hctx context.Context, sc cell.Scope) (uint64, error) {
		return rep.DurableWatermarkChain(hctx, sc, 0)
	}

	// Cold restore (ADR-092): a fresh owner hydrates a cell from its bucket
	// replica before serving, so acked backend-A writes survive crash/takeover.
	store.Hydrate = func(hctx context.Context, sc cell.Scope, dest string) error {
		hctx, span := telemetry.Tracer().Start(hctx, "cell.restore_full")
		defer span.End()
		hydrateStart := time.Now()
		ep, ok, herr := rep.LatestEpoch(hctx, sc, 0)
		if herr != nil {
			log.Warn("hydrate latest epoch failed", "scope", sc.String(), "elapsed", time.Since(hydrateStart), "err", herr)
			return herr
		}
		if !ok {
			return nil
		}
		segs, herr := rep.RestoreChain(hctx, sc, ep)
		if herr != nil {
			log.Warn("hydrate restore chain failed", "scope", sc.String(), "epoch", ep, "elapsed", time.Since(hydrateStart), "err", herr)
			return herr
		}
		if elapsed := time.Since(hydrateStart); elapsed > time.Second {
			log.Info("hydrate slow", "scope", sc.String(), "epoch", ep, "segments", len(segs), "elapsed", elapsed)
		}
		if len(segs) == 0 {
			return nil
		}
		raw := make([][]byte, 0, len(segs))
		for _, sg := range segs {
			raw = append(raw, sg.Raw)
		}
		_, applySpan := telemetry.Tracer().Start(hctx, "cell.restore_apply_sqlite")
		_, herr = restore.ApplyFileWithFinalize(dest, raw, func(path string, watermark uint64) error {
			return cellstore.AdoptRestoreFileTXID(hctx, path, watermark)
		})
		if herr != nil {
			applySpan.RecordError(herr)
		}
		applySpan.End()
		return herr
	}

	// Unified timers: producers upsert into the owning cell; the local runner
	// dispatches due timers, and the single fleet waker covers dead owners.
	timerReg := timer.NewRegistry()
	// Wake index: the bucket-side list of scopes with a pending timer, so the
	// fleet waker finds due work without listing every cell (ADR-099).
	wakeIdx := wake.New(b)
	wakeIdx.Validate = func(ctx context.Context, scope string) error {
		sc, err := cell.ParseScope(scope)
		if err != nil {
			return err
		}
		current, _, err := om.Resolve(ctx, sc)
		if err != nil {
			return err
		}
		if current.Node != cfg.NodeID || current.Session != cfg.SessionID || !lm.Healthy(time.Now()) {
			return owner.ErrNotOwner
		}
		return nil
	}
	// Forget may be run by the fleet waker without owning the retired scope.
	// Capture hint versions first; then prove that the resource is still retired.
	forgetDeletedWake := func(ctx context.Context, scope string) error {
		return wake.New(b).DeleteValidated(ctx, scope, func() error {
			sc, err := cell.ParseScope(scope)
			if err != nil {
				return err
			}
			if sc.Class != "__kv__" && sc.Class != "__queue__" {
				return owner.ErrNotOwner
			}
			current, _, err := om.Resolve(ctx, sc)
			if err != nil {
				return err
			}
			if !current.Retired || !lm.Healthy(time.Now()) {
				return owner.ErrNotOwner
			}
			return nil
		})
	}

	var timerIndex timer.Index = wakeIdx
	if shared != nil {
		timerIndex = scheduleindex.TimerIndex{Durable: wakeIdx, Shared: shared}
	}
	openTimer := func(tctx context.Context, sc cell.Scope) (*timer.Store, error) {
		if sc.Class == "__kv__" || sc.Class == "__queue__" {
			blocked, err := rep.ResourceBlocked(tctx, sc)
			if err != nil {
				return nil, err
			}
			if blocked {
				return nil, replica.ErrResourceDeleted
			}
		}
		c, err := store.Cell(tctx, sc)
		if err != nil {
			return nil, err
		}
		st, err := timer.NewStore(tctx, c)
		if err != nil {
			return nil, err
		}
		st.Index = timerIndex
		return st, nil
	}
	var dispatcher timer.Dispatcher = timer.NoopDispatcher{Log: log}
	if d := dispatch.NewHTTP(cfg.DispatchURL, cfg.TokenDispatch); d != nil {
		dispatcher = d
	}
	// Cron timers carry only a cell scope; enrich the dispatch with the owning
	// worker + its active bundle so user-runtime can run scheduled() (ADR-070).
	enricher := &cronEnricher{next: dispatcher}
	dispatcher = enricher
	// Workflows: KindWorkflowSleep timers resume the slept instance; other kinds
	// pass through to user-runtime (owner-aware control reads are wired below).
	workflowStore := workflow.New(store)
	workflowStore.Index = timerIndex
	workflowStore.Register = timerReg.Add
	const workflowLeaseMs = int64(60_000)
	// One short-TTL projection cache serves every control-derived dispatch
	// callback (queue consumers, workflow targets, DO alarm
	// bundles). Its source is wired once the control store exists.
	projCache := &projectionCache{ttl: projectionCacheTTL}
	var controlReads *server.ControlReader
	wfDisp := &dispatch.WorkflowDispatcher{
		URL: cfg.DispatchURL, Token: cfg.TokenDispatch, Log: log, Next: dispatcher,
		Skip: workflowStore.SkipWake,
		Params: func(cctx context.Context, ns, name, id string) ([]byte, error) {
			instance, err := workflowStore.Get(cctx, ns, name, id)
			if err != nil {
				return nil, err
			}
			return instance.Params, nil
		},
		TargetForInstance: func(cctx context.Context, ns, name, id string) (control.WorkflowTarget, bool, error) {
			instance, err := workflowStore.Get(cctx, ns, name, id)
			if err != nil {
				return control.WorkflowTarget{}, false, err
			}
			if instance.TargetWorker == "" || instance.TargetBundle == "" || instance.TargetClass == "" || instance.TargetVersion == 0 {
				return control.WorkflowTarget{}, false, nil
			}
			return control.WorkflowTarget{Namespace: ns, Worker: instance.TargetWorker, Name: name, ClassName: instance.TargetClass, BundleSHA: instance.TargetBundle, Version: instance.TargetVersion}, true, nil
		},
		Claim: func(cctx context.Context, ns, name, id string) (string, uint64, bool, error) {
			limit := 0
			if controlReads == nil {
				return "", 0, false, errors.New("workflow control unavailable")
			}
			raw, policyErr := controlReads.ResourceConfig(cctx, ns, "workflow", name)
			if policyErr != nil && !errors.Is(policyErr, control.ErrNotFound) {
				return "", 0, false, policyErr
			}
			if policyErr == nil {
				var policy workflow.Policy
				if err := json.Unmarshal(raw, &policy); err != nil {
					return "", 0, false, err
				}
				limit = policy.Concurrency
			}
			tok, gen, _, ok, err := workflowStore.ClaimRunLimited(cctx, ns, name, id, workflowLeaseMs, limit)
			return tok, gen, ok, err
		},
		CreateScheduled: func(cctx context.Context, ns, name, occurrence string) (string, error) {
			if controlReads == nil {
				return "", fmt.Errorf("workflow control unavailable")
			}
			target, ok, err := projCache.workflowTarget(cctx, ns, name)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", fmt.Errorf("workflow target not found")
			}
			id := "schedule-" + occurrence
			in, err := workflowStore.CreateWithTarget(cctx, ns, name, id, nil, workflow.Target{Worker: target.Worker, BundleSHA: target.BundleSHA, Version: target.Version, ClassName: target.ClassName})
			if err != nil {
				return "", err
			}
			return in.ID, nil
		},
		Target: func(tctx context.Context, ns, name string) (control.WorkflowTarget, bool) {
			if controlReads == nil {
				return control.WorkflowTarget{}, false
			}
			target, ok, err := projCache.workflowTarget(tctx, ns, name)
			if err != nil {
				return control.WorkflowTarget{}, false
			}
			return target, ok
		},
	}
	dispatcher = wfDisp
	// DO alarms route to the owning do-runtime (ADR-079); Bundle is wired after
	// the control plane is available.
	alarmDisp := &dispatch.DoAlarmDispatcher{Next: dispatcher, Resolve: om.Resolve, Token: cfg.TokenInternal, Log: log}
	dispatcher = alarmDisp
	kvExpireDisp := &dispatch.KVExpireDispatcher{Next: dispatcher, Resolve: om.Resolve, Token: cfg.TokenInternal, Log: log}
	dispatcher = kvExpireDisp
	openTimerScope := func(tctx context.Context, scope string) (*timer.Store, error) {
		sc, err := cell.ParseScope(scope)
		if err != nil {
			return nil, err
		}
		return openTimer(tctx, sc)
	}
	var timerRunner *timer.Runner
	if cfg.TimerInterval > 0 {
		timerRunner = &timer.Runner{
			Registry: timerReg, Open: openTimerScope, Dispatcher: dispatcher,
			Interval: cfg.TimerInterval, Batch: cfg.TimerBatch, FiredTTL: cfg.TimerFiredTTL,
			CronMaxAttempts: cfg.CronMaxAttempts,
			Forget: func(fctx context.Context, scope string) error {
				return forgetDeletedWake(fctx, scope)
			},
			Log: log,
		}
	}
	// Leader-only automatic recovery of dead nodes (ADR-066): the waker, on the
	// same pass as timer dispatch, collects acked-but-unuploaded segments from
	// dead nodes' followers via the node-log.
	recoveryRunner := &recovery.Runner{
		Nodelog:  nodeLog,
		Lease:    lm,
		Recovery: takeoverRecovery,
		Log:      log,
	}
	recoveryRunner.IsLeader = waker.NewElection(b, cfg.NodeID, cfg.SessionID, cfg.WakerTTL).Acquire
	go recoveryRunner.Run(bucket.WithReadSource(ctx, bucket.RecoveryRead), recovery.DefaultInterval)
	var wakerRunner *waker.Waker
	if cfg.WakerInterval > 0 {
		wakerRunner = &waker.Waker{
			Election:        waker.NewElection(b, cfg.NodeID, cfg.SessionID, cfg.WakerTTL),
			Batch:           cfg.WakerBatch,
			FiredTTL:        cfg.WakerFiredTTL,
			CronMaxAttempts: cfg.CronMaxAttempts,
			BackoffMax:      cfg.WakerBackoffMax,
			// DeadScopes reads the bucket wake index (scopes with a pending
			// timer) and keeps only those whose owner is gone, so the waker covers
			// timers/alarms after an owner dies without listing every cell.
			DeadScopes: func(wctx context.Context) ([]string, error) {
				scopes, err := wakeIdx.DueScopes(wctx, time.Now().UnixMilli())
				if err != nil {
					return nil, err
				}
				now := time.Now()
				out := make([]string, 0, len(scopes))
				for _, sc := range scopes {
					o, _, rerr := om.Resolve(wctx, sc)
					live, lerr := om.Live(wctx, o, now)
					if rerr != nil || lerr != nil || !live {
						out = append(out, sc.String())
					}
				}
				return out, nil
			},
			Open: openTimerScope, Dispatcher: dispatcher, Interval: cfg.WakerInterval,
			Forget: func(fctx context.Context, scope string) error {
				return forgetDeletedWake(fctx, scope)
			},
			Log: log,
			Cleanup: func(cctx context.Context) (int, error) {
				// Page 512 × up to 32 passes per 10m leader interval so historical
				// wake/due orphans (especially on CD-unsupported backends) drain
				// instead of stalling at ADR-267's former 128/pass ceiling.
				return wakeIdx.CleanupDrain(cctx, time.Now().Add(-time.Hour).UnixMilli(), 512, 16384, 16384)
			},
		}
	}

	// Control plane (ADR-036): per-app control cells in the same SQLite store;
	// secrets are envelope-encrypted with a root key that lives outside the cell.
	var env *control.Envelope
	if cfg.SecretKey != "" {
		key, kerr := control.ParseRootKey(cfg.SecretKey)
		if kerr != nil {
			log.Warn("control secret root key invalid; secrets disabled", "err", kerr)
		} else if e, eerr := control.NewEnvelope(key); eerr == nil {
			env = e
		} else {
			log.Warn("control envelope init failed; secrets disabled", "err", eerr)
		}
	}
	ctrl := control.New(store, env)
	// Server.ensureControlSchema initializes the authoritative cell before
	// capture labels its unnumbered DDL transactions. Do not pre-open a
	// non-owner's possibly stale SQLite at startup.
	enricher.proj = projCache

	// Autoscaler: read node lease signals -> recommend -> drive the (pluggable)
	// actuator. Default actuator only logs; a deployment plugs in its orchestrator.
	advisor := &autoscaler.Advisor{
		Min: cfg.AutoscaleMin, Max: cfg.AutoscaleMax,
		TargetCellsPerNode: cfg.CellsPerNode, Sample: lm.Sample,
		Cooldown: cfg.AutoscaleCooldown,
	}
	if cfg.AutoscaleInterval > 0 {
		advisor.Run(ctx, cfg.AutoscaleInterval, nil, log)
	}

	alarmDisp.Bundle = func(bctx context.Context, ns, worker string) (string, int, bool) {
		v, ok, err := projCache.activeVersion(bctx, ns, worker)
		if err != nil {
			return "", 0, false
		}
		return v.BundleSHA, v.Number, ok
	}

	// Cron scheduler: materialize due slots as KindCron timers (ADR-076). The
	// timer runner then dispatches them (enriched with worker/bundle, ADR-070).
	var cronScheduler *cron.Scheduler
	if cfg.CronInterval > 0 {
		// Pass serializes these callbacks. Bracket the first projection used
		// to compile a version because Projection reads its revision last.
		var workflowRefs map[control.ResourceRef]struct{}
		var cronVersion string
		cronScheduler = &cron.Scheduler{
			Interval: cfg.CronInterval,
			Version: func(ctx context.Context) (string, error) {
				etag, authority, err := controlReads.RevisionWithAuthority(ctx)
				if err != nil {
					return "", err
				}
				cronVersion = authority + "/" + etag
				return cronVersion, nil
			},
			Log:      log,
			Open:     openTimer,
			Register: func(scope string) { timerReg.Add(scope) },
			Targets: func(cctx context.Context) ([]cron.Target, error) {
				proj, authority, err := controlReads.ProjectionWithAuthority(cctx)
				if err != nil {
					return nil, err
				}
				if authority+"/"+proj.ETag != cronVersion {
					return nil, errors.New("cron: control version changed during worker discovery")
				}
				workflowRefs = make(map[control.ResourceRef]struct{})
				for _, app := range proj.Apps {
					for _, worker := range app.Workers {
						for _, binding := range worker.Version.Bindings {
							if binding.Type == "workflow" {
								workflowRefs[control.ResourceRef{Namespace: app.Namespace, Name: binding.ID}] = struct{}{}
							}
						}
					}
				}
				ct := proj.CronTargets()
				out := make([]cron.Target, 0, len(ct))
				for _, t := range ct {
					out = append(out, cron.Target{Namespace: t.Namespace, Worker: t.Worker, Cron: t.Cron})
				}
				return out, nil
			},
			WorkflowTargets: func(cctx context.Context) ([]cron.WorkflowTarget, error) {
				out := []cron.WorkflowTarget{}
				for ref := range workflowRefs {
					raw, err := controlReads.ResourceConfig(cctx, ref.Namespace, "workflow", ref.Name)
					if errors.Is(err, control.ErrNotFound) {
						continue
					}
					if err != nil {
						return nil, err
					}
					var policy workflow.Policy
					if err := json.Unmarshal(raw, &policy); err != nil {
						return nil, err
					}
					for _, expr := range policy.Schedules {
						out = append(out, cron.WorkflowTarget{Namespace: ref.Namespace, Workflow: ref.Name, Cron: expr})
					}
				}
				return out, nil
			},
		}
	}

	srv := server.New(server.Deps{
		PeerTargets:      targets,
		StartupListener:  startupListener,
		Cfg:              cfg,
		Bucket:           b,
		Lease:            lm,
		Owner:            om,
		Replica:          rep,
		Spool:            spool,
		PeerMgr:          peerMgr,
		PeerShipper:      shipper,
		NodeLog:          nodeLog,
		Recovery:         takeoverRecovery,
		Uploader:         uploader,
		PeerUploader:     peerUploader,
		Store:            store,
		Log:              log,
		Timers:           timerReg,
		OpenTimer:        openTimer,
		Control:          ctrl,
		D1:               d1.New(store),
		R2:               r2.New(b),
		Queue:            queue.New(store),
		Vectorize:        vectorize.New(store),
		Workflows:        workflowStore,
		DispatchWorkflow: wfDisp.Run,
		Admission:        admission.New(cfg.NSRPS, cfg.NSBurst),
		Advisor:          advisor,
		// Bounded per-worker log tail buffer: without it both the ingest and the
		// query endpoints return 503 and `cellhive tail --worker` is unusable.
		Logs: newLogBuffer(cfg),
	})
	controlReads = srv.BackgroundControl()
	projCache.src = controlReads
	enricher.ctrl = controlReads
	go runMultipartGC(bucket.WithReadSource(ctx, bucket.MaintenanceRead), log, srv.R2.GCStaleUploadsPage)
	if telemetry.LogsMode() == telemetry.LogsAll || telemetry.LogsMode() == telemetry.LogsTail {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				if telemetry.LogsMode() == telemetry.LogsAll {
					srv.PullRuntimeLogs(ctx, "", "")
				}
				if telemetry.LogsMode() == telemetry.LogsTail {
					for _, sub := range telemetry.ActiveLogSubscriptions() {
						srv.PullRuntimeLogs(ctx, sub[0], sub[1])
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	// Backend-A capture: wire cellstore-backed cells (KV/D1/Queue/Workflow/…)
	// into the replication chain. Writes are acked only after a fleet (peer
	// fsync) proof; the bucket upload is asynchronous/batched (ADR-092).
	// CELLHIVE_CAPTURE=off disables it (benchmarks / local-only deployments):
	// writes then stay on local disk with no replication proof.
	var captureMgr *cellcapture.Manager
	if v := os.Getenv("CELLHIVE_CAPTURE"); v == "off" || v == "0" || v == "false" {
		log.Warn("backend-A capture disabled: writes are local-only (no replication proof)")
	} else {
		captureMgr = &cellcapture.Manager{
			Store:        store,
			Committer:    srv.CaptureCommitter(),
			AutoSnapshot: true,
			PrepareContinuation: func(cctx context.Context, sc cell.Scope) error {
				if sc == control.Scope() {
					return ctrl.EnsureSchema(cctx)
				}
				return nil
			},
			Durable: func(cctx context.Context, sc cell.Scope, epoch uint64) (uint64, error) {
				return rep.DurableWatermarkChain(cctx, sc, epoch)
			},
			DurableEpoch: func(cctx context.Context, sc cell.Scope, epoch uint64) (uint64, error) {
				return rep.DurableWatermark(cctx, sc, epoch)
			},
			HasEpoch: func(cctx context.Context, sc cell.Scope, epoch uint64) (bool, error) {
				epochs, eerr := rep.Epochs(cctx, sc, epoch)
				if eerr != nil {
					return false, eerr
				}
				for _, e := range epochs {
					if e == epoch {
						return true, nil
					}
				}
				return false, nil
			},
			EpochSnapshotMaxBytes: cfg.CaptureEpochSnapshotMaxBytes,
			WriteContinuation: func(ctx context.Context, s cell.Scope, epoch uint64, h ltx.NativeHeader, pages []ltx.WALPage) error {
				return rep.AppendContinuation(ctx, s, epoch, h, pages...)
			},
			CheckContinuation: rep.CheckContinuation,
			RetireEpochsFunc: func(cctx context.Context, sc cell.Scope, epoch uint64) (int, error) {
				c, release, err := store.AcquireResident(cctx, sc)
				if err != nil || c == nil {
					return 0, err
				}
				defer release()
				return rep.RetireEpochsWithOptions(cctx, sc, epoch, cfg.EpochGCKeepPrevious, replica.RetireOptions{
					Safe: true, Grace: time.Hour,
					Ready: func(context.Context) (bool, error) { return c.PagedComplete(), nil },
					Verify: func(vctx context.Context) error {
						o, _, err := om.Resolve(vctx, sc)
						if err != nil {
							return err
						}
						if o.Node != cfg.NodeID || o.Session != om.Session || o.Epoch != epoch {
							return owner.ErrNotOwner
						}
						live, err := om.Live(vctx, o, time.Now())
						if err != nil {
							return err
						}
						if !live {
							return owner.ErrNotOwner
						}
						return nil
					},
				})
			},
			// celld publishes snapshot bytes and the epoch continuation atomically
			// through one canonical LTX range object.
			OnEnsure:            srv.RecordCaptureEnsure,
			AutoCheckpointBytes: cfg.CellWALCheckpointBytes,
			GroupCommitWait:     cfg.CaptureGroupCommitWait,
			PipelineThreshold:   cfg.CapturePipelineThreshold,
			SnapshotMaxBytes:    cfg.CaptureSnapshotMaxBytes,
			SnapshotPartBytes:   cfg.CaptureSnapshotPartBytes,
			Log:                 log,
			Owner: func(cctx context.Context, sc cell.Scope) (bool, uint64, error) {
				o, _, err := om.ResolveRoute(cctx, sc)
				if err != nil {
					return false, 0, err
				}
				live, lerr := om.LiveRoute(cctx, sc, o, time.Now())
				return live && o.Node == cfg.NodeID && o.Session == cfg.SessionID, o.Epoch, lerr
			},
			// Ownership moved: forget the local cell so later reads re-hydrate
			// from the bucket instead of serving a stale local copy (ADR-115).
			OnLostOwner: func(sc cell.Scope, epoch uint64) {
				// Passive loss never runs BeforeRelease. Drop that epoch even
				// when the local database file is kept for the janitor.
				rep.LoseL0(sc, epoch)
				if !cfg.ForgetOnLoss {
					return // keep the file; the disk-budget janitor reclaims LRU files
				}
				if err := store.Forget(context.Background(), sc); err != nil {
					log.Warn("forget cell after ownership loss failed", "scope", sc.String(), "err", err)
				}
			},
		}
		if captureMgr != nil {
			prevForget := store.OnForget
			store.OnForget = func(sc cell.Scope) {
				if prevForget != nil {
					prevForget(sc)
				}
				captureMgr.ForgetHint(sc)
			}
			// Eviction keeps the resume hint: the next Ensure uses it to avoid a
			// cold List/Get. Only explicit Forget/ownership loss clears it.
			prevEvict := store.OnEvicted
			store.OnEvicted = func(sc cell.Scope) {
				if prevEvict != nil {
					prevEvict(sc)
				}
			}
		}
		srv.Capture = captureMgr
		// Start only after capture wiring is complete. Read-only takeovers
		// need background continuation initialization before Fold can proceed.
		go runEpochFoldLoop(bucket.WithReadSource(ctx, bucket.EpochFoldRead), log, rep, om, cfg, captureMgr.EnsureContinuation)
		go runEpochGCLoop(bucket.WithReadSource(ctx, bucket.EpochGCRead), log, om, captureMgr, cfg.EpochGCInterval, cfg.EpochGCInitial)
		// Queue consumer dispatch: reconcile registered queues each minute and use
		// owner-local mutation hints and message deadlines to deliver claimed batches
		// to the consuming worker via the dispatch endpoint (docs/bindings.md). Only
		// the node that owns a queue's cell consumes it (OwnerGate claims unowned or
		// expired cells), and every store mutation is captured with a durability
		// proof, so a queue has exactly one consumer and RPO=0 (ADR-119).
		if cfg.QueueInterval > 0 {
			if qd := queue.NewHTTP(cfg.DispatchURL, cfg.TokenDispatch); qd != nil {
				resources := &queueResourceCache{src: controlReads}
				runner := &queue.Runner{
					Store:        srv.Queue,
					Resolve:      controlReads.QueueConsumer,
					Dispatch:     qd,
					Interval:     cfg.QueueInterval,
					Batch:        cfg.QueueBatch,
					LeaseMs:      cfg.QueueLease.Milliseconds(),
					RetryDelayMs: cfg.QueueRetryDelay.Milliseconds(),
					Log:          log,
					Filter:       queue.PreparedOwnerGate(om, srv.PrepareResourceAccess, resetLocal),
					Commit: func(cctx context.Context, ns, name string, fn func() error) error {
						sc := queue.Scope(ns, name)
						end, err := srv.BeginResourceRequest(cctx, sc)
						if err != nil {
							return err
						}
						defer end()
						blocked, err := rep.ResourceBlocked(cctx, sc)
						if err != nil {
							return err
						}
						if blocked {
							return replica.ErrResourceDeleted
						}
						return srv.CaptureWrite(cctx, queue.Scope(ns, name), fn)
					},
					DeadLetterSend:   srv.EnqueueToQueueOwner,
					ValidateDispatch: queue.OwnerCheck(om),
					AdmitRead: func(ctx context.Context, ref queue.Ref) (func(), error) {
						return srv.BeginResourceRequest(ctx, queue.Scope(ref.Namespace, ref.Name))
					},
					AdmitDispatch: func(ctx context.Context, ref queue.Ref) (func(), error) {
						return srv.BeginResourceRequest(ctx, queue.Scope(ref.Namespace, ref.Name))
					},
					Queues: func(qctx context.Context) ([]queue.Ref, error) {
						queues, err := resources.Resources(qctx)
						if err != nil {
							return nil, err
						}
						targets, err := projCache.queueTargets(qctx)
						if err != nil {
							return nil, err
						}
						refs, err := resolveQueueRefs(queues, targets)
						if err != nil {
							log.Warn("queue consumer resource resolution failed", "err", err)
						}
						return refs, nil
					},
				}
				srv.QueueLocalNudge = runner.Nudge
				srv.QueueNudge = runner.Nudge
				if shared != nil {
					runner.ProjectDeadline = func(ns, name string, at time.Time) {
						shared.Notify(scheduleindex.Hint{Kind: "queue", Scope: queue.Scope(ns, name).String(), Due: at.UnixMilli()})
					}
					srv.QueueNudge = func(ns, name string) {
						runner.Nudge(ns, name)
						shared.Notify(scheduleindex.Hint{Kind: "queue", Scope: queue.Scope(ns, name).String()})
					}
				}
				runner.Start(bucket.WithReadSource(ctx, bucket.QueueDispatch))
			}
		}
		// Capture keeps running until HTTP has quiesced; the shutdown path below
		// drains it so the local cell never ends ahead of the durable chain.
	}

	// Cell-handle eviction (ADR-096): bound open fds/memory by closing idle or
	// excess cached cells. Eviction stops the cell's capture first; ownership is
	// untouched, so the next request reopens the cell locally.
	// Reclamation requires current control ownership and a durable exclusion.
	// Unknown I/O outcomes retain their fence instead of granting a new holder.
	if shared != nil {
		store.EvictionReady = srv.ScheduleOutboxEvictionReady
		srv.ProjectMaintenance = func(kind string, at time.Time) {
			shared.Notify(scheduleindex.Hint{Kind: kind, Scope: control.Scope().String(), Due: at.UnixMilli()})
		}
		srv.Control.SchedulePurge = func(at time.Time) { srv.ProjectMaintenance("purge", at) }
		if cronScheduler != nil {
			srv.CronNudge = cronScheduler.Nudge
			cronScheduler.ProjectNext = func(ctx context.Context, at time.Time) error {
				if at.IsZero() {
					return nil
				}
				return shared.Publish(ctx, scheduleindex.Hint{Kind: "cron", Scope: control.Scope().String(), Due: at.UnixMilli()})
			}
		}
	}
	go srv.RunPurgeLoop(bucket.WithReadSource(ctx, bucket.PurgeRead), 5*time.Second)
	if cfg.AuditRetention > 0 {
		go runAuditPruneLoop(bucket.WithReadSource(ctx, bucket.MaintenanceRead), log, srv, cfg.AuditRetention)
	}
	if cfg.WorkflowRetention > 0 {
		go runDefaultWorkflowRetentionLoop(bucket.WithReadSource(ctx, bucket.WorkflowRetention), log, srv, cfg.WorkflowRetention)
	}
	go runWorkflowRetentionLoop(bucket.WithReadSource(ctx, bucket.WorkflowRetention), log, srv)
	if cfg.BundleGCInterval > 0 {
		go srv.RunArtifactGCLoop(bucket.WithReadSource(ctx, bucket.MaintenanceRead), cfg.BundleGCInterval)
	}
	store.MaxOpenCells = cfg.MaxOpenCells
	store.IdleTTL = cfg.CellIdleTTL
	if !cfg.ForgetOnLoss && cfg.CellDiskMax <= 0 {
		log.Warn("local cell files are kept after ownership loss and no disk budget is set; " +
			"the local disk can grow unbounded (set CELLHIVE_CELL_DISK_MAX, or CELLHIVE_FORGET_ON_LOSS=true)")
	}
	// Local disk management (ADR-122/123): a janitor enforces the byte budget
	// (LRU over non-owned files, plus idle owned files whose state the bucket
	// already covers) and maintains the disk-pressure signal used to refuse new
	// claims and release idle cells.
	var diskPressure atomic.Bool
	srv.Overloaded = diskPressure.Load
	if cfg.CellDiskMax > 0 || cfg.DiskHigh > 0 {
		go runCellDiskLoop(bucket.WithReadSource(ctx, bucket.MaintenanceRead), log, store, om, rep, lm, cfg, &diskPressure)
	}
	if captureMgr != nil {
		store.Drop = captureMgr.Drop
	}
	store.Start(ctx)
	// Background timer work uses the same owner and capture contract as API
	// writes. Serialize claims per scope so a second local loop cannot open a
	// stale SQLite copy while the first loop is restoring a newly claimed cell.
	var timerOwnerLocks [64]sync.Mutex
	timerOwn := func(tctx context.Context, sc cell.Scope) (bool, error) {
		if err := srv.PrepareResourceAccess(tctx, sc); err != nil {
			return false, err
		}
		h := fnv.New32a()
		_, _ = h.Write([]byte(sc.String()))
		mu := &timerOwnerLocks[h.Sum32()%uint32(len(timerOwnerLocks))]
		mu.Lock()
		defer mu.Unlock()
		now := time.Now()
		current, _, err := om.Resolve(tctx, sc)
		live := false
		if err == nil {
			var lerr error
			live, lerr = om.Live(tctx, current, now)
			if lerr != nil {
				return false, lerr
			}
		}
		if err == nil && live {
			if current.Node != cfg.NodeID || current.Session != cfg.SessionID {
				return false, nil
			}
		} else {
			if err != nil && !errors.Is(err, owner.ErrUnowned) {
				return false, err
			}
			if srv.Overloaded != nil && srv.Overloaded() {
				return false, nil
			}
			_, cerr := om.ClaimWithReset(tctx, sc, now, resetLocal)
			if cerr != nil {
				if errors.Is(cerr, owner.ErrOwnerLive) || errors.Is(cerr, bucket.ErrPrecondition) {
					return false, nil
				}
				return false, cerr
			}
		}
		return true, nil
	}
	if shared != nil {
		srv.ScheduleAcquire = timerOwn
	}
	timerCommit := func(tctx context.Context, scope string, fn func() error) error {
		sc, err := cell.ParseScope(scope)
		if err != nil {
			return err
		}
		o, _, err := om.Resolve(tctx, sc)
		if err != nil {
			return err
		}
		live, lerr := om.Live(tctx, o, time.Now())
		if lerr != nil || !live || o.Node != cfg.NodeID || o.Session != cfg.SessionID {
			return owner.ErrNotOwner
		}
		return srv.CaptureWrite(tctx, sc, fn)
	}
	timerAdmit := func(tctx context.Context, scope string) (func(), error) {
		sc, err := cell.ParseScope(scope)
		if err != nil {
			return nil, err
		}
		release, err := srv.BeginResourceRequest(tctx, sc)
		if err != nil {
			return nil, err
		}
		if captureMgr != nil {
			if _, err := captureMgr.Ensure(tctx, sc); err != nil {
				release()
				return nil, err
			}
		}
		return release, nil
	}
	if timerRunner != nil {
		timerRunner.Own = func(tctx context.Context, scope string) (bool, error) {
			sc, err := cell.ParseScope(scope)
			if err != nil {
				return false, err
			}
			return timerOwn(tctx, sc)
		}
		timerRunner.Admit = timerAdmit
		timerRunner.Commit = timerCommit
		timerRunner.Start(bucket.WithReadSource(ctx, bucket.TimerDispatch))
	}
	if wakerRunner != nil {
		wakerRunner.Own = func(tctx context.Context, scope string) (bool, error) {
			sc, err := cell.ParseScope(scope)
			if err != nil {
				return false, err
			}
			owned, err := timerOwn(tctx, sc)
			if err == nil && owned && timerReg != nil {
				// The waker may have just taken over a dead owner's timer cell.
				// Register it with the local runner so future/retried timers in
				// that cell are not left waiting for the 5-minute repair sweep.
				timerReg.Add(scope)
			}
			return owned, err
		}
		wakerRunner.Admit = timerAdmit
		wakerRunner.Commit = timerCommit
		wakerRunner.Start(bucket.WithReadSource(ctx, bucket.WakeScan))
	}
	if cronScheduler != nil {
		cronScheduler.Own = func(tctx context.Context, sc cell.Scope) (bool, error) {
			owned, err := timerOwn(tctx, sc)
			if err != nil || !owned {
				return owned, err
			}
			if captureMgr != nil {
				if _, err := captureMgr.Ensure(tctx, sc); err != nil {
					return false, err
				}
			}
			return true, nil
		}
		cronScheduler.Commit = func(tctx context.Context, sc cell.Scope, fn func() error) error {
			return timerCommit(tctx, sc.String(), fn)
		}
		cronScheduler.Start(bucket.WithReadSource(ctx, bucket.CronPlan))
	}
	if cfg.WakeRepairInterval > 0 {
		go runWakeRepairLoop(bucket.WithReadSource(ctx, bucket.WakeRepair), log, store, wakeIdx, om, srv, openTimer, cfg)
	}
	if cfg.RebalanceInterval > 0 {
		go runRebalanceLoop(bucket.WithReadSource(ctx, bucket.RebalanceRead), log, cfg, om, store, rep, lm, timerReg, placementWeight)
	}

	// Re-register timers this node owns from the wake index: after a restart the
	// in-memory registry is empty, but the bucket index still lists the scopes
	// with pending timers.
	go func() {
		if n, err := wakeIdx.Migrate(ctx, 0); err != nil {
			log.Warn("wake legacy migration failed", "migrated", n, "err", err)
		} else if n > 0 {
			log.Info("migrated legacy wake entries", "count", n)
		}
		entries, err := wakeIdx.List(ctx)
		if err != nil {
			return
		}
		now := time.Now()
		for _, e := range entries {
			sc, perr := cell.ParseScope(e.Scope)
			if perr != nil {
				continue
			}
			o, _, rerr := om.Resolve(ctx, sc)
			live, lerr := om.Live(ctx, o, now)
			if rerr != nil || lerr != nil || !live || o.Node != cfg.NodeID {
				continue
			}
			registerTimerScope(ctx, openTimer, srv, sc)
		}
	}()
	go srv.RunHyperdriveProxyHealthLoop(bucket.WithReadSource(ctx, bucket.MaintenanceRead))
	if shared != nil {
		indexCtx, stopIndex := context.WithCancel(ctx)
		indexDone := make(chan struct{})
		go func() { defer close(indexDone); shared.Run(indexCtx, srv.DeliverScheduleHint) }()
		outboxDone := make(chan struct{})
		go func() { defer close(outboxDone); srv.RunScheduleOutbox(indexCtx, shared.Publish) }()
		defer func() { stopIndex(); <-outboxDone }()
		defer func() { stopIndex(); <-indexDone }()
	}
	srv.SetReady(true)
	go srv.RunResourceCleanupLoop(bucket.WithReadSource(ctx, bucket.ResourceCleanup))
	go srv.RunCellTransferCleanupLoop(bucket.WithReadSource(ctx, bucket.TransferCleanup))
	go srv.RunProxySQLReconcileLoop(bucket.WithReadSource(ctx, bucket.MaintenanceRead))

	// Graceful handoff (cell-protocol §7): mark draining as soon as shutdown
	// begins so no new cell is claimed, then serialize the actual handoff with
	// the bucket drain token after HTTP has quiesced.
	drainMgr := drain.New(b, cfg.NodeID, cfg.SessionID, cfg.DrainTTL, 5*time.Second)
	go func() {
		<-ctx.Done()
		srv.SetDraining(true)
	}()

	if err := srv.Run(ctx); err != nil {
		log.Error("server stopped", "err", err)
		time.Sleep(100 * time.Millisecond)
		if captureMgr != nil {
			dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
			if derr := captureMgr.Drain(dctx); derr != nil {
				log.Warn("capture drain", "err", derr)
			}
			dcancel()
			captureMgr.Stop()
		}
		performHandoff(log, cfg, nodeLog, om, drainMgr, uploader)
		upCancel()
		shipper.Stop()
		peerUploader.Stop()
		uploader.Stop()
		_ = store.Close()
		os.Exit(1)
	}
	// Let in-flight background loops observe the cancelled context before the
	// final capture drain; otherwise a late control write lands after the drain
	// and leaves the local cell one txid ahead of the durable chain.
	time.Sleep(100 * time.Millisecond)
	if captureMgr != nil {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if derr := captureMgr.Drain(dctx); derr != nil {
			log.Warn("capture drain", "err", derr)
		}
		dcancel()
		captureMgr.Stop()
	}
	performHandoff(log, cfg, nodeLog, om, drainMgr, uploader)
	upCancel()
	shipper.Stop()
	peerUploader.Stop()
	uploader.Stop()
	if err := store.Close(); err != nil {
		log.Warn("close cell store", "err", err)
	}
	log.Info("cell-agent stopped")
}

// runEpochGCLoop collects superseded native epochs after local hydration and
// fresh ownership checks. The previous epoch and one-hour age margin remain.
func runEpochGCLoop(ctx context.Context, log *slog.Logger, om *owner.Manager, cm *cellcapture.Manager, interval, initial time.Duration) {
	if interval <= 0 {
		return
	}
	pass := func() {
		for _, o := range om.OwnedScopes() {
			n, err := cm.RetireEpochs(ctx, o.Scope, o.Epoch)
			if errors.Is(err, replica.ErrGCDeferred) {
				log.Info("epoch gc deferred", "scope", o.Scope.String(), "reason", err)
				continue
			}
			if err != nil {
				log.Warn("epoch gc failed", "scope", o.Scope.String(), "err", err)
				continue
			}
			if n > 0 {
				log.Info("epoch gc", "scope", o.Scope.String(), "epoch", o.Epoch, "objects", n)
			}
		}
	}
	if initial <= 0 {
		initial = time.Minute
	}
	timer := time.NewTimer(initial)
	defer timer.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pass()
		case <-ticker.C:
			pass()
		}
	}
}

const defaultCompactionTimeout = 5 * time.Minute

// idleShouldFold reports whether an idle cell should rehome its restore chain.
// A chain that still reads a predecessor epoch folds immediately. A chain that
// already lives in this epoch waits for the owner's L0 tally. Neither check
// lists the bucket; the predecessor check reads the restore plan.
func idleShouldFold(ctx context.Context, rep *replica.Manager, sc cell.Scope, epoch uint64, minObjects int) (bool, error) {
	if rep == nil {
		return false, nil
	}
	keys, err := rep.RestoreChainKeys(ctx, sc, epoch)
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		got, ok := ltxEpoch(key)
		if ok && got != epoch {
			return true, nil
		}
	}
	return idleL0Ready(rep, sc, epoch, minObjects), nil
}

func ltxEpoch(key string) (uint64, bool) {
	const marker = "/ltx/e"
	i := strings.Index(key, marker)
	if i < 0 {
		return 0, false
	}
	rest := key[i+len(marker):]
	j := strings.IndexByte(rest, '/')
	if j <= 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(rest[:j], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// idleL0Ready reports whether this owner's unpublished L0 tally is large
// enough to fold on idle eviction. It does not list the bucket.
func idleL0Ready(rep *replica.Manager, sc cell.Scope, epoch uint64, minObjects int) bool {
	if rep == nil {
		return false
	}
	minObjects = objectCountBar(minObjects)
	n, _ := rep.L0Pending(sc, epoch)
	return n >= minObjects
}

type compactionScope struct {
	scope  cell.Scope
	epoch  uint64
	verify func(context.Context) error
}

// compactionNudge is one scope whose L0 just became durable.
type compactionNudge struct {
	scope cell.Scope
	epoch uint64
}

// compactionWake coalesces per-scope nudges so the upload loop never waits on compaction.
type compactionWake struct {
	mu       sync.Mutex
	pending  map[string]compactionNudge
	attempts map[string]int
	ch       chan struct{}
}

func newCompactionWake() *compactionWake {
	return &compactionWake{pending: map[string]compactionNudge{}, ch: make(chan struct{}, 1)}
}

func (w *compactionWake) Nudge(sc cell.Scope, epoch uint64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.pending[compactionPendingKey(sc, epoch)] = compactionNudge{scope: sc, epoch: epoch}
	w.mu.Unlock()
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *compactionWake) take() []compactionNudge {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]compactionNudge, 0, len(w.pending))
	for _, n := range w.pending {
		out = append(out, n)
	}
	clear(w.pending)
	return out
}

func compactionPendingKey(sc cell.Scope, epoch uint64) string {
	return sc.String() + "\x00" + strconv.FormatUint(epoch, 10)
}

func (w *compactionWake) attempt(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.attempts[key]
}

func (w *compactionWake) noteAttempt(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.attempts == nil {
		w.attempts = map[string]int{}
	}
	w.attempts[key]++
}

func (w *compactionWake) clearAttempt(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.attempts, key)
}

// runCompactionScheduler promotes a nudged scope when that level's count or
// byte bar is met. The nudge comes from a durable L0 upload. There is no scan
// interval.
func runCompactionScheduler(ctx context.Context, log *slog.Logger, om *owner.Manager, rep *replica.Manager, cfg config.Config, wake *compactionWake) {
	if wake == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake.ch:
			compactScopes(ctx, log, rep, cfg, wake, nudgeCompactionScopes(wake.take(), om, cfg))
		}
	}
}

func nudgeCompactionScopes(nudges []compactionNudge, om *owner.Manager, cfg config.Config) []compactionScope {
	scopes := make([]compactionScope, 0, len(nudges))
	for _, n := range nudges {
		scopes = append(scopes, compactionWork(om, cfg, n.scope, n.epoch))
	}
	return scopes
}

func compactionWork(om *owner.Manager, cfg config.Config, sc cell.Scope, epoch uint64) compactionScope {
	return compactionScope{
		scope: sc,
		epoch: epoch,
		verify: func(vctx context.Context) error {
			o, _, err := om.Resolve(vctx, sc)
			if err != nil {
				return err
			}
			if o.Node != cfg.NodeID || o.Epoch != epoch {
				return errCompactionOwnership
			}
			return nil
		},
	}
}

// compactScopes bounds each scope independently so one hung bucket operation
// cannot delay every later owned scope. Ownership and epoch checks stay in the
// per-scope Verify callback.
func compactScopes(ctx context.Context, log *slog.Logger, rep *replica.Manager, cfg config.Config, wake *compactionWake, scopes []compactionScope) {
	timeout := cfg.CompactionTimeout
	if timeout <= 0 {
		timeout = defaultCompactionTimeout
	}
	compactAndFollow(ctx, log, wake, scopes, func(owned compactionScope) (bool, error) {
		compact := levelCompactor(log, rep, owned)
		cue := func(context.Context, uint16) (compaction.Cue, error) {
			return compaction.UploadCue(rep, owned.scope, owned.epoch), nil
		}
		return compactScopeCascade(ctx, timeout, cfg, cue, owned.verify, compact)
	})
}

// errCompactionOwnership means this process no longer owns the epoch. The
// scheduler must not retry that scope; the new owner compacts its own uploads.
var errCompactionOwnership = errors.New("compaction ownership changed")

const (
	compactionRetryBase     = time.Second
	compactionRetryMax      = time.Minute
	compactionContinueDelay = 50 * time.Millisecond
)

// scheduleCompactionRetry waits, then nudges, unless the process is stopping.
// Tests replace it so they do not sleep.
var scheduleCompactionRetry = func(ctx context.Context, delay time.Duration, nudge func()) {
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			nudge()
		}
	}()
}

// compactionFollowUp decides whether one scope should be nudged again.
// A lost owner or a shutdown is final. A storage or timeout error backs off.
// A batch that is still over the bar yields briefly so other scopes run.
func compactionFollowUp(again bool, err error, attempt int) (bool, time.Duration) {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return false, 0
		}
		if errors.Is(err, errCompactionOwnership) {
			return false, 0
		}
		return true, compactionBackoff(attempt)
	}
	if again {
		return true, compactionContinueDelay
	}
	return false, 0
}

func compactionBackoff(attempt int) time.Duration {
	delay := compactionRetryBase
	for i := 0; i < attempt; i++ {
		if delay >= compactionRetryMax/2 {
			return compactionRetryMax
		}
		delay *= 2
	}
	if delay > compactionRetryMax {
		return compactionRetryMax
	}
	return delay
}

func compactAndFollow(ctx context.Context, log *slog.Logger, wake *compactionWake, scopes []compactionScope, run func(compactionScope) (bool, error)) {
	for _, owned := range scopes {
		again, err := run(owned)
		if err != nil && log != nil {
			// Do not emit scope/key names or backend error text; those can contain
			// tenant identifiers and object names.
			log.Warn("compaction failed", "epoch", owned.epoch)
		}
		if wake == nil {
			continue
		}
		key := compactionPendingKey(owned.scope, owned.epoch)
		retry, delay := compactionFollowUp(again, err, wake.attempt(key))
		if !retry {
			wake.clearAttempt(key)
			continue
		}
		wake.noteAttempt(key)
		scope, epoch := owned.scope, owned.epoch
		scheduleCompactionRetry(ctx, delay, func() { wake.Nudge(scope, epoch) })
	}
}

func levelCompactor(log *slog.Logger, rep *replica.Manager, owned compactionScope) levelCompactFunc {
	return func(cctx context.Context, level uint16, opts compaction.Options) (compaction.Result, error) {
		res, err := compaction.CompactLevel(cctx, rep, owned.scope, owned.epoch, level, opts)
		if err != nil {
			return res, err
		}
		if res.Compacted {
			log.Info("compacted", "level", level, "epoch", owned.epoch, "inputs", res.Inputs,
				"parts", res.Parts, "min_txid", res.MinTxID, "max_txid", res.MaxTxID,
				"stored_bytes", res.InputBytes)
		}
		return res, nil
	}
}

// levelCompactFunc is the per-level compaction hook (production wraps CompactLevel).
type levelCompactFunc func(ctx context.Context, level uint16, opts compaction.Options) (compaction.Result, error)

// levelCueFunc reports the uncovered source tail for a destination level.
type levelCueFunc func(ctx context.Context, level uint16) (compaction.Cue, error)

// compactionBatchLimit stops one pass from publishing forever if the cue does
// not shrink. A batch takes whole LTX objects, up to the default object count,
// and does not cut them by size.
const compactionBatchLimit = 32

// compactScopeCascade promotes L0 to L1 when the object count or stored-byte
// bar is met. It keeps going until the bar drops or a batch publishes nothing.
// L1 objects stay at L1.
func compactScopeCascade(ctx context.Context, timeout time.Duration, cfg config.Config, cue levelCueFunc, verify func(context.Context) error, compact levelCompactFunc) (bool, error) {
	policy := promotePolicy(cfg)
	return compactWhile(ctx, timeout, 1, cue, verify, compact, func(c compaction.Cue) bool {
		return compaction.ShouldPromote(c, policy)
	})
}

// compactWhile publishes one level until due is false, a batch publishes
// nothing, or compactionBatchLimit is reached. again is true when the cap
// stopped a pass that is still due, so the caller can yield and continue
// without listing the bucket.
func compactWhile(ctx context.Context, timeout time.Duration, level uint16, cue levelCueFunc, verify func(context.Context) error, compact levelCompactFunc, due func(compaction.Cue) bool) (bool, error) {
	for range compactionBatchLimit {
		c, err := cue(ctx, level)
		if err != nil {
			return false, err
		}
		if !due(c) {
			return false, nil
		}
		sctx, cancel := context.WithTimeout(ctx, timeout)
		res, err := compact(sctx, level, compaction.Options{Verify: verify, MaxInputBytes: levelMaxInputBytes(level)})
		cancel()
		if err != nil {
			return false, err
		}
		if !res.Compacted {
			return false, nil
		}
	}
	c, err := cue(ctx, level)
	if err != nil {
		return false, err
	}
	return due(c), nil
}

func levelMaxInputBytes(uint16) int64 {
	return -1
}

func promotePolicy(cfg config.Config) compaction.PromotePolicy {
	return compaction.L1PromotePolicy(objectCountBar(cfg.CompactionMinObjects), cfg.CompactionMinBytes)
}

// objectCountBar is the shared L0 file and upload-segment count. A nonpositive
// configuration uses the default instead of disabling the count.
func objectCountBar(n int) int {
	if n <= 0 {
		return compaction.L1MinObjects
	}
	return n
}

// uploadSegmentCap is the async L0 batch size. It is the same count as the L1
// file bar. A nonpositive value uses that bar's default.
func uploadSegmentCap(cfg config.Config) int {
	return objectCountBar(cfg.CompactionMinObjects)
}

// performHandoff runs the shutdown-side of graceful handoff: acquire the bucket
// drain token (serializing concurrent shutdowns), prove every accepted async
// upload reached the bucket, seal this session's node-log, then release every
// owned cell and the token. If upload drain or seal fails, owner records are
// deliberately left in place so the next owner runs predecessor recovery rather
// than hydrating a bucket chain that is missing follower-acked writes.
func performHandoff(log *slog.Logger, cfg config.Config, nl *nodelog.Manager, om *owner.Manager, dm *drain.Manager, up *upload.Batcher) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DrainWait)
	defer cancel()
	if err := dm.Wait(ctx); err != nil {
		log.Warn("drain token not acquired; proceeding best-effort", "err", err)
	}
	// Prove every acknowledged async upload reached the bucket before deleting
	// the owner records. If this fails, keep the records (and open node-log) so
	// the next owner performs predecessor recovery instead of hydrating a
	// bucket chain that is missing follower-acked writes.
	if up != nil {
		if err := up.Flush(ctx); err != nil {
			log.Error("handoff: upload drain failed; leaving owner records for recovery", "err", err)
			_ = dm.Release(ctx)
			return
		}
	}
	if nl != nil {
		if _, err := nl.Seal(ctx, cfg.NodeID, cfg.SessionID); err != nil && !errors.Is(err, nodelog.ErrNotFound) {
			log.Error("handoff: node-log seal failed; leaving owner records for recovery", "err", err)
			_ = dm.Release(ctx)
			return
		}
	}
	// Optional pre-release folds run inside each release, so they share this
	// budget. Give the release phase its own deadline: a slow upload drain must
	// not leave the mandatory owner releases with an expired context.
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), cfg.DrainWait)
	defer releaseCancel()
	released := 0
	for _, owned := range om.OwnedScopes() {
		if err := om.Release(releaseCtx, owned.Scope, owned.Epoch); err != nil {
			log.Warn("owner release failed", "scope", owned.Scope.String(), "err", err)
			continue
		}
		released++
	}
	if err := dm.Release(releaseCtx); err != nil {
		log.Warn("drain token release failed", "err", err)
	}
	log.Info("handoff complete", "released", released)
}

// foldL2MaxInputBytes is one handoff or idle L2 increment. It does not follow
// CELLHIVE_EPOCH_SNAPSHOT_MAX, so raising the whole-database L9 ceiling does
// not turn those increments into a 1GiB merge.
const foldL2MaxInputBytes int64 = 256 << 20

// Optional pre-release fold budget. The fold is best-effort: it must never hold
// the mandatory owner releases, which run in the same context.
var (
	preReleaseFoldLimit   = defaultCompactionTimeout
	preReleaseFoldReserve = 2 * time.Second
	preReleaseFoldMin     = 2 * time.Second
)

// errShedSkipped means a background rebalance or disk shed left the owner in
// place. The cell is open, or this process no longer owns the epoch.
var errShedSkipped = errors.New("shed skipped")

// shedReleaseBudget is the claim-lock cap for deleting the owner after a
// background fold. The fold runs before this, without the lock.
const shedReleaseBudget = 10 * time.Second

type skipPreReleaseFoldKey struct{}

func withSkipPreReleaseFold(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipPreReleaseFoldKey{}, true)
}

func skipPreReleaseFold(ctx context.Context) bool {
	v, _ := ctx.Value(skipPreReleaseFoldKey{}).(bool)
	return v
}

// shedIdleCell folds a closed cell without the claim lock, then deletes the
// owner only if the cell is still closed and this process still owns epoch.
// A fold error still releases: the successor restores whatever chain remains.
// Cancelling ctx during the fold keeps the owner.
func shedIdleCell(ctx context.Context, om *owner.Manager, sc cell.Scope, epoch uint64, idle func() bool, fold func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if idle != nil && !idle() {
		return errShedSkipped
	}
	if fold != nil {
		_ = fold(ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if idle != nil && !idle() {
		return errShedSkipped
	}
	cur, _, err := om.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if cur.Node != om.NodeID {
		return errShedSkipped
	}
	if cur.Session != om.Session {
		return errShedSkipped
	}
	if cur.Epoch != epoch {
		return errShedSkipped
	}
	relCtx, cancel := context.WithTimeout(withSkipPreReleaseFold(ctx), shedReleaseBudget)
	defer cancel()
	err = om.ReleaseWhen(relCtx, sc, epoch, idle)
	if errors.Is(err, owner.ErrReleaseBusy) {
		return errShedSkipped
	}
	return err
}

// foldBudgetFor reports how long an optional fold may run given the release
// deadline. ok=false means "skip the fold": too little time remains after
// reserving space for the release itself.
func foldBudgetFor(ctx context.Context, limit time.Duration) (time.Duration, bool) {
	if limit <= 0 || limit > preReleaseFoldLimit {
		limit = preReleaseFoldLimit
	}
	if deadline, has := ctx.Deadline(); has {
		remaining := time.Until(deadline) - preReleaseFoldReserve
		if remaining <= 0 {
			return 0, false
		}
		if remaining < limit {
			limit = remaining
		}
	}
	if limit < preReleaseFoldMin {
		return 0, false
	}
	return limit, true
}

// runRebalanceLoop releases idle owned cells so the fleet moves toward a
// weight-proportional ownership split (ADR-101). It only schedules or releases
// hibernated (locally closed) cells; a peer claims them on demand.
func runRebalanceLoop(ctx context.Context, log *slog.Logger, cfg config.Config, om *owner.Manager, store *cellstore.Store, rep *replica.Manager, lm *lease.Manager, timers *timer.Registry, weight int) {
	t := time.NewTicker(cfg.RebalanceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			owned := om.OwnedScopes()
			cands := make([]rebalance.Candidate, 0, len(owned))
			for _, o := range owned {
				cands = append(cands, rebalance.Candidate{Scope: o.Scope.String(), Epoch: o.Epoch, Idle: !store.IsOpen(o.Scope)})
			}
			now := time.Now()
			peers := []rebalance.Load{}
			for _, l := range lm.SampleCached(ctx, 5*time.Second) {
				if l.Node == cfg.NodeID || !l.Live(now) {
					continue
				}
				peers = append(peers, rebalance.Load{Node: l.Node, Weight: l.Load.Weight, Owned: l.Load.OwnedCells})
			}
			moves := rebalance.Plan(
				rebalance.Load{Node: cfg.NodeID, Weight: weight, Owned: len(owned)},
				peers, cands, cfg.RebalanceMaxMove)
			for _, m := range moves {
				sc, err := cell.ParseScope(m.Scope)
				if err != nil {
					continue
				}
				if err := shedIdleCell(ctx, om, sc, m.Epoch, func() bool { return !store.IsOpen(sc) }, nil); err != nil {
					if !errors.Is(err, errShedSkipped) {
						log.Warn("rebalance release failed", "scope", m.Scope, "err", err)
					}
					continue
				}
				timers.Remove(m.Scope)
				log.Info("rebalance released cell", "scope", m.Scope, "epoch", m.Epoch)
			}
		}
	}
}

// registerTimerScope arms the local runner for a self-owned scope with pending
// timers (startup re-registration; ADR-099).
// runCellDiskLoop enforces a local disk budget over cell files (ADR-122): cell
// files for scopes this node does not own are pure copies of bucket state, so
// least-recently-used ones are deleted until the total fits the budget. Files of
// scopes this node owns are never evicted (they may hold acked writes the bucket
// has not seen yet).
func runCellDiskLoop(ctx context.Context, log *slog.Logger, store *cellstore.Store, om *owner.Manager, rep *replica.Manager, lm *lease.Manager, cfg config.Config, pressure *atomic.Bool) {
	t := time.NewTicker(cfg.CellDiskSweep)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			files, err := store.DiskFiles()
			if err != nil {
				log.Warn("cell disk scan failed", "err", err)
				continue
			}
			ownedEpoch := map[string]uint64{}
			for _, o := range om.OwnedScopes() {
				ownedEpoch[o.Scope.String()] = o.Epoch
			}
			// Phase 1 is a cheap walk: no cell opens and no bucket reads.
			var total int64
			for i := range files {
				total += files[i].Bytes
			}

			// Disk high watermark: report pressure (the server refuses new claims)
			// only when a live peer has placement headroom — a single node keeps
			// claiming and relies on this janitor to free space instead. Regardless,
			// shed idle owned cells so the fleet can move them.
			over := cfg.DiskHigh > 0 && total > cfg.DiskHigh
			shed := over && lease.HasShedTarget(lm.SampleCached(ctx, 5*time.Second), cfg.NodeID, time.Now())
			pressure.Store(shed)
			if over {
				released := 0
				for _, o := range om.OwnedScopes() {
					if released >= 8 {
						break
					}
					if o.Scope.Class == control.ControlClass || store.IsOpen(o.Scope) {
						continue
					}
					if err := shedIdleCell(ctx, om, o.Scope, o.Epoch, func() bool { return !store.IsOpen(o.Scope) }, nil); err != nil {
						if !errors.Is(err, errShedSkipped) {
							log.Warn("disk pressure release failed", "scope", o.Scope.String(), "err", err)
						}
						continue
					}
					log.Info("disk pressure: released idle cell", "scope", o.Scope.String(), "epoch", o.Epoch)
					released++
				}
			}

			if cfg.CellDiskMax <= 0 || total <= cfg.CellDiskMax {
				continue
			}
			// Phase 2 (only when over budget): pick LRU files, deciding eligibility
			// lazily so the manifest check runs only for the files considered.
			eligible := func(i int) (owned, safe bool) {
				sc := files[i].Scope
				ep, ok := ownedEpoch[sc.String()]
				if !ok {
					return false, false // not ours: a pure copy
				}
				// The control cell is tiny and its writes do not gate on eviction.
				if sc.Class == control.ControlClass || store.IsOpen(sc) {
					return true, false
				}
				release, gateErr := store.BeginRequestContext(ctx, sc.String())
				if gateErr != nil {
					return true, false
				}
				defer release()
				c, cerr := store.Cell(ctx, sc)
				if cerr != nil {
					return true, false
				}
				txid, terr := c.TxID(ctx)
				if terr != nil {
					return true, false
				}
				if ok, cerr := rep.Covers(ctx, sc, ep, txid); cerr == nil && ok {
					return true, true
				}
				return true, false
			}
			var freed int64
			for _, i := range cellstore.SelectDiskEvictions(files, cfg.CellDiskMax, eligible) {
				sc := files[i].Scope
				ep, ok := ownedEpoch[sc.String()]
				if !ok {
					continue
				}
				c, release, cerr := store.AcquireResident(ctx, sc)
				if cerr != nil {
					continue
				}
				release() // Never hold a request pin while Forget waits for requests.
				if c == nil {
					continue
				}
				// Re-verify under the eviction gate: the txid may have advanced
				// (and been acked) after the eligibility check above, and the
				// bucket manifest must still cover it.
				verify := func() error {
					txid, terr := c.TxID(ctx)
					if terr != nil {
						return terr
					}
					covered, rerr := rep.Covers(ctx, sc, ep, txid)
					if rerr != nil {
						return rerr
					}
					if !covered {
						return fmt.Errorf("cellstore: bucket manifest does not cover txid %d", txid)
					}
					return nil
				}
				if err := store.ForgetWithVerify(ctx, sc, verify); err != nil {
					log.Warn("cell disk evict skipped", "scope", sc.String(), "err", err)
					continue
				}
				freed += files[i].Bytes
			}
			log.Info("cell disk gc", "total_bytes", total, "budget_bytes", cfg.CellDiskMax,
				"high_bytes", cfg.DiskHigh, "freed_bytes", freed)
		}
	}
}

// newLogBuffer builds the bounded per-worker log tail buffer (ADR-136): both
// /v1/internal/logs (ingest) and GET /v1/control/logs (query) return 503 without
// it, so it must be wired with the configured bounds.
func newLogBuffer(cfg config.Config) *logbuf.Buffer {
	return logbuf.New(cfg.LogBufferEntries, cfg.LogBufferWorkers)
}

func registerTimerScope(ctx context.Context, open func(context.Context, cell.Scope) (*timer.Store, error), srv *server.Server, sc cell.Scope) {
	if err := srv.PrepareResourceAccess(ctx, sc); err != nil {
		return
	}
	release, err := srv.BeginResourceRequest(ctx, sc)
	if err != nil {
		return
	}
	defer release()
	st, err := open(ctx, sc)
	if err != nil {
		return
	}
	// (Re)publish the wake index from the authoritative rows: a registration or
	// claim is the natural repair point for an entry lost to a crash (ADR-177).
	_ = st.SyncIndex(ctx)
	if next, err := st.NextDue(ctx); err == nil && next > 0 {
		srv.Timers.Arm(sc.String(), next)
	}
}

// repairWakeIndexPass re-syncs the bucket wake index from local timer cells over
// a bounded, rotating window, so every local cell is covered after a few passes
// without a full bucket List. It also re-arms the local runner for scopes this
// node owns. Index entries are non-authoritative (ADR-099) and this is the
// repair path (ADR-177).
func repairWakeIndexPass(ctx context.Context, log *slog.Logger, store *cellstore.Store, idx *wake.Index, om *owner.Manager, srv *server.Server, open func(context.Context, cell.Scope) (*timer.Store, error), nodeID string, batch int, cursor *int) {
	scopes, err := store.LocalScopes()
	if err != nil {
		log.Warn("wake repair: list local scopes failed", "err", err)
		return
	}
	if len(scopes) == 0 {
		*cursor = 0
		return
	}
	if batch <= 0 {
		batch = 256
	}
	if *cursor >= len(scopes) {
		*cursor = 0
	}
	examined, repaired := 0, 0
	for examined < batch && examined < len(scopes) {
		sc := scopes[(*cursor+examined)%len(scopes)]
		examined++
		shouldRegister := func() bool {
			if err := srv.PrepareResourceAccess(ctx, sc); err != nil {
				return false
			}
			release, err := srv.BeginResourceRequest(ctx, sc)
			if err != nil {
				return false
			}
			defer release()
			p, perr := store.Path(sc)
			if perr != nil {
				return false
			}
			min, hasTimers, perr := cellstore.PendingTimerMin(ctx, p)
			if perr != nil || !hasTimers {
				return false
			}
			if min <= 0 {
				if err := idx.Delete(ctx, sc.String()); err == nil {
					repaired++
				}
				return false
			}
			if err := idx.Put(ctx, sc.String(), min, "", ""); err == nil {
				repaired++
			}
			if o, _, rerr := om.Resolve(ctx, sc); rerr == nil {
				live, lerr := om.Live(ctx, o, time.Now())
				return lerr == nil && live && o.Node == nodeID
			}
			return false
		}()
		if shouldRegister {
			registerTimerScope(ctx, open, srv, sc)
		}
	}
	*cursor = (*cursor + examined) % len(scopes)
	if repaired > 0 {
		log.Info("wake index repaired", "examined", examined, "repaired", repaired)
	}
}

// runWakeRepairLoop runs one pass at startup then every WakeRepairInterval.
func runWakeRepairLoop(ctx context.Context, log *slog.Logger, store *cellstore.Store, idx *wake.Index, om *owner.Manager, srv *server.Server, open func(context.Context, cell.Scope) (*timer.Store, error), cfg config.Config) {
	if cfg.WakeRepairInterval <= 0 {
		return
	}
	cursor := 0
	pass := func() {
		repairWakeIndexPass(ctx, log, store, idx, om, srv, open, cfg.NodeID, cfg.WakeRepairBatch, &cursor)
	}
	pass()
	t := time.NewTicker(cfg.WakeRepairInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pass()
		}
	}
}

// runAuditPruneLoop hourly deletes control-plane audit records older than the
// retention window, across all registered apps.
func runAuditPruneLoop(ctx context.Context, log *slog.Logger, srv *server.Server, retention time.Duration) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-retention).UnixMilli()
			n, err := srv.RunOwnedMaintenance(ctx, control.Scope(), func() (int, error) {
				apps, err := srv.Control.Apps(ctx)
				if err != nil {
					return 0, err
				}
				total := 0
				for _, app := range apps {
					n, err := srv.Control.PruneAudit(ctx, app.Namespace, cutoff)
					if err != nil {
						return total, err
					}
					total += n
				}
				return total, nil
			})
			if err != nil {
				log.Warn("audit prune failed", "err", err)
			} else if n > 0 {
				log.Info("audit pruned", "removed", n)
			}
		}
	}
}

func runDefaultWorkflowRetentionLoop(ctx context.Context, log *slog.Logger, srv *server.Server, retention time.Duration) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			proj, err := srv.BackgroundControl().Projection(ctx)
			if err != nil {
				log.Warn("workflow retention: projection failed", "err", err)
				continue
			}
			before := time.Now().Add(-retention).UnixMilli()
			for _, wt := range proj.WorkflowTargets() {
				n, err := srv.RunOwnedMaintenance(ctx, workflow.Scope(wt.Namespace, wt.Name), func() (int, error) { return srv.Workflows.Prune(ctx, wt.Namespace, wt.Name, before) })
				if err != nil {
					log.Warn("workflow retention failed", "ns", wt.Namespace, "workflow", wt.Name, "err", err)
				} else if n > 0 {
					log.Info("workflow retention pruned", "ns", wt.Namespace, "workflow", wt.Name, "instances", n)
				}
			}
		}
	}
}

func runWorkflowRetentionLoop(ctx context.Context, log *slog.Logger, srv *server.Server) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			proj, err := srv.BackgroundControl().Projection(ctx)
			if err != nil {
				log.Warn("workflow retention: projection failed", "err", err)
				continue
			}
			now := time.Now().UnixMilli()
			for _, app := range proj.Apps {
				for _, wk := range app.Workers {
					for _, b := range wk.Version.Bindings {
						if b.Type != "workflow" {
							continue
						}
						raw, e := srv.BackgroundControl().ResourceConfig(ctx, app.Namespace, "workflow", b.ID)
						if e != nil {
							if !errors.Is(e, control.ErrNotFound) {
								log.Warn("workflow retention: policy failed", "ns", app.Namespace, "workflow", b.ID, "err", e)
							}
							continue
						}
						var p workflow.Policy
						if json.Unmarshal(raw, &p) != nil {
							continue
						}
						if p.SuccessRetentionMs <= 0 && p.ErrorRetentionMs <= 0 {
							// No retention for this binding: skip the scan entirely.
							continue
						}
						var sb, eb int64
						if p.SuccessRetentionMs > 0 {
							sb = now - p.SuccessRetentionMs
						}
						if p.ErrorRetentionMs > 0 {
							eb = now - p.ErrorRetentionMs
						}
						if n, e := srv.RunOwnedMaintenance(ctx, workflow.Scope(app.Namespace, b.ID), func() (int, error) { return srv.Workflows.PruneRetention(ctx, app.Namespace, b.ID, sb, eb) }); e != nil {
							log.Warn("workflow retention failed", "ns", app.Namespace, "workflow", b.ID, "err", e)
						} else if n > 0 {
							log.Info("workflow instances pruned", "ns", app.Namespace, "workflow", b.ID, "removed", n)
						}
					}
				}
			}
		}
	}
}

// openBucket selects the explicitly configured authority storage. An empty
// CELLHIVE_BUCKET uses the local filesystem; unknown schemes must fail closed
// rather than turning a cloud URL into a local path.
func openBucket(ctx context.Context, cfg config.Config) (bucket.Bucket, error) {
	if cfg.BucketURL == "" {
		return bucket.NewFSBucket(cfg.BucketDir)
	}
	if strings.HasPrefix(cfg.BucketURL, "s3://") {
		name := strings.TrimPrefix(cfg.BucketURL, "s3://")
		data, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
			Endpoint:  cfg.S3Endpoint,
			Region:    cfg.S3Region,
			AccessKey: cfg.S3AccessKey,
			SecretKey: cfg.S3SecretKey,
			Bucket:    name,
			PathStyle: cfg.S3PathStyle,
		})
		if err != nil {
			return nil, err
		}
		return bucket.NewS3ControlBucket(data), nil
	}
	if strings.HasPrefix(cfg.BucketURL, "oss://") || strings.HasPrefix(cfg.BucketURL, "cos://") {
		name := cfg.BucketURL[6:]
		if name == "" || strings.Contains(name, "/") {
			return nil, fmt.Errorf("invalid provider bucket name %q", name)
		}
		endpoint, access, secret := cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey
		var appender bucket.PositionAppender
		var err error
		if strings.HasPrefix(cfg.BucketURL, "oss://") {
			if cfg.OSSEndpoint == "" {
				return nil, fmt.Errorf("OSS_ENDPOINT is required")
			}
			endpoint = cfg.OSSEndpoint
			if !strings.Contains(endpoint, "://") {
				endpoint = "https://" + endpoint
			}
			access, secret = cfg.OSSAccessKey, cfg.OSSSecretKey
			appender, err = bucket.NewOSSAppender(endpoint, name, access, secret)
		} else {
			endpoint = cfg.COSEndpoint
			access, secret = cfg.COSAccessKey, cfg.COSSecretKey
			if cfg.COSEndpoint == "" {
				return nil, fmt.Errorf("COS_ENDPOINT is required")
			}
			appender, err = bucket.NewCOSAppender(endpoint, access, secret)
		}
		if err != nil {
			return nil, err
		}
		region := cfg.S3Region
		if strings.HasPrefix(cfg.BucketURL, "cos://") {
			endpoint, region, err = bucket.COSServiceEndpoint(endpoint, name)
			if err != nil {
				return nil, err
			}
		}
		data, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: endpoint, Region: region, AccessKey: access, SecretKey: secret, Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
		if err != nil {
			return nil, err
		}
		return bucket.NewProviderBucket(data, appender, bucket.AuthorityOptions{ValidateRoots: cfg.AuthorityMode == "validate" || cfg.AuthorityMode == "reclaim", ReclaimRetired: cfg.AuthorityMode == "reclaim"}), nil
	}
	scheme := cfg.BucketURL
	if i := strings.Index(scheme, "://"); i >= 0 {
		scheme = scheme[:i]
	}
	return nil, fmt.Errorf("unsupported bucket scheme %q: authority storage must implement atomic create, CAS, and conditional delete", scheme)
}

// projectionCacheTTL bounds how long a dispatch callback may reuse a routing
// projection built on this node. Ownership is still resolved per dispatch, so a
// stale entry can only select an older worker/bundle/queue/cron view. Five
// seconds matches the pre-existing cron enrichment cache and keeps the default
// one-second queue poll from rebuilding on every pass.
const projectionCacheTTL = 5 * time.Second

type projectionSource interface {
	Projection(context.Context) (control.Projection, error)
}

// projectionCache is a short-TTL, single-flight cache of the routing projection.
// The queue runner, workflow dispatcher and DO alarm dispatcher
// all resolve through it, so a batch of N callbacks performs at most one build
// per TTL instead of one per item.
type projectionCache struct {
	src projectionSource
	ttl time.Duration
	now func() time.Time

	mu        sync.Mutex
	proj      *control.Projection
	at        time.Time
	building  chan struct{} // non-nil while one rebuild is in flight
	authority string
}

// Projection returns the cached projection, rebuilding it on expiry or on the
// first call. Concurrent callers wait for the in-flight build instead of
// stampeding the control cell. Errors are never cached.
func (c *projectionCache) Projection(ctx context.Context) (control.Projection, error) {
	if err := ctx.Err(); err != nil {
		return control.Projection{}, err
	}
	if c == nil || c.src == nil {
		return control.Projection{}, errors.New("control plane unavailable")
	}
	ttl := c.ttl
	if ttl <= 0 {
		ttl = projectionCacheTTL
	}
	now := c.now
	if now == nil {
		now = time.Now
	}
	for {
		authority := ""
		if src, ok := c.src.(interface{ AuthorityKey() string }); ok {
			authority = src.AuthorityKey()
		}
		c.mu.Lock()
		cached := c.proj
		if cached != nil && now().Sub(c.at) < ttl && authority == c.authority {
			p := *cached
			c.mu.Unlock()
			return p, nil
		}
		if c.building != nil {
			wait := c.building
			c.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return control.Projection{}, ctx.Err()
			}
		}
		c.building = make(chan struct{})
		c.mu.Unlock()

		var p control.Projection
		var err error
		if src, ok := c.src.(interface {
			ProjectionWithAuthority(context.Context) (control.Projection, string, error)
		}); ok {
			p, authority, err = src.ProjectionWithAuthority(ctx)
		} else {
			p, err = c.src.Projection(ctx)
		}

		c.mu.Lock()
		if err == nil {
			c.proj = &p
			c.at = now()
			c.authority = authority
		}
		close(c.building)
		c.building = nil
		c.mu.Unlock()
		if err != nil {
			return control.Projection{}, err
		}
		return p, nil
	}
}

func (c *projectionCache) activeVersion(ctx context.Context, ns, worker string) (control.Version, bool, error) {
	p, err := c.Projection(ctx)
	if err != nil {
		return control.Version{}, false, err
	}
	v, ok := p.ActiveVersion(ns, worker)
	return v, ok, nil
}

func (c *projectionCache) workflowTarget(ctx context.Context, ns, name string) (control.WorkflowTarget, bool, error) {
	p, err := c.Projection(ctx)
	if err != nil {
		return control.WorkflowTarget{}, false, err
	}
	t, ok := p.WorkflowTargetFor(ns, name)
	return t, ok, nil
}

func (c *projectionCache) queueTargets(ctx context.Context) ([]control.QueueTarget, error) {
	p, err := c.Projection(ctx)
	if err != nil {
		return nil, err
	}
	return p.QueueTargets(), nil
}

// cronEnricher resolves a cron timer's dispatch target (worker + active bundle)
// from the routing projection, so cell-agent can hand user-runtime everything it
// needs to run the scheduled() handler. The projection is cached briefly.
type cronEnricher struct {
	next timer.Dispatcher
	ctrl projectionSource
	proj *projectionCache

	mu      sync.Mutex
	targets map[string][]control.CronTarget // ns\x00worker -> targets (one per cron)
	at      time.Time
}

func (e *cronEnricher) Dispatch(ctx context.Context, t timer.Timer) error {
	if t.Kind == timer.KindCron && t.Worker == "" {
		sc, err := cell.ParseScope(t.Scope)
		if err != nil {
			return fmt.Errorf("cron dispatch scope: %w", err)
		}
		targets, err := e.lookup(ctx, sc.Namespace, sc.ID)
		if err != nil {
			return fmt.Errorf("cron dispatch target %s: %w", t.Scope, err)
		}
		if len(targets) == 0 {
			return fmt.Errorf("cron dispatch target %s: active worker unavailable", t.Scope)
		}
		tg := targets[0]
		t.Worker = tg.Worker
		t.BundleSHA = tg.BundleSHA
		t.Version = tg.Version
		t.Cron = cronExprForSlot(targets, t)
	}
	return e.next.Dispatch(ctx, t)
}

// cronExprForSlot returns the expression that fired this slot. A worker usually
// has one cron; with several we re-evaluate the schedules against the slot so
// scheduled(event) still sees the exact expression (ADR-154).
func cronExprForSlot(targets []control.CronTarget, t timer.Timer) string {
	if expr := cron.Expression(t.Occurrence); expr != "" {
		return expr
	}
	if len(targets) == 1 {
		return targets[0].Cron
	}
	minute, err := strconv.ParseInt(t.Occurrence, 10, 64)
	if err != nil {
		return ""
	}
	slot := time.Unix(minute*60, 0).UTC()
	for _, tg := range targets {
		if sch, err := cron.Parse(tg.Cron); err == nil && sch.Matches(slot) {
			return tg.Cron
		}
	}
	return ""
}

func (e *cronEnricher) lookup(ctx context.Context, ns, worker string) ([]control.CronTarget, error) {
	if e.proj != nil {
		p, err := e.proj.Projection(ctx)
		if err != nil {
			return nil, err
		}
		return cronDispatchTargets(p)[ns+"\x00"+worker], nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.targets == nil || time.Since(e.at) > 5*time.Second {
		var proj control.Projection
		var err error
		switch {
		case e.proj != nil:
			proj, err = e.proj.Projection(ctx)
		case e.ctrl != nil:
			proj, err = e.ctrl.Projection(ctx)
		default:
			return nil, errors.New("control plane unavailable")
		}
		if err != nil {
			return nil, err
		}
		e.targets = cronDispatchTargets(proj)
		e.at = time.Now()
	}
	return e.targets[ns+"\x00"+worker], nil
}

// An already materialized slot remains deliverable after a redeploy removes
// its schedule. The occurrence retains the old expression, while the active
// worker supplies the current bundle even when it has no new cron triggers.
func cronDispatchTargets(proj control.Projection) map[string][]control.CronTarget {
	m := make(map[string][]control.CronTarget)
	for _, app := range proj.Apps {
		for _, w := range app.Workers {
			if w.Version.BundleSHA == "" {
				continue
			}
			key := app.Namespace + "\x00" + w.Worker
			for _, expr := range w.Version.Crons {
				m[key] = append(m[key], control.CronTarget{
					Namespace: app.Namespace, Worker: w.Worker, Cron: expr,
					BundleSHA: w.Version.BundleSHA, Version: w.Version.Number,
				})
			}
			if len(m[key]) == 0 {
				m[key] = []control.CronTarget{{
					Namespace: app.Namespace, Worker: w.Worker,
					BundleSHA: w.Version.BundleSHA, Version: w.Version.Number,
				}}
			}
		}
	}
	return m
}

// pagedSource adapts replica.PageFetcher to pagedvfs.Source: SQLite faults pages
// synchronously, so ReadPage does a blocking ranged read of the pinned cut.
type pagedSource struct {
	pf *replica.PageFetcher
}

func (p *pagedSource) PageSize() int { return int(p.pf.PageSize()) }
func (p *pagedSource) Commit() int   { return int(p.pf.Commit()) }
func (p *pagedSource) TxID() uint64  { return p.pf.TxID() }

func (p *pagedSource) Close() error { return p.pf.Close() }

func (p *pagedSource) ReadPage(pgno uint32) ([]byte, error) {
	return p.pf.Page(context.Background(), pgno)
}

// ReadRun implements pagedvfs.RunSource: one ranged read per page window.
func (p *pagedSource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	return p.pf.ReadRun(context.Background(), pgno, maxPages)
}
