// Command cell-agent is the CellHive state/control node (P0 skeleton).
package main

import (
	"context"
	"fmt"
	"log/slog"
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
	"cellhive/internal/artifacts"
	"cellhive/internal/auth"
	"cellhive/internal/autoscaler"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/cron"
	"cellhive/internal/d1"
	"cellhive/internal/dispatch"
	"cellhive/internal/drain"
	"cellhive/internal/lease"
	"cellhive/internal/logbuf"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/objgc"
	"cellhive/internal/owner"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/peer"
	"cellhive/internal/purge"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/rebalance"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
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
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(2)
	}
	for _, w := range config.LegacyEnvWarnings() {
		log.Warn(w)
	}

	b, err := openBucket(context.Background(), cfg)
	if err != nil {
		log.Error("open bucket", "err", err)
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
		log.Info("otel export enabled", "endpoint", cfg.OTLPEndpoint, "ratio", cfg.TraceSampleRatio)
	}

	// The bucket is the authority for owner election and epoch fencing. Serving
	// after a failed conformance probe could admit two owners, so fail closed.
	if err := bucket.Diagnose(ctx, b); err != nil {
		log.Error("storage diagnose failed", "err", err)
		os.Exit(1)
	}
	log.Info("storage diagnose ok")

	lm := lease.NewManager(b, cfg.NodeID, cfg.SessionID, cfg.Advertise, cfg.PeerURL, cfg.LeaseTTL)
	lm.AZ = cfg.PlacementAZ
	om := &owner.Manager{
		B:         b,
		NodeID:    cfg.NodeID,
		Session:   cfg.SessionID,
		Advertise: cfg.Advertise,
		Role:      cell.RoleCellAgent,
		OwnerTTL:  cfg.LeaseTTL,
	}

	placementWeight := cfg.PlacementWeight
	if placementWeight <= 0 {
		placementWeight = runtime.NumCPU()
	}
	publishLoad := func() {
		owned := len(om.OwnedScopes())
		if err := lm.Publish(ctx, lease.Load{OwnedCells: owned, ResidentCells: owned, Weight: placementWeight, SampledMs: time.Now().UnixMilli()}); err != nil {
			log.Warn("lease publish failed", "err", err)
		}
	}
	publishLoad()

	// Renew node lease at TTL/3.
	go func() {
		t := time.NewTicker(cfg.LeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				publishLoad()
			}
		}
	}()

	// Renew owned cell leases at TTL/3. Without this an owner record expires
	// after LeaseTTL, so backend-A capture stops and owner-gated writes fail
	// (ADR-092). Renewal is idempotent per (scope, epoch); a lost lease is
	// re-established through the normal claim path, not here.
	go func() {
		t := time.NewTicker(cfg.LeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := time.Now()
				for _, owned := range om.OwnedScopes() {
					if _, err := om.Renew(ctx, owned.Scope, owned.Epoch, now); err != nil {
						log.Warn("owner renew failed", "scope", owned.Scope.String(), "err", err)
					}
				}
			}
		}
	}()

	spool, err := peer.NewSpool(cfg.PeerSpoolDir)
	if err != nil {
		log.Error("open peer spool", "err", err)
		os.Exit(1)
	}
	peerTransport := peer.NewHTTPTransport(cfg.TokenPeer, nil)
	peerMgr := peer.NewManager(peerTransport)
	nodeLog := nodelog.New(b, cfg.NodeID)

	rep := replica.New(b)
	upCtx, upCancel := context.WithCancel(ctx)
	shards := cfg.UploadShards
	if shards <= 0 {
		shards = runtime.NumCPU()
		if shards > 8 {
			shards = 8
		}
	}
	uploader := upload.NewSharded(rep, log, 64, 1<<20, 20*time.Millisecond, shards)
	// Durable queue for the asynchronous bucket copy: a failed upload (or a
	// restart) defers the segment instead of dropping it (ADR-143).
	if sp, serr := upload.OpenSpool(filepath.Join(cfg.DataDir, "upload-spool"), 200000); serr != nil {
		log.Warn("upload spool disabled; async bucket uploads are best-effort", "err", serr)
	} else {
		uploader.SetSpool(sp)
	}
	uploader.Start(upCtx)
	// Follower-side group commit: coalesce replicated appends into one spool
	// fsync per batch (short window; the owner's ack waits on it).
	peerUploader := upload.New(spool, log, 64, 1<<20, 2*time.Millisecond)
	peerUploader.Start(upCtx)
	// Fleet log group commit: coalesce concurrent commits into one framed batch
	// per follower set (1ms window, up to 256 segments), mirroring celld's
	// CELLD_LOG_GROUP_COMMIT_MS + CELLD_LOG_PIPELINE=4. StreamTransport uses
	// four scope-hashed HTTP 101 binary lanes per follower, with four batches
	// allowed in flight per lane.
	var shipTransport peer.Transport = peer.NewStreamTransport(cfg.TokenPeer, 4, 4)
	if cfg.PeerLatency > 0 {
		log.Info("peer latency injection enabled", "one_way", cfg.PeerLatency)
		shipTransport = peer.NewLatencyTransport(shipTransport, cfg.PeerLatency)
	}
	shipper := peer.NewShipBatcher(shipTransport, 256, 4<<20, time.Millisecond, 4, cfg.PeerPipeline, cfg.PeerHedgeMS, cfg.PeerHedgeMaxMS)
	shipper.Start(upCtx)

	// Background L0->L1 compaction for owned scopes (cold path; never on the
	// write-ack path). Disabled when the interval is zero.
	if cfg.CompactionInterval > 0 {
		go runCompactionLoop(ctx, log, om, rep, cfg)
	}

	store, err := cellstore.New(cfg.DataDir)
	if err != nil {
		log.Error("open cell store", "err", err)
		os.Exit(1)
	}
	// Paged cold restore (ADR-160): when the cell's compacted restore chain is
	// large enough, open the main database over a sparse local file and fault
	// pages from the replica on first use instead of downloading the whole
	// chain. Small or uncompacted cells fall through to the full hydrate below.
	if cfg.PagedRestore {
		store.Paged = func(pctx context.Context, sc cell.Scope, dest string) (pagedvfs.Source, bool, error) {
			ep, ok, perr := rep.LatestEpoch(pctx, sc, 0)
			if perr != nil || !ok {
				return nil, false, nil
			}
			pf, perr := rep.NewPageFetcher(pctx, sc, ep)
			if perr != nil {
				// No compacted L1 page index yet: clone the chain whole.
				return nil, false, nil
			}
			imageBytes := int64(pf.PageSize()) * int64(pf.Commit())
			if imageBytes < cfg.PagedMinBytes {
				return nil, false, nil
			}
			log.Info("paged restore", "scope", sc.String(), "epoch", ep,
				"bytes", imageBytes, "pages", pf.Commit())
			return pagedSource{pf: pf}, true, nil
		}
		store.PagedHydrateMBPS = cfg.PagedHydrateMBPS
		if cfg.PagedWindowPages > 0 {
			pagedvfs.WindowPages = cfg.PagedWindowPages
		}
		if cfg.PagedPrefetchWorkers > 0 {
			pagedvfs.PrefetchWorkers = cfg.PagedPrefetchWorkers
		}
	}

	// Cold restore (ADR-092): a fresh owner hydrates a cell from its bucket
	// replica before serving, so acked backend-A writes survive crash/takeover.
	store.Hydrate = func(hctx context.Context, sc cell.Scope, dest string) error {
		ep, ok, herr := rep.LatestEpoch(hctx, sc, 0)
		if herr != nil || !ok {
			return herr
		}
		segs, herr := rep.Restore(hctx, sc, ep)
		if herr != nil {
			return herr
		}
		if len(segs) == 0 {
			return nil
		}
		raw := make([][]byte, 0, len(segs))
		for _, sg := range segs {
			raw = append(raw, sg.Raw)
		}
		_, herr = restore.ApplyFile(dest, raw)
		return herr
	}

	// Unified timers: producers upsert into the owning cell; the local runner
	// dispatches due timers, and the single fleet waker covers dead owners.
	timerReg := timer.NewRegistry()
	// Wake index: the bucket-side list of scopes with a pending timer, so the
	// fleet waker finds due work without listing every cell (ADR-099).
	wakeIdx := wake.New(b)
	openTimer := func(tctx context.Context, sc cell.Scope) (*timer.Store, error) {
		c, err := store.Cell(tctx, sc)
		if err != nil {
			return nil, err
		}
		st, err := timer.NewStore(tctx, c)
		if err != nil {
			return nil, err
		}
		st.Index = wakeIdx
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
	// pass through to user-runtime (control plane is wired just below via ctrlRef).
	ltx.CompressionEnabled = cfg.LTXCompression
	workflowStore := workflow.New(store)
	const workflowLeaseMs = int64(60_000)
	var ctrlRef *control.Store
	wfDisp := &dispatch.WorkflowDispatcher{
		URL: cfg.DispatchURL, Token: cfg.TokenDispatch, Log: log, Next: dispatcher,
		Params: func(cctx context.Context, ns, name, id string) ([]byte, error) {
			instance, err := workflowStore.Get(cctx, ns, name, id)
			if err != nil {
				return nil, err
			}
			return instance.Params, nil
		},
		Claim: func(cctx context.Context, ns, name, id string) (string, uint64, bool, error) {
			tok, gen, _, ok, err := workflowStore.ClaimRun(cctx, ns, name, id, workflowLeaseMs)
			return tok, gen, ok, err
		},
		Target: func(tctx context.Context, ns, name string) (control.WorkflowTarget, bool) {
			if ctrlRef == nil {
				return control.WorkflowTarget{}, false
			}
			proj, err := ctrlRef.Projection(tctx)
			if err != nil {
				return control.WorkflowTarget{}, false
			}
			return proj.WorkflowTargetFor(ns, name)
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
	if cfg.TimerInterval > 0 {
		(&timer.Runner{
			Registry: timerReg, Open: openTimerScope, Dispatcher: dispatcher,
			Interval: cfg.TimerInterval, Batch: cfg.TimerBatch, FiredTTL: cfg.TimerFiredTTL,
			Log: log,
		}).Start(ctx)
	}
	// Leader-only automatic recovery of dead nodes (ADR-066): the waker, on the
	// same pass as timer dispatch, collects acked-but-unuploaded segments from
	// dead nodes' followers via the node-log.
	recoveryRunner := &recovery.Runner{
		Nodelog:  nodeLog,
		Lease:    lm,
		Recovery: recovery.New(rep, peerTransport),
		SelfNode: cfg.NodeID,
		Log:      log,
	}
	if cfg.WakerInterval > 0 {
		(&waker.Waker{
			Election:   waker.NewElection(b, cfg.NodeID, cfg.SessionID, cfg.WakerTTL),
			Batch:      cfg.WakerBatch,
			FiredTTL:   cfg.WakerFiredTTL,
			BackoffMax: cfg.WakerBackoffMax,
			// DeadScopes reads the bucket wake index (scopes with a pending
			// timer) and keeps only those whose owner is gone, so the waker covers
			// timers/alarms after an owner dies without listing every cell.
			DeadScopes: func(wctx context.Context) ([]string, error) {
				entries, err := wakeIdx.Due(wctx, time.Now().UnixMilli())
				if err != nil {
					return nil, err
				}
				now := time.Now()
				out := make([]string, 0, len(entries))
				for _, e := range entries {
					sc, perr := cell.ParseScope(e.Scope)
					if perr != nil {
						continue
					}
					o, _, rerr := om.Resolve(wctx, sc)
					if rerr != nil || o.Expired(now) {
						out = append(out, e.Scope)
					}
				}
				return out, nil
			},
			Open: openTimerScope, Dispatcher: dispatcher, Interval: cfg.WakerInterval, Log: log,
			RecoverNodes: recoveryRunner.Pass,
		}).Start(ctx)
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
	ctrlRef = ctrl
	enricher.ctrl = ctrl

	// Object GC (ADR-110/111): delete bundles and asset versions no longer
	// referenced by any worker version, after a grace period. Cold path; disabled
	// by default (CELLHIVE_BUNDLE_GC_INTERVAL).
	if cfg.BundleGCInterval > 0 {
		go runGCLoop(ctx, log, b, ctrl, cfg)
	}

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

	// Workflow retention: periodically prune terminal instances of every
	// registered workflow definition (cold path).
	if cfg.WorkflowRetention > 0 {
		go func() {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					proj, err := ctrl.Projection(ctx)
					if err != nil {
						continue
					}
					before := time.Now().Add(-cfg.WorkflowRetention).UnixMilli()
					for _, wt := range proj.WorkflowTargets() {
						if n, err := workflowStore.Prune(ctx, wt.Namespace, wt.Name, before); err == nil && n > 0 {
							log.Info("workflow retention pruned", "ns", wt.Namespace, "workflow", wt.Name, "instances", n)
						}
					}
				}
			}
		}()
	}
	alarmDisp.Bundle = func(bctx context.Context, ns, worker string) (string, int, bool) {
		proj, err := ctrl.Projection(bctx)
		if err != nil {
			return "", 0, false
		}
		v, ok := proj.ActiveVersion(ns, worker)
		return v.BundleSHA, v.Number, ok
	}

	// Cron scheduler: materialize due slots as KindCron timers (ADR-076). The
	// timer runner then dispatches them (enriched with worker/bundle, ADR-070).
	if cfg.CronInterval > 0 {
		(&cron.Scheduler{
			Interval: cfg.CronInterval,
			Log:      log,
			Open:     openTimer,
			Register: func(scope string) { timerReg.Add(scope) },
			Targets: func(cctx context.Context) ([]cron.Target, error) {
				proj, err := ctrl.Projection(cctx)
				if err != nil {
					return nil, err
				}
				ct := proj.CronTargets()
				out := make([]cron.Target, 0, len(ct))
				for _, t := range ct {
					out = append(out, cron.Target{Namespace: t.Namespace, Worker: t.Worker, Cron: t.Cron})
				}
				return out, nil
			},
		}).Start(ctx)
	}

	if cfg.AuditRetention > 0 {
		go runAuditPruneLoop(ctx, log, ctrl, cfg.AuditRetention)

	}

	srv := server.New(server.Deps{
		Cfg:              cfg,
		Bucket:           b,
		Lease:            lm,
		Owner:            om,
		Replica:          rep,
		Spool:            spool,
		PeerMgr:          peerMgr,
		PeerShipper:      shipper,
		NodeLog:          nodeLog,
		Uploader:         uploader,
		PeerUploader:     peerUploader,
		Store:            store,
		Log:              log,
		Timers:           timerReg,
		OpenTimer:        openTimer,
		Control:          ctrl,
		AdminAuth:        buildAdminAuth(cfg),
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
			Store:               store,
			Committer:           srv.CaptureCommitter(),
			AutoSnapshot:        true,
			AutoCheckpointBytes: cfg.CellWALCheckpointBytes,
			GroupCommitWait:     cfg.CaptureGroupCommitWait,
			PipelineThreshold:   cfg.CapturePipelineThreshold,
			Log:                 log,
			Owner: func(cctx context.Context, sc cell.Scope) (bool, uint64, error) {
				o, _, err := om.Resolve(cctx, sc)
				if err != nil {
					return false, 0, err
				}
				return o.Node == cfg.NodeID && !o.Expired(time.Now()), o.Epoch, nil
			},
			// Ownership moved: forget the local cell so later reads re-hydrate
			// from the bucket instead of serving a stale local copy (ADR-115).
			OnLostOwner: func(sc cell.Scope) {
				if !cfg.ForgetOnLoss {
					return // keep the file; the disk-budget janitor reclaims LRU files
				}
				if err := store.Forget(context.Background(), sc); err != nil {
					log.Warn("forget cell after ownership loss failed", "scope", sc.String(), "err", err)
				}
			},
		}
		srv.Capture = captureMgr
		// Queue consumer dispatch: poll registered queues and deliver claimed batches
		// to the consuming worker via the dispatch endpoint (docs/bindings.md). Only
		// the node that owns a queue's cell consumes it (OwnerGate claims unowned or
		// expired cells), and every store mutation is captured with a durability
		// proof, so a queue has exactly one consumer and RPO=0 (ADR-119).
		if cfg.QueueInterval > 0 {
			if qd := queue.NewHTTP(cfg.DispatchURL, cfg.TokenDispatch); qd != nil {
				(&queue.Runner{
					Store:        queue.New(store),
					Dispatch:     qd,
					Interval:     cfg.QueueInterval,
					Batch:        cfg.QueueBatch,
					LeaseMs:      cfg.QueueLease.Milliseconds(),
					RetryDelayMs: cfg.QueueRetryDelay.Milliseconds(),
					Log:          log,
					Filter:       queue.OwnerGate(om),
					Commit: func(cctx context.Context, ns, name string, fn func() error) error {
						return srv.CaptureWrite(cctx, queue.Scope(ns, name), fn)
					},
					DeadLetterSend: srv.EnqueueToQueueOwner,
					Queues: func(qctx context.Context) ([]queue.Ref, error) {
						queues, err := ctrl.ResourcesByKind(qctx, "queue")
						if err != nil {
							return nil, err
						}
						proj, err := ctrl.Projection(qctx)
						if err != nil {
							return nil, err
						}
						// Map a queue to the worker that consumes it (and its active
						// bundle) so user-runtime can load the handler directly.
						consumers := map[string]queue.Ref{}
						for _, t := range proj.QueueTargets() {
							consumers[t.Namespace+"\x00"+t.Queue] = queue.Ref{
								Namespace: t.Namespace, Name: t.Queue,
								Worker: t.Worker, BundleSHA: t.BundleSHA, Version: t.Version,
								MaxRetries:      t.Consumer.MaxRetries,
								DeadLetterQueue: t.Consumer.DeadLetterQueue,
								MaxBatchSize:    t.Consumer.MaxBatchSize,
								MaxConcurrency:  t.Consumer.MaxConcurrency,
							}
						}
						out := make([]queue.Ref, 0, len(queues))
						for _, r := range queues {
							if ref, ok := consumers[r.Namespace+"\x00"+r.Name]; ok {
								out = append(out, ref)
							}
						}
						return out, nil
					},
				}).Start(ctx)
			}
		}
		go func() { <-ctx.Done(); captureMgr.Stop() }()
	}

	// Cell-handle eviction (ADR-096): bound open fds/memory by closing idle or
	// excess cached cells. Eviction stops the cell's capture first; ownership is
	// untouched, so the next request reopens the cell locally.
	// Purge jobs (ADR-131): soft-deleted apps/workers are hard-deleted here. The
	// hook drops the data side on every node that runs this loop — local cell
	// handles plus the bucket prefixes (ADR-142); it is resumable, so a large
	// namespace is cleaned in bounded passes.
	purger := &purge.Purger{Bucket: b, Cells: store}
	go ctrl.RunPurgeLoop(ctx, log, 5*time.Second, purger.Run)
	if cfg.MaxResidentCells > 0 {
		store.MaxOpenCells = cfg.MaxResidentCells
	} else {
		store.MaxOpenCells = cfg.MaxOpenCells
	}
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
		go runCellDiskLoop(ctx, log, store, om, rep, lm, cfg, &diskPressure)
	}
	if captureMgr != nil {
		store.Drop = captureMgr.Drop
	}
	store.Start(ctx)
	if cfg.WakeRepairInterval > 0 {
		go runWakeRepairLoop(ctx, log, store, wakeIdx, om, srv, openTimer, cfg)
	}
	if cfg.RebalanceInterval > 0 {
		go runRebalanceLoop(ctx, log, cfg, om, store, lm, timerReg, placementWeight)
	}

	// Re-register timers this node owns from the wake index: after a restart the
	// in-memory registry is empty, but the bucket index still lists the scopes
	// with pending timers.
	go func() {
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
			if rerr != nil || o.Node != cfg.NodeID || o.Expired(now) {
				continue
			}
			registerTimerScope(ctx, openTimer, srv, sc)
		}
	}()
	srv.SetReady(true)

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
		performHandoff(log, cfg, nodeLog, om, drainMgr)
		upCancel()
		shipper.Stop()
		peerUploader.Stop()
		uploader.Stop()
		_ = store.Close()
		os.Exit(1)
	}
	performHandoff(log, cfg, nodeLog, om, drainMgr)
	upCancel()
	shipper.Stop()
	peerUploader.Stop()
	uploader.Stop()
	if err := store.Close(); err != nil {
		log.Warn("close cell store", "err", err)
	}
	log.Info("cell-agent stopped")
}

// runCompactionLoop periodically folds each owned scope's L0 chain into an L1
// snapshot so a takeover restore reads a bounded set of objects. It re-checks
// ownership (node + epoch) immediately before writing, so a node that lost the
// lease never publishes a manifest.
func runCompactionLoop(ctx context.Context, log *slog.Logger, om *owner.Manager, rep *replica.Manager, cfg config.Config) {
	t := time.NewTicker(cfg.CompactionInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, owned := range om.OwnedScopes() {
				sc, epoch := owned.Scope, owned.Epoch
				verify := func(vctx context.Context) error {
					o, _, err := om.Resolve(vctx, sc)
					if err != nil {
						return err
					}
					if o.Node != cfg.NodeID || o.Epoch != epoch {
						return fmt.Errorf("ownership changed: node=%s epoch=%d", o.Node, o.Epoch)
					}
					return nil
				}
				res, err := compaction.Compact(ctx, rep, sc, epoch, compaction.Options{
					MinSegments: cfg.CompactionMinSegments,
					MinBytes:    cfg.CompactionMinBytes,
					Verify:      verify,
					GC:          true,
				})
				if err != nil {
					log.Warn("compaction failed", "scope", sc.String(), "err", err)
				} else if res.Compacted {
					log.Info("compacted", "scope", sc.String(), "inputs", res.Inputs,
						"parts", res.Parts, "min_txid", res.MinTxID, "max_txid", res.MaxTxID,
						"deleted_l1", res.DeletedL1, "deleted_l0", res.DeletedL0,
						"stored_bytes", res.InputBytes)
				}
			}
		}
	}
}

// performHandoff runs the shutdown-side of graceful handoff: acquire the bucket
// drain token (serializing concurrent shutdowns), seal this node's session so the
// next owner takes the recovery fast path, release every owned cell so a peer can
// claim it immediately, then release the token.
func performHandoff(log *slog.Logger, cfg config.Config, nl *nodelog.Manager, om *owner.Manager, dm *drain.Manager) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DrainWait)
	defer cancel()
	if err := dm.Wait(ctx); err != nil {
		log.Warn("drain token not acquired; proceeding best-effort", "err", err)
	}
	if rec, _, err := nl.Get(ctx, cfg.NodeID, cfg.SessionID); err == nil && rec.Status != nodelog.StatusSealed {
		if _, err := nl.Seal(ctx, cfg.NodeID, cfg.SessionID); err != nil {
			log.Warn("node-log seal failed", "err", err)
		}
	}
	released := 0
	for _, owned := range om.OwnedScopes() {
		if err := om.Release(ctx, owned.Scope, owned.Epoch); err != nil {
			log.Warn("owner release failed", "scope", owned.Scope.String(), "err", err)
			continue
		}
		released++
	}
	if err := dm.Release(ctx); err != nil {
		log.Warn("drain token release failed", "err", err)
	}
	log.Info("handoff complete", "released", released)
}

// buildAdminAuth picks the admin authenticator: OIDC/JWT bearer when a JWKS URL
// is configured (accepting the static ops token too), else the static token.
// runRebalanceLoop releases idle owned cells so the fleet moves toward a
// weight-proportional ownership split (ADR-101). It only schedules or releases
// hibernated (locally closed) cells; a peer claims them on demand.
func runRebalanceLoop(ctx context.Context, log *slog.Logger, cfg config.Config, om *owner.Manager, store *cellstore.Store, lm *lease.Manager, timers *timer.Registry, weight int) {
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
				if err := om.Release(ctx, sc, m.Epoch); err != nil {
					log.Warn("rebalance release failed", "scope", m.Scope, "err", err)
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
					if err := om.Release(ctx, o.Scope, o.Epoch); err != nil {
						log.Warn("disk pressure release failed", "scope", o.Scope.String(), "err", err)
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
				c, cerr := store.Cell(ctx, sc)
				if cerr != nil {
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

// runGCLoop periodically garbage-collects unreferenced worker bundles and asset
// versions (ADR-110/ADR-111). It is a cold path (bucket List) and disabled
// unless CELLHIVE_BUNDLE_GC_INTERVAL > 0.
func runGCLoop(ctx context.Context, log *slog.Logger, b bucket.Bucket, ctrl *control.Store, cfg config.Config) {
	st := artifacts.New(b)
	passes := []struct {
		kind  string
		items objgc.Items
	}{
		{"bundle", artifacts.BundleItems{Store: st}},
		{"asset", artifacts.AssetItems{Store: st}},
	}
	t := time.NewTicker(cfg.BundleGCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, p := range passes {
				g := &objgc.GC{Items: p.items, Refs: control.GCRefs{S: ctrl, Kind: p.kind}, Grace: cfg.BundleGCGrace, Log: log}
				res, err := g.Pass(ctx)
				if err != nil {
					log.Warn("gc pass failed", "kind", p.kind, "err", err)
					continue
				}
				if res.Marked > 0 || res.Deleted > 0 {
					log.Info("gc pass", "kind", p.kind, "total", res.Total, "referenced", res.Referenced,
						"marked", res.Marked, "deleted", res.Deleted)
				}
			}
		}
	}
}

func registerTimerScope(ctx context.Context, open func(context.Context, cell.Scope) (*timer.Store, error), srv *server.Server, sc cell.Scope) {
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
		p, perr := store.Path(sc)
		if perr != nil {
			continue
		}
		min, hasTimers, perr := cellstore.PendingTimerMin(ctx, p)
		if perr != nil || !hasTimers {
			continue
		}
		if min <= 0 {
			if err := idx.Delete(ctx, sc.String()); err == nil {
				repaired++
			}
			continue
		}
		if err := idx.Put(ctx, sc.String(), min, "", ""); err == nil {
			repaired++
		}
		if o, _, rerr := om.Resolve(ctx, sc); rerr == nil && o.Node == nodeID && !o.Expired(time.Now()) {
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

func buildAdminAuth(cfg config.Config) auth.Authenticator {
	static := auth.StaticToken{Token: cfg.AdminToken}
	if cfg.OIDCJWKSURL == "" {
		return static
	}
	jwks := &auth.JWKS{URL: cfg.OIDCJWKSURL, TTL: 10 * time.Minute}
	return auth.Any{
		&auth.JWTBearer{Issuer: cfg.OIDCIssuer, Audience: cfg.OIDCAudience, Keys: jwks},
		static,
	}
}

// runAuditPruneLoop hourly deletes control-plane audit records older than the
// retention window, across all registered apps.
func runAuditPruneLoop(ctx context.Context, log *slog.Logger, ctrl *control.Store, retention time.Duration) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-retention).UnixMilli()
			apps, err := ctrl.Apps(ctx)
			if err != nil {
				log.Warn("audit prune: list apps failed", "err", err)
				continue
			}
			for _, app := range apps {
				if n, err := ctrl.PruneAudit(ctx, app.Namespace, cutoff); err != nil {
					log.Warn("audit prune failed", "ns", app.Namespace, "err", err)
				} else if n > 0 {
					log.Info("audit pruned", "ns", app.Namespace, "removed", n)
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
		return bucket.NewS3Bucket(ctx, bucket.S3Options{
			Endpoint:  cfg.S3Endpoint,
			Region:    cfg.S3Region,
			AccessKey: cfg.S3AccessKey,
			SecretKey: cfg.S3SecretKey,
			Bucket:    name,
			PathStyle: cfg.S3PathStyle,
		})
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
		return bucket.NewProviderBucket(data, appender), nil
	}
	scheme := cfg.BucketURL
	if i := strings.Index(scheme, "://"); i >= 0 {
		scheme = scheme[:i]
	}
	return nil, fmt.Errorf("unsupported bucket scheme %q: authority storage must implement atomic create, CAS, and conditional delete", scheme)
}

// cronEnricher resolves a cron timer's dispatch target (worker + active bundle)
// from the routing projection, so cell-agent can hand user-runtime everything it
// needs to run the scheduled() handler. The projection is cached briefly.
type cronEnricher struct {
	next timer.Dispatcher
	ctrl *control.Store

	mu      sync.Mutex
	targets map[string][]control.CronTarget // ns\x00worker -> targets (one per cron)
	at      time.Time
}

func (e *cronEnricher) Dispatch(ctx context.Context, t timer.Timer) error {
	if t.Kind == timer.KindCron && t.Worker == "" && e.ctrl != nil {
		if sc, err := cell.ParseScope(t.Scope); err == nil {
			if targets, ok := e.lookup(ctx, sc.Namespace, sc.ID); ok && len(targets) > 0 {
				tg := targets[0]
				t.Worker = tg.Worker
				t.BundleSHA = tg.BundleSHA
				t.Version = tg.Version
				t.Cron = cronExprForSlot(targets, t)
			}
		}
	}
	return e.next.Dispatch(ctx, t)
}

// cronExprForSlot returns the expression that fired this slot. A worker usually
// has one cron; with several we re-evaluate the schedules against the slot so
// scheduled(event) still sees the exact expression (ADR-154).
func cronExprForSlot(targets []control.CronTarget, t timer.Timer) string {
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

func (e *cronEnricher) lookup(ctx context.Context, ns, worker string) ([]control.CronTarget, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.targets == nil || time.Since(e.at) > 5*time.Second {
		proj, err := e.ctrl.Projection(ctx)
		if err != nil {
			return nil, false
		}
		m := map[string][]control.CronTarget{}
		for _, tg := range proj.CronTargets() {
			k := tg.Namespace + "\x00" + tg.Worker
			m[k] = append(m[k], tg)
		}
		e.targets = m
		e.at = time.Now()
	}
	tgs, ok := e.targets[ns+"\x00"+worker]
	return tgs, ok
}

// pagedSource adapts replica.PageFetcher to pagedvfs.Source: SQLite faults pages
// synchronously, so ReadPage does a blocking ranged read of the pinned cut.
type pagedSource struct {
	pf *replica.PageFetcher
}

func (p pagedSource) PageSize() int { return int(p.pf.PageSize()) }
func (p pagedSource) Commit() int   { return int(p.pf.Commit()) }

func (p pagedSource) ReadPage(pgno uint32) ([]byte, error) {
	return p.pf.Page(context.Background(), pgno)
}

// ReadRun implements pagedvfs.RunSource: one ranged read per page window.
func (p pagedSource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	return p.pf.ReadRun(context.Background(), pgno, maxPages)
}
