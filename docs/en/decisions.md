# Decision records (English index)

CellHive records every design decision as an ADR. **The full ADR text is written in Chinese**: [`../decisions.md`](../decisions.md) is the authoritative record (ADR-001 ~ 179). This page is an English index of the titles and one-line summaries; read the Chinese record for the full rationale, evidence and cost of each decision.

> Generated from `docs/decisions.md`. If the two disagree, the Chinese record wins.

| ADR | Title | Summary |
|---|---|---|
| ADR-001 | Use Stock workerd for the Compute Layer; Do Not Modify workerd | The compute layer relies on unmodified upstream workerd to reduce maintenance and preserve compatibility. |
| ADR-002 | State Model: Everything Is a Cell; SQLite + Object Storage Are Authoritative | All state is modeled as cells, with SQLite and object storage serving as sources of truth. |
| ADR-003 | Owner Resolution = Go Library + Local Endpoint, Not a Standalone Service | Ownership lookup is embedded as a Go library with a local endpoint instead of a separate service. |
| ADR-004 | Build an In-House Cell Service; Do Not Reuse or Modify External Implementations | The cell service is developed internally rather than adapted from external systems. |
| ADR-005 | Standardize on Go; Invoke External esbuild Binary for Packaging, No Node | Go is the primary language, while bundling uses an external esbuild binary without Node.js. |
| ADR-006 | Durable Objects = Native workerd Facet + Per-Object Lease + WAL→cell-agent + Output Gate | Durable Objects use native workerd facets with leases, WAL forwarding, and gated output. |
| ADR-007 | Accept Durable Objects with Equivalent Results but Weaker Mechanisms | Durable Object semantics prioritize result equivalence while accepting weaker underlying mechanisms. |
| ADR-008 | FUSE Is Not Default; Default Is WAL Capture → cell-agent | The default persistence path captures WAL and sends it to cell-agent instead of using FUSE. |
| ADR-009 | Timers: Per-Cell State + Local Due Queue + Single Waker; No Separate Scheduler | Timers are managed per cell with local due tracking and one waker, avoiding a standalone scheduler. |
| ADR-010 | No d1-runtime or kv-runtime; Only do-runtime Needs Custom PID1 Supervisor | Dedicated D1 and KV runtimes are avoided; only Durable Objects require a custom PID1 supervisor. |
| ADR-011 | Deployment: Same Cluster; At Least Two cell-agents; S3 Outside Ack Path | Deploy within one cluster with redundant cell-agents while keeping S3 off the acknowledgment path. |
| ADR-012 | Merge Control Plane into cell-agent with Separate Listeners and Authorization | Control-plane functions live in cell-agent but use distinct listeners and authorization. |
| ADR-013 | Persistence External API: Internal REST First, Typed Public REST Later | Persistence APIs start as internal REST and later expose typed public REST endpoints. |
| ADR-014 | Wrangler Compatibility Strategy | Define how the platform remains compatible with Wrangler workflows and expectations. |
| ADR-015 | Do Not Introduce a Second JS Engine; SSR Depends on the workerd Ecosystem | Server-side rendering relies on workerd’s ecosystem without adding another JavaScript engine. |
| ADR-016 | Replication Based on LiteFS + superfly/ltx; License Compliance | Replication follows LiteFS and superfly/ltx patterns while ensuring license compliance. |
| ADR-017 | No Independent Gateway; Entry = Traefik + user-runtime | Traffic enters through Traefik and user-runtime instead of a separate gateway service. |
| ADR-018 | Fixed cell-agent Cluster + Distributed Elastic do-runtime, Cross-Node Capable | cell-agents form a stable fleet while do-runtime scales elastically across nodes. |
| ADR-019 | cell-agent = State Fleet, Does Not Execute User Code | cell-agent manages persistent state only and never runs tenant user code. |
| ADR-020 | Data Placement | Define where data resides across storage, ownership, and runtime components. |
| ADR-021 | user-runtime Dual Ports = Security Boundary | Separate user-runtime ports enforce security boundaries between internal and external traffic. |
| ADR-022 | Role-Based Object Storage | Object storage access is divided by roles to limit privileges and responsibilities. |
| ADR-023 | cell-agent = Sole Long-Term Bucket Credential Holder and Persistent Data Endpoint | cell-agent alone holds long-term bucket credentials and exposes persistent data access. |
| ADR-024 | Ownership = Node Holding the Working SQLite | The node with the active working SQLite database is considered the owner. |
| ADR-025 | do-runtime Has No Heartbeats; Liveness via Replication Activity, Candidates via Platform Discovery | do-runtime liveness is inferred from replication, while candidates are discovered by the platform. |
| ADR-026 | Internal Protocols: Go↔Go Uses gRPC; workerd JS and External APIs Use REST/JSON | Go services communicate over gRPC, while JavaScript runtimes and public interfaces use REST/JSON. |
| ADR-027 | Discovery Not Tied to Kubernetes: Bucket + Replaceable Service Discovery | Service discovery avoids Kubernetes lock-in by using buckets and pluggable discovery mechanisms. |
| ADR-028 | Port Allocation | Define port assignments for components, listeners, and security boundaries. |
| ADR-029 | Tenant Isolation and Binding Authorization | Enforce tenant separation and authorize bindings before granting access to resources. |
| ADR-030 | Bundle and Asset Read Paths | Define how runtime components read deployed bundles and static assets. |
| ADR-031 | Route Projection Delivery (Pull-Only + Short TTL) | Distribute route projections via client pulls with short-lived TTLs to avoid push complexity and stale routing. |
| ADR-032 | DO Alarms and WebSocket | Define how Durable Object alarms interact with WebSocket connections for scheduling and real-time communication. |
| ADR-037 | DO Claiming and Recovery Strategy | Specify how Durable Objects are claimed, recovered, and reassigned after failures. |
| ADR-033 | Timer Deduplication | Prevent duplicate timer execution by defining deduplication rules for scheduled tasks. |
| ADR-034 | Protocol Versioning | Introduce explicit protocol versions to manage compatibility and safe evolution. |
| ADR-035 | Rate Limiting / Quotas / Admission (P1) | Define priority-one admission controls, quotas, and rate limits to protect system resources. |
| ADR-036 | Admin Console Model: No Distinction Between Admins and Tenants | Use a unified management model without separating administrator and tenant identities. |
| ADR-038 | Go Technology Stack and Framework | Choose the Go language stack and supporting framework for implementation. |
| ADR-039 | Persistence Posture Naming: `fleet` / `bucket` | Establish `fleet` and `bucket` terminology for persistence posture concepts. |
| ADR-040 | Follower Spool Uses Single Append-Log with Group Commit fsync | Implement follower spooling as one append log with group-committed fsync for durability and efficiency. |
| ADR-041 | Fleet Transport Group Commit: Framed Batching + Single Batch In Flight | Batch fleet transport frames with group commit while allowing only one in-flight batch. |
| ADR-042 | Fleet Peer Uses HTTP 101 Persistent Binary Stream and Ordered Lanes | Use upgraded HTTP persistent binary streams with ordered lanes for fleet peer communication. |
| ADR-043 | SQL Durability Benchmark Uses Atomic txid + WAL1 Page Payload + Output Barrier | Benchmark SQL durability using atomic transaction IDs, WAL1 page payloads, and output barriers. |
| ADR-044 | SQL Capture: Incremental WAL, Binary Commit, In-Memory Tickets | Capture SQL changes through incremental WAL processing, binary commits, and in-memory tickets. |
| ADR-045 | SQL Capture Uses WAL2 Page-Map Deduplication | Deduplicate SQL capture data using WAL2 page-map tracking. |
| ADR-046 | SQL Write Path Prepared Statement + Peer Network Latency Injection Benchmark | Benchmark SQL prepared-statement writes with injected peer network latency. |
| ADR-047 | Fleet Ordered Pipelining: Multiple In-Flight Batches per Lane (SQL Path Pending) | Allow ordered fleet pipelining with multiple in-flight batches per lane, with SQL support pending. |
| ADR-048 | Capture-Level Ordered Pipelining | Add ordered pipelining at the capture layer to improve throughput while preserving order. |
| ADR-049 | Controlled Checkpoint + KindSnapshot + Recovery Apply | Define controlled checkpoints, snapshot kinds, and recovery apply behavior. |
| ADR-050 | Safe WAL Checkpoint Takeover Under Continuous Writes | Ensure WAL checkpoint ownership can transfer safely while writes continue. |
| ADR-051 | workerd DO → WAL → LTX → cell-agent Output Gate Integration (No workerd Changes) | Integrate the workerd Durable Object output path through WAL, LTX, and cell-agent without modifying workerd. |
| ADR-052 | cellstore Per-Scope Cell Cache (cell-agent SQLite Interface) | Add per-scope cell caching in cellstore through the cell-agent SQLite interface. |
| ADR-053 | do-runtime Host-Actor + Facets and Supervisor Lifecycle Implementation | Implement host actors, facets, and supervisor lifecycle management in do-runtime. |
| ADR-054 | Snapshot Pagination (Splitting Large Cell Snapshots) | Split large cell snapshots into paginated segments for transfer and processing. |
| ADR-055 | On-Demand Paging Primitives (Page Index + Sparse Materialize) and Snapshot Watermark Consistency Fix | Add page-indexed sparse materialization and fix snapshot watermark consistency for on-demand paging. |
| ADR-056 | Cold-Start On-Demand Fetch (L1 Page Index Object + ranged-get PageFetcher) | Fetch cold-start pages on demand using L1 page indexes and ranged-get page retrieval. |
| ADR-057 | Drain Token + Graceful Handoff + node-log/recovery Completion | Complete graceful handoff and recovery using drain tokens and node-log recovery support. |
| ADR-058 | Unified Timer Abstraction + Local Dispatch + Single Fleet Waker | Unify timer handling with local dispatch and a single fleet-level waker. |
| ADR-059 | Owner Resolution Library + Endpoint + Non-Owner Forwarding | Provide owner resolution APIs and endpoints, forwarding non-owner requests appropriately. |
| ADR-060 | Control Plane Skeleton (Apps/Versions/Routes/Keys, Sharded by App) | Build the initial control plane for apps, versions, routes, and keys, sharded by application. |
| ADR-061 | Binding Auth Defense in Depth: Scope Claims + Per-Load Scoped Tokens | Adds scoped claims and per-load tokens to strengthen binding authorization layers. |
| ADR-062 | Content-Addressed Bundle Assets, Traefik Delivery, Audit Retention, and OIDC/JWT Admin | Finalizes P1 asset delivery, auditing, and admin authentication mechanisms. |
| ADR-063 | Binding Facade and Cell-Agent API for KV, D1, R2, and Queue | Defines facade and agent APIs for core storage and queue bindings. |
| ADR-064 | Implement Local Development Mode for `cellhive dev` | Introduces local development workflow support through the Cellhive dev command. |
| ADR-065 | Implement `cellhive dev` with Bun CLI and Miniflare, plus Deploy Compatibility Guards | Combines Bun and Miniflare for local dev while blocking unsupported deploy-side features. |
| ADR-066 | Implement Automatic Recovery Orchestration for Dead Nodes | Adds leader-only, idempotent recovery using leases to detect node liveness. |
| ADR-067 | Implement Queue Consumer Dispatch | Uses cell-agent polling and user-runtime internal dispatch to deliver queue messages. |
| ADR-068 | Implement Public Loader in User Runtime | Loads route projections, versions, and facade environments for user runtime execution. |
| ADR-069 | Implement User-Runtime Asset Pipeline | Adds loader-side index, typing, ETags, headers, redirects, and fallback asset handling. |
| ADR-070 | Integrate Scheduled and Timer Dispatch into User Runtime | Routes cron triggers into user-runtime scheduled handlers. |
| ADR-071 | Implement Asset Routing Configuration | Supports worker-first execution and configurable not-found handling for assets. |
| ADR-072 | Implement Queue Consumer Configuration and Dead Letter Queues | Adds queue consumer settings and DLQ support for failed message handling. |
| ADR-073 | Implement Tenant Binding Facade Egress via Platform Service Bindings | Routes tenant binding outbound traffic through platform service bindings. |
| ADR-074 | Simplify Internal Binding Auth with Local HMAC, No Expiry, and Resource Binding | Replaces internal binding auth with local HMAC tokens tied to resources. |
| ADR-075 | Platform Internal Tier 1: Role-Based Platform Tokens and Dispatch Inbound Auth | Introduces role-scoped platform tokens and adds inbound authorization for dispatch. |
| ADR-076 | Implement Cron Scheduler Generating Slot Timers | Builds cron scheduling by generating timer slots. |
| ADR-077 | DO Runtime Host-Actor Skeleton with Native DO Facets and Local Disk | Establishes the Durable Object runtime host actor foundation. |
| ADR-078 | Implement DO Ownership, Fencing, Draining, and Residency | Manages Durable Object placement, ownership safety, graceful draining, and residency. |
| ADR-079 | Implement DO Alarm Shim | Provides platform-backed alarms when native facet alarms are unavailable, using unified timer dispatch. |
| ADR-080 | End-to-End DO Protocol: Bindings, Placement, Forwarding, Hints, Unknown Results, and WebSockets | Completes Durable Object protocol flow across clients, owners, forwarding, uncertainty, and WebSockets. |
| ADR-081 | DO Storage Lifecycle: Lazy Restart, Migration Validation, and Explicit Storage IDs | Handles Durable Object storage versioning, migrations, and explicit storage identity. |
| ADR-082 | DO Migrations v2: Registry, Rename, Delete, and Proactive Restart | Adds object registry migrations with data-preserving renames, deletion lifecycle, and active restarts. |
| ADR-083 | Do-Supervisor: Facet Storage Capture and Output Gate | Begins RPO-zero support by capturing facet storage and gating outputs. |
| ADR-084 | Cross-Node Cold Activation, Object Addressing, On-Demand Paging, and Takeover | Enables objects to activate cold across nodes, page data as needed, and transfer ownership. |
| ADR-085 | Runtime SQLite VFS Lazy Reads: Feasibility Boundaries and Alternatives | Finalizes limits of lazy SQLite VFS reads and identifies replacement approaches. |
| ADR-086 | Self-Built Workflows Engine | Partially implements an in-house workflows execution engine. |
| ADR-087 | P4: Release Logs, Idempotent Deployments, Admission, and Autoscaler | Adds deployment observability, idempotency, admission controls, and autoscaling. |
| ADR-088 | P5: Protocol Versioning, Diagnostics Enhancements, and Regression Guards | Adds reader-before-writer protocol evolution, improved diagnostics, and regression protection. |
| ADR-089 | Bindings Inside Durable Objects with Cloudflare Parity | Aligns Durable Object internal bindings with Cloudflare behavior. |
| ADR-090 | Migrate Bindings to RPC Entrypoint Env, Phase 0 for KV | Starts migrating bindings to RPC entrypoint environments, beginning with KV. |
| ADR-091 | CLI Resource Commands + Assets Version Token + Tail | Adds CLI resource commands, asset version tokens, and tailing support for operational visibility. |
| ADR-092 | backend-A Capture Wiring (cellstore → LTX → fleet → bucket) | Defines backend-A capture pipeline from cellstore through LTX and fleet into buckets. |
| ADR-093 | cell-agent Write Path Optimization (Binding Cache / KV Batch Writes / Capture Tuning) | Optimizes cell-agent writes using binding caching, KV batching, and capture parameter tuning. |
| ADR-094 | Single-Key Write Path Optimization (Per-Cell Write Serialization + Window Activation) | Improves single-key writes with per-cell serialization and window-based activation. |
| ADR-095 | Write Path Cost Breakdown + txid In-Memory Mirror | Breaks down write path costs and introduces an in-memory txid mirror. |
| ADR-096 | Cell Handle LRU + Idle Eviction | Manages cell handles with LRU caching and eviction of idle entries. |
| ADR-097 | KV Namespace Must Have an ID (No More Hardcoded default) | Requires explicit KV namespace IDs instead of relying on a hardcoded default. |
| ADR-098 | KV TTL and Metadata (Expiration Cleanup Reuses timer/waker Activation) | Adds KV TTL and metadata with expiration cleanup driven by timer/waker activation. |
| ADR-099 | Scalable timer/waker: Bucket Wake Index + Scan Only on Expiry (Fix Alarm Failure) | Scales timer/waker by indexing bucket wakeups and scanning only expired items. |
| ADR-100 | Skip Capture for D1 Read-Only Statements | Avoids capture overhead for D1 statements that only perform reads. |
| ADR-101 | Resident Limits + Ownership Rebalance Prototype + RPO=0 Fault Injection Tests | Adds resident limits, prototypes ownership rebalancing, and tests RPO=0 failure behavior. |
| ADR-102 | Service Binding Uses Same-Instance Native JSRPC | Routes service bindings through same-instance native JSRPC for lower overhead. |
| ADR-103 | DO Owner Hint (cell-agent Side) Saves One Hop | Uses cell-agent owner hints for Durable Objects to eliminate an extra hop. |
| ADR-104 | Pin Service Target Version at Deployment | Pins service target versions during deployment for deterministic routing. |
| ADR-105 | Localized Service Fetch + One-Hop DO Caller (Shard-Signed Owner Ticket) | Localizes service fetch and enables one-hop Durable Object calls using shard-signed owner tickets. |
| ADR-106 | Fast Path Switch + A/B Latency Quantification | Adds a fast-path toggle and A/B measurement for latency impact. |
| ADR-107 | DO Session Policy + Deletion Lock (Align with CF) | Defines Durable Object session policy and deletion locking aligned with Cloudflare behavior. |
| ADR-108 | Owner Epoch Monotonic Generation (Fix Epoch Reuse) | Prevents owner epoch reuse by enforcing monotonic generation values. |
| ADR-109 | Configurable DO Lease TTL + Persistent Object Registration Index (Optional) | Makes Durable Object lease TTL configurable and optionally indexes persistent object registrations. |
| ADR-110 | Bundle GC (Reclaim Unreferenced Worker Bundles) | Garbage-collects worker bundles that are no longer referenced. |
| ADR-111 | Assets GC (Reclaim Unreferenced Assets Versions) | Garbage-collects assets versions that are no longer referenced. |
| ADR-112 | Queue Consumer `max_concurrency` | Adds `max_concurrency` control for queue consumers. |
| ADR-113 | R2 Multipart Upload | Supports multipart uploads for R2 objects. |
| ADR-114 | Dev CLI Assets Alignment with Production Loader (`_headers`/`_redirects`/`not_found_handling`/Worker Fallback) | Aligns dev CLI asset handling with production loader behavior and fallback rules. |
| ADR-115 | Route Read Path: Per-Host Pointer + Versioned Worker Details + Cache Governance | Optimizes route reads with per-host pointers, versioned worker details, and cache management. |
| ADR-116 | Faster Route Revocation + Unknown Host Query Throttling | Speeds route revocation and throttles queries for unknown hosts. |
| ADR-117 | Single Control Database + Relational Tables (Replaces ADR-060 Per-App Sharding) | Replaces per-app sharding with one control database and relational tables. |
| ADR-118 | Ownerless Control Plane and D1/KV Write Path (Transparent Claiming/Forwarding) | Enables transparent claiming or forwarding for ownerless control-plane and D1/KV writes. |
| ADR-119 | Queue Consumer Ownership (Only One Owner Consumes) + Writes Through Capture | Ensures only one queue owner consumes while writes go through capture. |
| ADR-120 | Forward Reads to Owner + Forwarding Retry Classification | Forwards reads to the owner and classifies retry behavior for forwarded requests. |
| ADR-121 | Write-Path Ownership Audit: Complete Gates and Capture for Timer, KV Expiry, and DO Alarms | Audits write-path ownership and adds missing gates and capture for timers, KV expiry, and Durable Object alarms. |
| ADR-122 | Local Disk: File Ledger, Byte Budget, and LRU Eviction with Owned Files Preserved | Introduces local disk accounting, byte budgeting, and LRU eviction while ensuring owned files are never deleted. |
| ADR-123 | Disk: Safe Reclamation of Owned Files and High-Watermark Backpressure | Adds safe owned-file reclamation and high-watermark backpressure to protect disk capacity. |
| ADR-124 | cellhive deploy --config: Directly Consume Wrangler Configuration | Enables deployments to read Wrangler configuration directly through the config option. |
| ADR-125 | Hyperdrive Binding: Connection String Only, No Platform Connection Pooling | Defines Hyperdrive bindings as connection-string providers without platform-managed connection pooling. |
| ADR-126 | Loader Cache IDs Must Include Version Numbers | Requires loader cache identifiers to include versions so binding-only deployments take effect. |
| ADR-127 | Version Numbers Across All Internal Dispatch Paths | Propagates versions through internal dispatch so isolate and facet keys include worker, version, and SHA. |
| ADR-128 | Inject Bindings for Queue, Scheduled, and Workflow Dispatch | Resolves specs by version and injects bindings into queue, scheduled, and workflow dispatch. |
| ADR-129 | Hyperdrive as a Registered Resource | Registers Hyperdrive resources with envelope-encrypted origin URLs and name-based resolution. |
| ADR-130 | Local Connection Reuse via Durable Object-Held Connections | Implements local connection reuse by holding connections inside Durable Objects, with evidence and wiring. |
| ADR-131 | Control Plane Schema v2 and Domain Route Model Implementation | Implements control-plane schema v2 with domain and route modeling. |
| ADR-132 | Remove Edge Configuration Delivery Implementation | Removes edge configuration delivery because the platform does not provide external proxy functionality. |
| ADR-133 | No DNS Verification for Domains Implementation | Treats domain registration as authorization without performing DNS verification. |
| ADR-134 | 2026-09-17 Code Review Fix Batch Implementation | Applies review fixes for authorization, watermarks, loading, storage, and documentation. |
| ADR-135 | Review Follow-Up Fix Batch Implementation | Fixes purge resurrection, commit fencing, WebSocket identity, storage fencing, loading, and authorization hardening. |
| ADR-136 | Remove Unimplemented gRPC Plane and Clean Environment Wiring | Removes the unimplemented gRPC plane and cleans related environment variables and wiring. |
| ADR-137 | Derive All Credentials from a Single Root Key | Simplifies environment configuration by deriving all credentials from one root key. |
| ADR-138 | cellhive wrangler Prefix Implementation | Adds Wrangler-style aliases and automatic configuration discovery under the cellhive wrangler prefix. |
| ADR-139 | Deployable Artifacts Implementation | Provides deployable images, compose files, and Kustomize manifests. |
| ADR-140 | do-supervisor Wiring: Durable Object Output-Gate Deployment Shape | Wires do-supervisor to support the Durable Object output-gate deployment model. |
| ADR-141 | Deployment Hardening Implementation | Hardens deployment with non-root execution, Helm charts, CI gates, and orchestration integrity checks. |
| ADR-142 | Purge Closed Loop Implementation | Wires data-side purge cleanup across buckets, local replicas, and Durable Object segments. |
| ADR-143 | Async Upload: Persistent Retry Queue Implementation | Adds a write-ahead spool for durable asynchronous upload retries. |
| ADR-144 | Service Binding ACL and Cross-Namespace Service Binding Implementation | Implements access controls for service bindings and supports bindings across namespaces. |
| ADR-145 | R2 List: Cursor Pagination and Sized Listing Implementation | Adds cursor pagination and bounded listing to avoid materializing entire prefixes. |
| ADR-146 | W3C Trace Context Propagation Implementation with Boundaries | Propagates traceparent context across supported boundaries while defining limits. |
| ADR-147 | Control Plane Secrets Management: Delete and List Implementation | Adds delete and list operations for control-plane secrets management. |
| ADR-148 | CLI Compatibility Completion Implementation | Adds dry-run, var, secrets-file support, and compatible command groups. |
| ADR-149 | Local Alternative Validation for Class C Environments Completed | Completes audits for local substitutes in Class C environments and documents remaining gaps. |
| ADR-150 | Root Key Sources: Environment and File, KMS Seam, and mTLS Tradeoff Implementation | Supports root keys from environment or files, leaves a KMS seam, and excludes cloud KMS implementation. |
| ADR-151 | scaling-and-ha Finalization: Autoscaler Cooldown and Cross-AZ Follower Placement Implementation | Implements autoscaler cooldown behavior and places followers across availability zones for improved high availability. |
| ADR-152 | timers-and-dispatch Finalization: Batch/Retry/DLQ Defaults, Waker Backoff, and Timer Cell Strategy Implementation | Defines timer and dispatch defaults, waker backoff, and timer cell placement strategy. |
| ADR-153 | Compatibility Matrix Closure: Exact Flag List, Framework Acceptance, and D1 Sessions Rejection Implementation | Finalizes compatibility flags, acceptance criteria, and explicit rejection of unsupported D1 sessions. |
| ADR-154 | Full-Service E2E Dispatch Defect Fixes: Timer Namespace, event.cron, DISPATCH_URL, and MessageBatch Implementation | Fixes dispatch issues exposed by full-service end-to-end testing. |
| ADR-155 | Queue: Per-Message ack()/retry({delaySeconds}), Delayed Production, and Typed Body Implementation | Adds per-message acknowledgements, delayed retries, delayed enqueueing, and typed queue bodies. |
| ADR-156 | vwork Operations API: Resource Revocation, Queue Status/DLQ Replay, Readiness and Drain Probes Implementation | Adds operational controls for resources, queues, dead-letter replay, readiness, and draining. |
| ADR-157 | Resource Management Delegated to Functional Domains, Per-Domain Stats (Metadata-Only), and Legacy Path Aliases Implementation | Moves resource management into domains while preserving stats and legacy path compatibility. |
| ADR-158 | Vectorize Binding: Resource Registration, In-Cell float32 Storage, and Go Exact KNN Implementation | Implements Vectorize resource binding, float32 storage, and exact KNN search in Go. |
| ADR-159 | Replace SQLite with CGo and vec1 ANN: Vector Retrieval Implementation | Switches SQLite integration to CGo and adds vec1 ANN-based vector search. |
| ADR-160 | Runtime VFS Lazy Reads: Cell-Agent Paged Cold Restore Implementation | Enables lazy paged restore from cell-agent during runtime cold starts. |
| ADR-161 | LTX Compression Runtime Switch (`CELLHIVE_LTX_COMPRESSION`) Implementation | Adds a runtime environment switch to control LTX compression. |
| ADR-162 | Durable Object Calls Support RPC: Tagged JSON and Native JSRPC Implementation | Adds RPC support for Durable Object calls via tagged JSON and native JSRPC. |
| ADR-163 | R2/D1 Contract Fidelity: Full R2Object Fields, put Metadata, and D1 last_row_id/Error Shape Implementation | Aligns R2 and D1 behavior with expected object fields, metadata, row IDs, and error shapes. |
| ADR-164 | Peer Adaptive Hedging and Sequence-Idempotent Spool Implementation | Adds adaptive peer request hedging and idempotent spool processing by sequence. |
| ADR-165 | Observability Metrics Completion: Batch 1, Server Side Implementation | Adds the first batch of missing server-side observability metrics. |
| ADR-166 | Observability Metrics Completion: Batch 2, Replication/Timers and do-runtime Implementation | Adds missing metrics for replication, timers, and the Durable Object runtime. |
| ADR-167 | Standard OpenTelemetry OTLP Trace Export Implementation | Implements standard OTLP export for distributed tracing. |
| ADR-168 | R2 list include/delimiter Cloudflare Alignment Implementation | Aligns R2 list include and delimiter behavior with Cloudflare semantics. |
| ADR-169 | Purge Changed to Cursor-Paginated Deletion Implementation | Replaces purge deletion with cursor-based pagination for safer large-scale cleanup. |
| ADR-170 | DO Output Gate Concurrent Capture and orderedDispatcher Active Eviction Implementation | Completes workerd integration by adding concurrent output capture and proactive orderedDispatcher eviction. |
| ADR-171 | Upload Spool Power-Loss Safety: Atomic Writes and Memoized Directory fsync Implementation | Makes upload spooling resilient to power loss through atomic writes and directory fsync memoization. |
| ADR-172 | Optional Tenant Log OTLP Export: Off by Default, Export Only for Tail Subscriptions Implementation | Adds tenant log OTLP export gated by tail subscriptions and disabled by default. |
| ADR-173 | Fleet Broadcast for Log Subscriptions: Tail Gate Covers All Nodes Implementation | Broadcasts log subscriptions fleet-wide so tail gating applies across every node. |
| ADR-174 | Example Compatibility Fixes: KV put Body Type, Legacy DO Class, and DO Response Pass-Through Implementation | Fixes example compatibility issues for KV bodies, legacy Durable Objects, and response pass-through. |
| ADR-175 | Compatibility Closure: DO WebSocket Wiring, R2 Local Metadata, and DO Alarm Identity Implementation | Finalizes compatibility for Durable Object WebSockets, R2 local metadata, and alarm identity. |
| ADR-176 | KV Write Validation Aligned with Cloudflare: Key, Metadata, TTL, and Expiration Implementation | Aligns KV write validation rules with Cloudflare for keys, metadata, TTL, and expiration. |
| ADR-177 | Wake Index Not Behind Timer: Publish First and Repairable Implementation | Ensures wake indexes do not lag timers by publishing first and allowing repair. |
| ADR-179 | Namespace attribution for metrics (ns dimension) + tenant attributes on binding spans | Tenant-attributable metrics carry a bounded `ns` label (cap `CELLHIVE_METRICS_NS_MAX`; overflow `other`, empty -> `platform`), and binding `http.server` spans carry `cellhive.namespace`. |
| ADR-178 | Logs carry trace context + a multi-tenant observability reference pipeline (push) | Tenant log lines carry trace_id/span_id, OTLP resource gains service.instance.id, and a Collector+backend org/stream push model replaces exposing /metrics. |
| ADR-184 | Zero Tenant-Env Platform Transport via Platform-Side Stubs | DO WebSocket passthrough keeps both workerLoader boundaries fetch-shaped: public loader → `CellHiveHost.fetch(Request)` and tenant DO facade → `DurableObjectNamespace.fetch(Request)`. Owner lookup/tickets remain trusted-side; `CH_DO_CONNECT` is not restored. |
| ADR-185 | Tenant Env Is Fully User-Owned: Zero Platform Keys | Tenant Worker and DO env namespaces contain only user-declared values and user-named bindings; platform transport and credentials stay in trusted hosts. |
| ADR-186 | Stock-workerd Runtime Baseline and Security Hardening | Pins stock workerd `1.20260916.1` and esbuild `0.28.2`; uses `fromEnvironment`, removes platform credentials from final WorkerCode, generates compatibility rules from pinned source, and enforces 64 MiB code / 1016 KiB env budgets. Real Docker source-build/env/KV/gated-DO restart acceptance and the no-skip gate pass (`GATE: PASS 14/14`). |

_Last updated: 2026-09-22_
