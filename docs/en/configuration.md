# Configuration and Environment Variables

The authoritative source is the code: `internal/config/config.go` (cell-agent), `cmd/user-runtime/main.go`, `cmd/do-runtime/main.go`, `cmd/cellhive/main.go` (CLI), `internal/workerdbin`, and `internal/bundler`. This document lists **all** environment variables, defaults, and purposes by subsystem. If it conflicts with the code, the code takes precedence.

## Parsing Rules

| Type | Rule |
|---|---|
| String | Unset or empty string → default value |
| Boolean | `true`/`1` are true; everything else (including `false`/`0`) is false. Exception: `CELLHIVE_DO_PREVENT_EVICTION` only accepts **exactly** `true`/`false` (otherwise startup fails) |
| Duration | **Always use Go syntax**: `100ms`, `30s`, `5m`, `24h`; invalid values fall back to the default. There are no integer variables with `_MS`/`_S` suffixes |
| Bytes | Plain integer (bytes) or `512MB`/`2GiB`/`1g`; KB/MB/GB are decimal, KiB/MiB/GiB are binary |
| List | Comma-separated, trim whitespace per item, ignore empty items |
| Pair/rate | `a:b` (such as `CELLHIVE_LOG_BUFFER=1000:200`) or `rps/burst` (such as `CELLHIVE_NS_RATE=100/200`) |

**Credential model (ADR-137)**: Set only **one** `CELLHIVE_ROOT_KEY`. Tokens for each role are derived with HKDF-SHA256 using domain separation, so all components automatically stay consistent as long as they share the same root. There is no need to configure them one by one:

```
peer / internal / dispatch / log / admin / scope / do-ticket / secrets-root
     = HKDF-SHA256(ROOT_KEY, "cellhive/token/<role>")
```

`CELLHIVE_ADMIN_TOKEN` can still independently override the admin credential (to make independent operations rotation easier). Startup fails if no root is set and `CELLHIVE_ALLOW_INSECURE_DEFAULTS` is not enabled.

**Migration note**: Variables removed or renamed by ADR-136/137 (such as `CELLHIVE_TOKEN_*`, `CELLHIVE_S3_*`, `CELLHIVE_NS_RPS`, `CELLHIVE_*_MS`/`_S`) will print a WARN with the replacement name (`config.LegacyEnvWarnings`) when components start if they are still set. They will not silently take effect.

## Security / Credentials

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_ROOT_KEY` | Empty | Platform root key (base64 or hex, ≥16 bytes, 32 recommended); derives all role credentials. In production, **configure only this one** (preferred when not using a file) |
| `CELLHIVE_ROOT_KEY_FILE` | Empty | Root key **file** path (Docker/K8s secret mount; trim after reading). Takes precedence over the dev fallback and lower precedence than inline `CELLHIVE_ROOT_KEY`; if the file is unreadable/empty, it **fails closed** (ADR-150) |
| `CELLHIVE_ADMIN_TOKEN` | Empty→derived | Optional: independently override the control-plane admin credential |
| `CELLHIVE_ALLOW_INSECURE_DEFAULTS` | `false` | Allow the local development root (`DevRootKey`). Must be false in production |
| `CELLHIVE_OIDC_JWKS_URL` | Empty | When set, admin can additionally use verified JWT bearer tokens (ADR-036/131) |
| `CELLHIVE_OIDC_ISSUER` / `_AUDIENCE` | Empty | JWT iss/aud validation (JWT must contain `exp`) |

## Object Storage

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_BUCKET` | Empty | Use S3-compatible storage with `s3://<bucket>`; empty means use a filesystem bucket |
| `CELLHIVE_BUCKET_DIR` | `./.cellhive/bucket` | Filesystem bucket directory. Required |
| `AWS_ENDPOINT_URL` | Empty | S3 endpoint (MinIO/cloud; standard AWS variable name) |
| `AWS_REGION` | `us-east-1` | Region |
| `AWS_ACCESS_KEY_ID` | Empty | Access credential |
| `AWS_SECRET_ACCESS_KEY` | Empty | Secret key |
| `CELLHIVE_S3_PATH_STYLE` | Custom endpoint present→`true`, otherwise `false` | path-style; by default inferred from whether a custom endpoint is present (local/compatible storage commonly uses path-style, cloud commonly uses virtual-host) |

## cell-agent: Processes and Listening

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_NODE_ID` | `node-1` | Stable node identity (owner/handoff key), required |
| `CELLHIVE_SESSION_ID` | Startup nanoseconds | Session id for this run; new after restart |
| `CELLHIVE_ADVERTISE` | `127.0.0.1:7001` | Internal advertised address (owner forwarding target); internally only REST `:7001` |
| `CELLHIVE_PEER_URL` | `http://127.0.0.1:7001` | This node's REST callback address (peer replication target) |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | Local working directory root (spool, DO disks, etc. are derived from this) |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | Ephemeral runtime directory root; each runtime uses a subdirectory |
| `CELLHIVE_REST_ADDR` | `:7001` | Internal REST (shared by Go↔Go and workerd bindings, no gRPC) |
| `CELLHIVE_ADMIN_ADDR` | `:8082` | admin/control-plane listener |
| `CELLHIVE_DO_RUNTIMES` | Empty | do-runtime list (DO placement); empty disables `/v1/do/invoke` |

## Durability / Replication / RPO

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_DURABILITY` | `auto` | ack posture: `auto`/`fleet`/`bucket`; `fleet` requires follower fsync and never silently acks |
| `CELLHIVE_BUCKET_WAIT` | `true` | true=ack after waiting for bucket upload (RPO=0); false=ack after enqueue; `fleet` must be true |
| `CELLHIVE_LEASE_TTL` | `10s` | owner lease, must be positive |
| `CELLHIVE_PEER_LATENCY` | `0` | Synthetic one-way latency (load testing, round trip ×2) |
| `CELLHIVE_PEER_PIPELINE` | `4` | Number of in-flight batches per lane, 1=disable pipelining |
| `CELLHIVE_PEER_HEDGE_MS` | `adaptive` | Replica hedge (ADR-164): write primary first; `adaptive`/`auto` (default) sends the second copy after exceeding `max(250ms, 4×recent slowest append)`; `0`=only send primary (single copy); `>0`=fixed wait in ms |
| `CELLHIVE_PEER_HEDGE_MAX_MS` | `2000` | Upper bound for adaptive hedge wait (backstop) |
| `CELLHIVE_OTLP_ENDPOINT` | Empty=off | OpenTelemetry OTLP/HTTP export endpoint (such as `http://collector:4318` or OpenObserve `http://openobserve:5080/api/default`; a full `/v1/traces` URL is also accepted). Empty = completely disabled, zero overhead (ADR-167; see [`tracing.md`](tracing.md)) |
| `CELLHIVE_OTLP_HEADERS` | Empty | OTLP export request headers, `k=v,k2=v2` (for authentication) |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | Head sampling ratio for new entry traces (upstream `traceparent` sampled bit takes precedence) |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | `service.name` for the OTLP Resource (the same variable is used for entry sampling in user-runtime) |
| `CELLHIVE_OTLP_LOGS` | `off` | OTLP logs export: `off`=off; `tail`=only workers subscribed by `cellhive tail` are exported; `all`=all (ADR-172; requires `CELLHIVE_OTLP_ENDPOINT`) |
| `CELLHIVE_UPLOAD_SHARDS` | `0`→NumCPU(≤8) | Parallel bucket upload channels |
| `CELLHIVE_CAPTURE` | Enabled | `off`/`0`/`false` disables capture=writes only local, with no replication proof |
| `CELLHIVE_CAPTURE_GROUPCOMMIT` | `0`→2ms | WAL coalescing window |
| `CELLHIVE_CAPTURE_PIPELINE` | `0`→8ms | Commit latency threshold; if exceeded, multiple chunks are in flight |
| `CELLHIVE_CELL_WAL_CHECKPOINT` | `64MiB` | Capture cell WAL truncation threshold, `0` disables |
| `CELLHIVE_FORGET_ON_LOSS` | `true` | Delete local files after ownership is lost (false requires disk budget configuration) |

## cell-agent: Local Disk and Memory Budgets

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_MAX_OPEN_CELLS` | `0` unlimited | Cache handle limit |
| `CELLHIVE_MAX_RESIDENT_CELLS` | `0` unlimited | Resident handle limit |
| `CELLHIVE_CELL_IDLE` | `0` no cleanup | Close idle handles |
| `CELLHIVE_CELL_DISK_MAX` | `0` unlimited | Cell file byte budget (LRU deletes non-owned files) |
| `CELLHIVE_CELL_DISK_SWEEP` | `1m` | janitor interval |
| `CELLHIVE_DISK_HIGH` | `0` off | High watermark→overloaded, reject claim |

## cell-agent: Ownership / Scaling

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_CELLS_PER_NODE` | `100` | Per-node capacity target |
| `CELLHIVE_PLACEMENT_WEIGHT` | `0`→CPU | Ownership share |
| `CELLHIVE_PLACEMENT_AZ` | Empty | This node's failure domain (rack/zone); after it is set, follower selection **prefers a different AZ** (ADR-151). Empty=no preference |
| `CELLHIVE_REBALANCE_INTERVAL` | `0` off | Rebalancing loop |
| `CELLHIVE_REBALANCE_MAX_MOVE` | `32` | Maximum releases per round |
| `CELLHIVE_AUTOSCALE_MIN` | `1` | Target lower bound |
| `CELLHIVE_AUTOSCALE_MAX` | `0` unlimited | Target upper bound |
| `CELLHIVE_AUTOSCALE_INTERVAL` | `30s` | Evaluation interval |
| `CELLHIVE_AUTOSCALE_COOLDOWN` | `5m` | Cooldown window for action **changes** (debounce; 0=off, ADR-151). During cooldown, returns `hold/cooldown` + `cooldown_remaining_ms` |

## cell-agent: Background Loops

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 off) | Expiration timer dispatch |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Expiration limit per pass / fired marker retention |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 off) | queue consumption |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0` (store default) | Messages per batch / visibility lease |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | Redelivery delay for failed batches (retry limit/DLQ are configured per consumer) |
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 off) | cron→timer materialization |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 off) | fleet waker (TTL is automatically 2×) |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Limit per pass / fired marker retention |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | Backoff cap on consecutive errors (base=interval, `interval·2^fails`) |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 off) | Local wake index repair scan (ADR-177); runs once at startup first |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | Number of local cells checked per pass (rotating window, covers all after several passes) |
| `CELLHIVE_DRAIN_TTL` | `30s` | drain token validity period (shutdown wait = same value) |
| `CELLHIVE_PAGED_RESTORE` | bool, default `true` | cell-agent: during cold restore, use fault-in VFS to page-load cells that are "compacted and whose chain ≥ MIN_BYTES" (ADR-160) |
| `CELLHIVE_PAGED_MIN_BYTES` | bytes, default `256MiB` | Chains smaller than this are cloned in full; `0` = always page |
| `CELLHIVE_PAGED_HYDRATE_MBPS` | integer, default `16` | Background hydration rate for paged cells (MiB/s, one at a time per node); `0` = keep sparse and read each cold page from the bucket |
| `CELLHIVE_PAGED_WINDOW_PAGES` | integer, default `64` | Window prefetch: maximum number of adjacent pages to fetch in one ranged read (also has a 256KiB budget) |
| `CELLHIVE_PAGED_PREFETCH_WORKERS` | integer, default `4` | Number of concurrent child prefetch workers |
| `CELLHIVE_LTX_COMPRESSION` | bool, default `true` | Use LZ4 for the LTX page-map (`WAL3`); `false` falls back to fixed-frame uncompressed `WAL2` (saves CPU, uses more bucket bytes; both formats are compatible for decoding/GC, ADR-161) |
| `CELLHIVE_COMPACTION_INTERVAL` | `30s` (0 off) | L0→L1 compaction |
| `CELLHIVE_COMPACTION_MIN_SEGMENTS` | `64` | Segment count trigger |
| `CELLHIVE_COMPACTION_MIN_BYTES` | `64MiB` | Byte trigger |
| `CELLHIVE_BUNDLE_GC_INTERVAL` | `0` off | bundle/assets GC |
| `CELLHIVE_BUNDLE_GC_GRACE` | `24h` | Unreferenced retention period |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 forever) | Audit retention |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` forever | Prune terminal instances |
| `CELLHIVE_LOG_BUFFER` | `1000:200` | Log tail buffer `<entries>:<workers>` (`cellhive tail --worker`) |
| `CELLHIVE_METRICS_NS_MAX` | `1000` | Cap on `ns` label values for tenant-attributable `/metrics` (overflow -> `other`, empty -> `platform`, 0 = unlimited) (ADR-179) |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 off) | binding declaration cache |
| `CELLHIVE_NS_RATE` | Empty (off) | Per-ns write admission `rps[/burst]` (default burst=rps) |

## cell-agent: Control Plane / Domains / DO

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_BASE_DOMAIN` | Empty | Built-in domain `<ns>-<worker>.<base>`; empty=disabled |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | Automatically create app on first deploy |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | Eagerly restart DO on deploy |
| `CELLHIVE_DISPATCH_URL` | Empty | Timer/queue/cron/workflow dispatch target — must be the **user-runtime internal privileged endpoint** (`http://user-runtime:8088`). If empty, dispatch loops do not start (already set by default in compose/k8s/Helm) |
## workerd / Packaging

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_WORKERD` | auto-discover | workerd path (prefer pinned `1.20260615.1`) |
| `CELLHIVE_WORKERD_DIR` | empty | local package store dir (searched for `@cloudflare+workerd-linux-64@*`, pinned preferred) |
| `CELLHIVE_ESBUILD` | auto-discover | esbuild path |
| `CELLHIVE_ESBUILD_DIR` | empty | local package store dir (searched for `esbuild@*`) |

## user-runtime

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_CELL_URL` | `http://127.0.0.1:7001` | cell-agent REST |
| `CELLHIVE_ROOT_KEY` | required | Derives internal/scope/dispatch/log credentials |
| `CELLHIVE_USER_RUNTIME_PORT` | `8081` | public entrypoint |
| `CELLHIVE_USER_RUNTIME_INTERNAL_PORT` | `8088` | internal dispatch |
| `CELLHIVE_USER_RUNTIME_JS` | `workerd/user-runtime` | loader JS |
| `CELLHIVE_FACADES_JS` | `workerd/platform/facades.js` | facade source |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | runtime directory (subdirectory `user-runtime/`) |
| `CELLHIVE_SERVICE_NATIVE` | empty=enabled | `0` disables native service RPC |
| `CELLHIVE_DO_DIRECT` | empty=enabled | `0` disables owner-hint direct access |
| `CELLHIVE_TENANT_OUTBOUND` | empty→`public` | egress category `public`/`private`/`local` |
| `CELLHIVE_CAP_EGRESS` | empty (recommended default) | extra network ranges (CIDR/categories, comma-separated, OR-combined) allowed for the capability data-plane egress (PLATFORM binding). **No configuration needed** when capability runs over the public domain + TLS (private denied by default = secure default); for intranet-direct runtime-services add its CIDR block (e.g. `10.20.0.0/16`) or single IP (`10.20.1.5/32`) |
| `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | empty | BYO AI endpoint and key; empty=not injected |

## do-runtime

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_DO_ADDR` | `*:8788` | workerd listener |
| `CELLHIVE_DO_NODE` | hostname | node id |
| `CELLHIVE_DO_ADVERTISE` | `http://127.0.0.1:8788` | advertised address (owner forwarding target) |
| `CELLHIVE_DO_PREVENT_EVICTION` | `true` | resident/evictable; must be exactly `true`/`false` |
| `CELLHIVE_DO_LEASE` | `30s` | DO owner lease (Go duration, converted to whole seconds for the host actor) |
| `CELLHIVE_DO_RUNTIME_JS` | `workerd/do-runtime` | host actor JS |
| `CELLHIVE_DO_GATE_URL` | empty | output gate base URL; empty=no gate |
| `CELLHIVE_DO_OBJECT_INDEX` | `false` | persist object registry to bucket (`true`/`1`) |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | DO SQLite disk = `<DATA_DIR>/do` |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | runtime directory (subdirectory `do-runtime/`) |
| `CELLHIVE_CELL_URL` / `CELLHIVE_ROOT_KEY` / `TENANT_OUTBOUND` / `AI_*` | same as above | same as user-runtime |

## CLI (`cellhive`)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_ADMIN_URL` | `http://127.0.0.1:8082` | admin endpoint |
| `CELLHIVE_CONTROL_URL` | `http://127.0.0.1:7001` | internal endpoint |
| `CELLHIVE_ROOT_KEY` | required | Derives admin (`CELLHIVE_ADMIN_TOKEN` can override) and internal tokens |
| `CELLHIVE_ADMIN_JWT` | empty | If set, use `Bearer` instead (namespace-level authorization) |
| `CELLHIVE_BIN` | auto | packager used by dev `--strict-build` |

## Tests / CI (non-runtime)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_S3_TEST_ENDPOINT` | empty | Points to an existing S3; if empty, `make s3-test` attempts to start MinIO, and skips if docker is unavailable |
| `CELLHIVE_S3_TEST_ACCESS` / `_SECRET` / `_BUCKET` | `minioadmin`/`minioadmin`/`cellhive` | S3 integration test credentials |
| `CELLHIVE_PERF_GATE` | empty | Set by `make perf-test`; enables performance gate assertions |
| `TEST_BYTES` | — | Used by `internal/config` unit tests |

## Related documentation

- Services and ports, K8s, rolling upgrades: [`deployment.md`](deployment.md)
- Operations runbook: [`operations.md`](operations.md)
- Security boundaries and credential layering: [`security.md`](security.md)
- Bucket roles and credentials: [`storage-and-s3.md`](storage-and-s3.md)

_Last updated: 2026-09-19_