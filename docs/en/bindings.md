# Binding Mapping

Workers see Cloudflare-shaped bindings through `env`; the platform uses a **host adapter** (platform code inside workerd) to translate them into calls to `cell-agent`. A binding is uniquely bound to a specific cell by immutable props.

## Mapping Table

| CF binding | Backend | API shape | Storage | owner |
|---|---|---|---|---|
| **KV** | KV cell | Async | SQLite (`__kv__` cell) | cell-agent node |
| **D1** | D1 cell | Async (`prepare/bind/all/batch/exec`) | SQLite (`__d1__` cell) | cell-agent node |
| 　└ Contract fidelity | `meta` includes `changes/last_row_id/changed_db/duration`; error `err.name=D1_ERROR` + `err.code` (ADR-163) | | | |
| **R2** | S3-compatible object storage | Async | `r2/<ns>/<bucket>/...` + `r2meta/...` (metadata sidecar) | Object storage |
| 　└ Contract fidelity | All fields of `R2Object/R2ObjectBody` (`key/version/size/etag/httpEtag/uploaded/httpMetadata/customMetadata/checksums` + `text/json/arrayBuffer/blob`); `put` supports `{httpMetadata,customMetadata,md5,sha256}` (validation); `head()`; list `truncated/cursor` + `include` (metadata on demand) + `delimiter`/`delimitedPrefixes` (ADR-163/168) | | | |
| **Queues producer** | Queue cell | Async | SQLite (`__queue__` cell) | cell-agent node |
| **Queues consumer** | `queue()` handler | Dispatch | Same as above | Dispatched by cell-agent |
| **Workflows** | Workflow cell + engine | Async | SQLite (`__workflow__` cell) | cell-agent node |
| **Cron** | cron projection cell | `scheduled()` | SQLite (`__cron__` cell) | cell-agent node |
| **Durable Objects** | do-runtime native facet | **Synchronous SQL**; bindings `env.DO.get(...).fetch()` and **`getByName(...).method(...)` DO RPC** (ADR-080 shard/owner/hint/WS 1012; ADR-162 RPC tagged JSON + native JSRPC) | workerd actor SQLite (working replica) → cell-agent replication | **do-runtime node** |
| **ASSETS** | Object storage (versioned) | Async | `assets/<ns>/<worker>/<token>/...` | Object storage |
| **Service bindings** | workerd JSRPC | Sync/async | — | Target Worker |
| **Vars / Secrets** | Injected into `env` at load time | — | secrets in control cell (ciphertext) | cell-agent |
| **AI** | BYO OpenAI-compatible endpoint (`env.AI.run`) | Async | — | cell-agent (`CELLHIVE_AI_URL`/`CELLHIVE_AI_KEY`; no platform-hosted catalog) |

## host adapter Model

- Each binding generates a **binding-scoped facade** at load time, with immutable props (`ns` + binding type/id);
- Tenant code only gets the facade; it **cannot access** internal tokens, backend addresses, or a generic Fetcher;
- The facade calls `cell-agent` (`:7001` REST/JSON) to execute the corresponding operation; the tenant loaded worker’s `globalOutbound` is public-only, and capability bindings run as **platform-side entrypoint stubs** (the `:7001` transport stays in the platform worker; ADR-184);
- The facade only carries **`x-cellhive-scope-token`** (computed locally with HMAC at platform load time, no expiration, ADR-074); `ns/kind/name` are inside the same token, and cell-agent verifies the signature + `HasBinding`. A broad-privilege internal token is **no longer carried**.

## Synchronous vs Async

| | API | Notes |
|---|---|---|
| KV/D1/R2/Queue/Workflows/Cron/ASSETS | **Async Promise** | Can go out of process to `cell-agent` (`cell-agent` owns SQLite) |
| **DO `ctx.storage.sql` / `transactionSync`** | **Synchronous** | Must stay inside the workerd process → native facet; persistence via supervisor→cell-agent (see [`durable-objects.md`](durable-objects.md)) |

## Unsupported (Explicitly Rejected)

`Vectorize`, `AI Search`, `Dispatch Namespaces`, `Secrets Store`, `send_email`, `Browser Rendering`, `Images`, `Cache API`, `Python Workers`, `Analytics Engine`, etc. (consistent with the workerd exposure surface). **Hyperdrive is supported** (`connectionString` + host/port/user/password/database; the origin URL is stored in the control plane as a registered resource with **envelope encryption**, resolved by name, ADR-125/129).

**Local connection reuse (ADR-130)**: the platform does not perform pooling; ordinary workerd handlers **cannot** reuse connections across requests (request-scoped I/O), but **Durable Objects can**—store the connection returned by `connect()` in a DO instance field and reuse it for the lifetime of the DO (including pools provided by the driver). Shape:

```js
import { DurableObject } from "cloudflare:workers";
import { connect } from "cloudflare:sockets";
export class DbPool extends DurableObject {
  async fetch(req) {
    if (!this.conn) {                      // One (or N) connections per DO instance
      this.conn = connect("db.internal:5432");
      this.w = this.conn.writable.getWriter();
      this.r = this.conn.readable.getReader();
    }
    // ...use this.w/this.r to run the query protocol, reused across requests...
  }
}
```
Connections follow the DO lifecycle (rebuilt on eviction/redeploy/owner change; `/v1/do/abort` disconnects immediately). By default only public egress is allowed; `CELLHIVE_TENANT_OUTBOUND=public,private` can allow private network source databases (relaxes I-09, an operations decision).

## Assets Service (ADR-069, Implemented)

After the loader resolves the active version, if `assets_sha` is non-empty, **assets take precedence**: path normalization + directory index, content-type by extension, ETag/`If-None-Match`→304, `_headers` (path blocks overlay response headers), `_redirects` (301/302 + `200` rewrite + splat), and on miss **fall back to worker**. Bytes go through the internal cell-agent `GET /v1/internal/asset` (no direct bucket access). **Route configuration (ADR-071)**: `not_found_handling` (`none`/`404-page`→`/404.html` 404/`single-page-application`→`/index.html` 200), `run_worker_first` (bool or list of path prefixes; after worker 404, fall back to assets). CLI `--assets-not-found/--run-worker-first/--run-worker-first-path`. End-to-end tested against real workerd.

## Cron → scheduled() (ADR-070, Implemented)

`Version.Crons` (CLI `--cron`) → `Projection.CronTargets()`; for `KindCron` timers (scope convention `<ns>/__cron__/<worker>`), cell-agent adds `worker`+`bundle_sha` at dispatch time → user-runtime `POST /v1/timers/dispatch` → workerLoader loads → `scheduled(event, env, ctx)`. **Consumer configuration/dead letter (ADR-072)**: `Consumer{Queue,MaxRetries,DeadLetterQueue,MaxBatchSize,MaxConcurrency}`; when the runner reaches the limit, it sends to DLQ (same ns `Send`) or drops with an alert; `max_concurrency` is the upper limit of **concurrently in-flight batches** per queue (default 0/1=serial, ADR-112). CLI `--consumer <queue>[:maxRetries[:dlq[:maxConcurrency]]]`. **Scheduler (ADR-076)**: `internal/cron` materializes slots from 5-field UTC expressions into `KindCron` timers (`CELLHIVE_CRON_INTERVAL` default 30s, idempotent, no catch-up runs).

## To Be Refined

- Consolidated table of REST endpoints/error codes for each binding (currently spread across ADRs);
- service binding: version freezing (ADR-104 pin at deployment) + **target-side allowlist** (ADR-144, `cellhive service-acl`; same ns allowed by default, cross-ns requires authorization; target written as `ns/worker`). Entrypoint-level ACLs are not further subdivided.

## Queue Consumer Dispatch (ADR-067, Implemented)

## Vectorize (ADR-158, Implemented)

Index = **registered resource** (kind `vectorize`) + **immutable configuration** `{dimensions(1..1536), metric(cosine|euclidean|dot-product)}` (stored in the control plane with envelope encryption):

```bash
cellhive vectorize create acme docs --dimensions 768 --metric cosine
cellhive vectorize insert acme docs --file vectors.ndjson     # {"id","values","metadata"?,"namespace"?} one per line
cellhive vectorize query  acme docs --vector "0.1,0.2,..." --top-k 5 --return-values --return-metadata all
cellhive vectorize info   acme docs                           # dims/metric/vectorCount/namespaces/metadata indexes
```

Binding (Workers side, CF-compatible):

```js
export default {
  async fetch(req, env) {
    await env.INDEX.upsert([{ id: "a", values: [...], metadata: { lang: "en" }, namespace: "eu" }]);
    const r = await env.INDEX.query([...], { topK: 5, returnValues: true, returnMetadata: "indexed", filter: { lang: "en" } });
    const byId = await env.INDEX.queryById("a", { topK: 3 });
    const got  = await env.INDEX.getByIds(["a"]);
    const del  = await env.INDEX.deleteByIds(["b"]);
    const d    = await env.INDEX.describe();          // { dimensions, metric, vectorCount, ... }
    const lv   = await env.INDEX.listVectors({ count: 100, cursor });
  },
};
```

- **config**: `vectorize: [{ binding: "INDEX", index_name: "docs" }]` —— **env key = binding name, resource = `index_name`** (scoped token is minted with the index name; validation gate looks up registration by `index_name`); `--vectorize INDEX=docs` (wrangler prefix/`--config` both supported).
- **score semantics (consistent with CF)**: `cosine` = cosine similarity (**larger is closer**); `euclidean` = distance (**smaller is closer**); `dot-product` = inner product (larger is closer). `returnMetadata: "none" | "indexed" | "all"` (legacy boolean `true`→`all`); `topK` defaults to 5, ≤100 without payload, ≤50 with values/metadata.
- **filter**: `$eq/$ne/$in/$nin/$lt/$lte/$gt/$gte`, implicit `$eq`, dot-notated nested paths, implicit AND across multiple keys, string ranges (prefix search). `namespace` is an independent partition (before metadata filtering).
- **Storage and retrieval (ADR-159, CGo + official SQLite `vec1`)**: in the index cell (`<ns>/__vectorize__/<index>`), the `vectors` table is the **source of truth** (float32 `raw` + metadata + namespace), and `vec USING vec1(embedding, ns)` is the derived ANN index. Default is **flat exact scan** (already about 8× faster than the old Go KNN: 20k×256/K=10 about **6.3ms**); `cellhive vectorize rebuild` uses `vec1_train` to build an **IVFADC+OPQ** model (`--buckets/--quantizer/--codesize/--nprobe`), with queries around **0.22ms** (same data, ≈210× vs the old implementation); `cellhive vectorize drop-ann` returns to flat. The `ann` field in `describe`/`stats` shows the current model.
- **Metric/filtering differences**: `dot-product` is **not supported** (`vec1` only has l2/cos), and `create` fails explicitly; namespace is a metadata column in `vec1` and is **pushed down into the index**; other metadata filters are **post-filters** (4× oversampling, not guaranteed to return a full topK—CF filters first); metadata indexes are **not enforced** (catalog retained for CF compatibility; 64B truncation of indexed strings is not simulated).
- **Operations**: `GET /v1/vectorize/stats` (ADR-157 generic stats surface: dims/metric/vector_count/namespaces/metadata_indexes/disk/max_vectors), `GET|POST|DELETE /v1/vectorize/metadata-index(es)`, `cellhive vectorize info|stats`; after `resource delete` revokes the resource, binding calls fail closed (403).
- **dev**: Miniflare only has the config schema (workerd has no vectorize service); when `cellhive dev` encounters vectorize, it **fails explicitly and exits**; for local debugging, use `cellhive vectorize` against a real namespace.

**Per-domain stats `GET /v1/<kind>/stats` (ADR-157)**: all read-only, metadata-only (PRAGMA/stat/schema/bounded lists), and does not decode row data. Common fields come from `cellstore.DiskStats`: `page_size`/`page_count`/`size_bytes`/`freelist_pages`/`freelist_bytes`/`main_bytes`/`wal_bytes`/`total_bytes`/`journal_mode` (WAL is separate because cellstore disables autocheckpoint, so `page_count` underestimates). Per-domain additions:
- **KV**: `expires_indexed`, `expired`, `next_expiry_ms` (`kv_expires` partial index), `rows_estimate` (dbstat leaf-page `ncell`; skip above 20000 pages and return `estimate_note`); `keys=count(*)` is returned only with `?exact=1`.
- **D1**: `tables`/`table_names`/`indexes`/`auto_indexes`/`schema_version`/`user_version`/`sqlite_version`; `?tables=1` appends per-table `pages`/`payload_bytes`/`rows_estimate` (dbstat; skip above the threshold). cellstore internal tables (`kv`/`cell_meta`) and `sqlite_*` are not counted in `tables`.
- **Queue**: `depth`/`visible`/`leased`, `oldest_visible_ms` (lag metric), `max_attempts`, `dead_letter_queue`/`dead_letter_depth`/`dead_letter_visible`/`dead_letter_oldest_visible_ms`, `disk`.
- **R2**: `objects`/`bytes`/`truncated`/`cursor`/`listing_ms` + `multipart_uploads`/`multipart_parts`. **This is a diagnostic approximation from a bounded List** (`limit` defaults to 1000, maximum 10000), not an exact count; the `r2/.mpu/` staging area is not visible to user List operations.
- **Workflow**: `disk` + `instances_estimate` (dbstat); `?exact=1` returns `instances`+`by_status`.
- **Hyperdrive**: `registered`/`has_config` (never returns the origin URL).
- **DO**: `indexed_objects`/`classes`/`workers`/`available` (`available:false` when the object index ADR-109 is disabled).

**Resource revocation (ADR-156)**: `resource delete` deletes the control-plane registration + derived binding rows and writes a tombstone to `revoked_resources`; binding resolution **checks the tombstone first**, so even if that binding is still present in immutable version metadata (or that worker is redeployed), it **remains fail-closed** until `resource create` is run again to unblock it. Data cells (KV entries/queue messages/DO SQLite) are **not deleted**; queue consumers stop because the resource is no longer listed by `ResourcesByKind`.

**Batch shape and per-message results (ADR-154/155)**: the `batch` passed to `queue(batch, env, ctx)` keeps an **array shape** (`length`/indexes, backward-compatible) and additionally provides `batch.messages`, `batch.queue`, `batch.ackAll()`, and `batch.retryAll({delaySeconds})`; each message has `message.ack()` and `message.retry({delaySeconds})`. Messages not explicitly handled are **implicitly acked** on successful return, and requests exceeding `max_retries` are re-enqueued into the DLQ. `message.body` is typed by content-type (JSON → object, text → string, others → byte array). Delayed production: `env.QUEUE.send(body, {delaySeconds})` (not visible before `visible_at_ms`).

cell-agent's `queue.Runner` periodically obtains the polling set from **queue resources registered in the control plane** (`control.ResourcesByKind("queue")`, cold path), `Claim`s a batch (lease) for each queue, and POSTs it to **`/v1/queues/dispatch`** at `CELLHIVE_DISPATCH_URL`; on success it `Ack`s, and on failure it `Retry`s the entire batch (**at-least-once**). The body contains `(namespace, queue, worker, bundle_sha, version, messages)` (worker/bundle/version are resolved by `Projection.QueueTargets()`, ADR-067/112/127). user-runtime directly loads that active version and calls its `queue()` handler. The body does not carry bindings; on load, user-runtime calls `GET /v1/internal/worker/bindings` with `(ns, worker, version)` to fetch that version's binding spec and inject it into env (ADR-128, fail-open), so the env for `queue(batch, env)` is consistent with fetch. Configuration: `CELLHIVE_QUEUE_INTERVAL` (default 1s, 0 disables). **Implemented**: user-runtime `/v1/queues/dispatch` (ADR-067); retry limit + dead letter (`DeadLetterQueue`, ADR-072, `internal/queue/runner.go`).

_Last updated: 2026-09-19_

## Binding implementation (ADR-090, RPC entrypoint env)

Tenant bindings are no longer HTTP facades: the platform worker exports `WorkerEntrypoint` capability classes, and `ctx.exports.X({props})` generates props binding stubs placed into **the env of the loaded tenant worker**.

- **KV / D1 / R2 / Queue**: entrypoint stubs (`workerd/platform/bindings.js`); D1 `prepare()` returns an `RpcTarget` statement, and R2 `get()` returns an `RpcTarget` body. The tenant fetch and its DO's `this.env` and constructor parameter `env` are all natively visible.
- **DO namespace / Workflow**: the CF API requires **synchronous** `idFromName`, which cannot cross RPC → use the generated `bindings-wrapper.js` to patch the importable `env` (building local facades from `CH_FACADE_SPEC`). Both hosts are unified (do-runtime `bindings-wrapper.js`; user-runtime `queue-wrapper.js`/`workflow-wrapper.js`).
- **ServiceBinding**: `env.SVC.fetch()` and `env.SVC.<method>()` (Proxy forwarding) → cell-agent `/v1/service/{fetch,run}` (scope kind=service) → user-runtime `/v1/services/{fetch,run}` → target worker / its named entrypoint (same namespace); `binding.entrypoint` specifies the target entrypoint.
- **R2 list**: `env.BUCKET.list({prefix,limit,cursor,startAfter})` → `/v1/r2/list` returns `{objects,truncated,cursor}` (cursor is exclusive; sizes come from the list response, ADR-145).
- **R2 presign**: `env.BUCKET.createPresignedUrl(key,{expiresIn})` → cell-agent `/v1/r2/presign` → bucket `PresignGet` (self-hosted backend returns a short-lived read URL).
- **Log tail**: loaded worker `log-tail.js` patches `console.*` → the platform-side `LogSink` entrypoint stub (the platform worker holds the `log` role token and transport) POSTs to cell-agent `/v1/internal/logs` → bounded ring buffer (`CELLHIVE_LOG_BUFFER=<entries>:<workers>`) → `cellhive tail --worker`; log tokens are derived from `CELLHIVE_ROOT_KEY` (ADR-137).
- **AI (BYO)**: `env.AI.run(model, inputs)` → `<AI_URL>/chat/completions` (OpenAI-compatible, `AI_KEY` Bearer), returns `{response,model,usage}`; `inputs.messages` or `{prompt}`.