# Compatibility Matrix

Status: **Supported** (available to regular applications) · **Partial** (with explicit boundaries) · **Rejected** (explicitly rejected at deployment/configuration time) · **Internal** (platform surface, not a tenant API).

## Cloudflare Runtime Surface

| Surface | Status | Notes |
|---|---|---|
| ES module Workers / `fetch` | Supported | `workerLoader` dynamically loads immutable versions |
| WebSocket | Supported | Proxied by user-runtime; 1012 during DO migration/deployment, clients reconnect (CF-compatible); **`env.DO.get().fetch(upgrade request)` connects directly to the owner via `/v1/do/connect` (`{owner,ticket}` query + ticket only valid on this shard, ADR-175)** |
| KV | Supported | Strongly consistent (reads are also forwarded to the owner, ADR-120; cell SQLite); no global edge replication; **`put` accepts string/ArrayBuffer/ArrayBufferView/Blob/ReadableStream (ADR-174)**; `get`/`getWithMetadata` accept string types and `{type:"text"|"json"|"arrayBuffer"}`; `cacheTtl` is accepted but has no edge-cache effect; **write validation: key≤512B, metadata≤1KiB, absolute expiration must be in the future, value≤25MiB (ADR-176); `expirationTtl` is validated at ≥60s** |
| Wrangler Cloudflare API resources | Partial | Cloudflare shaped KV/D1/R2/Queues `create/list/delete`, Worker secrets, and routes are available for Wrangler 4.133.0; resources are explicitly created before deploy and IDs are referenced by bindings; deploy never implicitly provisions production resources |
| Wrangler Assets / Workflows API | Partial | The assets upload session + JWT single-file upload flow and Workflow create/list/get/delete are mapped; advanced Workflow limits/concurrency/schedules are explicitly rejected |
| D1 | Supported | SQL + `prepare`/`bind`/`all`/`first`/`run`/`raw`/`batch`/`exec`; `prepare()`/`bind()` synchronously return local statement objects that ORMs can pass to `batch()`; `ArrayBuffer`/typed arrays bind and round-trip as BLOBs; single writer; **Sessions/bookmarks and transactions explicitly rejected** (`withSession()`/`session()` throw, `BEGIN` is unsupported, no read replicas, ADR-153); `meta` includes `changes`/`last_row_id`/`changed_db`, errors map to `D1_ERROR` (ADR-163) |
| R2 | Supported | S3-compatible + **presign (`createPresignedUrl`)** + **multipart (ADR-113)** + **cursor-paginated list (ADR-145)**; **full R2Object fields + `put` http/custom metadata/checksums + `head()` (ADR-163)**; **list supports `include` (backfills per-object metadata on demand) and `delimiter`/`delimitedPrefixes` (ADR-168)**; **`R2ObjectBody.body` + `writeHttpMetadata` (ADR-174/175: local Proxy wrapper, metadata written to caller Headers)**; `onlyIf` supports exists/etagMatches/etagDoesNotMatch conditional reads/writes; no SSE-C/jurisdiction or object versioning |
| Queues | Partial | At-least-once + DLQ; `send()`/`sendBatch()` with atomic 1..100 batches, JSON/text/bytes/v8 content types, metadata, enqueue timestamps, idempotency conflicts, 128 KiB body limit, delay validation, and supported `max_concurrency`. `max_batch_timeout`, cross-batch ordering, and some retry/DLQ edge semantics remain partial. |
| Cron | Supported | UTC five-field numeric cron (lists/ranges/steps, Vixie DOM/DOW OR); minute-aligned, best-effort, no catch-up runs; scheduler scans every 30s by default, so exact-once and exact-time delivery are not guaranteed |
| Durable Objects | Partial | Native facet + synchronous SQL; **bindings inside DO ✅ (ADR-090)**; only same worker class; deployments default to **lazy restart** (configurable via `do_eager_restart`/`session_policy`); WS migration/restart 1012; **DO RPC ✅ (ADR-162)**: `env.NS.get(id|name).method(...)`, tagged JSON + host→facet native JSRPC (≤8 MiB; no functions/stubs/streams); **legacy classes (not `extends DurableObject`) supported via facet wrapping (ADR-174)**; **non-2xx DO `fetch` responses are returned unchanged (status/content-type, ADR-174)** |
| Workflows | Partial | Self-built engine; supports create/createBatch/get/status/lifecycle/delete/list + `step.do` (including `retries{limit,delay,backoff}`)/`sleep`/`waitForEvent` + `sendEvent`; total create/createBatch payload ≤1 MiB, batch size 1..100. No cross-worker instances, step-history listing, or progress callbacks; `locationHint` is accepted but ignored (no placement effect). Duration strings, Date/time sleepUntil, wait timeout errors, step.do timeout, and creation-time worker/bundle/version pinning are supported; sensitive and some lifecycle options remain partial. |
| ASSETS | Supported | Object storage + versioning; complete asset pipeline |
| AI | Partial (optional) | **BYO OpenAI-compatible endpoint** (`env.AI.run`, `CELLHIVE_AI_URL/_KEY`); no platform-hosted catalog, no workers-ai catalog models |
| Service / Platform bindings | Supported | Version pinning + ACL; **worker↔worker RPC = same-instance native JSRPC (ADR-102)**; **DO RPC = owner-routed JSON+tagged (ADR-162)** |
| Vars / Secrets | Supported | Vars, managed secrets, and user-named binding stubs enter tenant env; tenant env has zero platform keys and no reserved prefixes (ADR-185). Secrets are envelope-encrypted in the control cell and decrypted by cell-agent for Worker/DO runtime injection; a same-named secret overrides a var |
| Platform capture of tenant `console.*` | Supported | Host-bound native Tail Fetchers capture console output and exceptions from Workers, service targets, and DO facets into the ring and optional OTLP by default; identity and log credentials stay outside tenant env (ADR-185). Set runtime-level `CELLHIVE_TENANT_LOG_CAPTURE=off` to disable capture and its Tail overhead (ADR-192) |
| `nodejs_compat` | Supported | Provided by workerd; must be enabled |
| Cache API | Rejected | No edge cache |
| AI Search / Browser / Email / Analytics Engine | Rejected | Not provided by workerd; Vectorize is supplied by CellHive's SQLite vec1 facade (see below, ADR-158/159), while Hyperdrive is supported |
| Python Workers | Rejected | Not supported |

### Queue and Workflow call boundaries

These describe tenant runtime behavior. Parsing `queues.producers/consumers` or `workflows` configuration does not imply full Cloudflare API compatibility.

| Call/configuration | Current CellHive behavior | Migration guidance |
|---|---|---|
| Queue `send()` body format | Supports `json`, `text`, `bytes`, and tagged `v8`; consumer receives the enum and decodes the body. | Keep payloads within 128 KiB and use the declared content type. |
| Queue `sendBatch()` | Atomic 1..100 messages; returns IDs in input order, with metadata, delay and idempotency per message. | Handle explicit idempotency conflicts. |
| Queue size/delay | Body limit 128 KiB; `delaySeconds` is validated to 0..86400. | |
| Queue consumer settings | `max_batch_size` and `max_concurrency` work. `max_batch_timeout` is parsed/stored but not yet consumed; cross-batch order and some `max_retries=0`/DLQ edges remain partial. | Do not rely on timeout-based batching; set explicit retry/DLQ policy. |
| Queue message metadata | Enqueue timestamp and metadata are preserved; cross-batch order is not guaranteed. | Use an application ordering key when required. |
| Workflow `step.sleep/sleepUntil` | Supports numeric and common duration strings plus Date/time values. | |
| Workflow `step.waitForEvent` | Supports numeric/duration timeout and throws `WorkflowTimeoutError`; event payload remains the stored JSON object. | |
| Workflow step/instance options | `step.do` timeout is supported; `sensitive`, `restart({from})`, `terminate(options)`, subscribe, step-history listing, and progress stream remain partial or unsupported. | Check status before relying on lifecycle details. |
| Workflow status/version | `status()` maps sleeping to waiting, UTF-8 output is preserved, and instances retain creation-time worker/bundle/version targets on redispatch. | |

## Wrangler Configuration Surface

| Configuration | Status |
|---|---|
| `name` / `main` / `compatibility_date` / `compatibility_flags` | Supported |
| `vars` / `secrets`(declarations) | Supported |
| `kv_namespaces` / `d1_databases` / `r2_buckets` | Supported |
| `queues.producers` / `queues.consumers` | Supported (including `max_concurrency`, ADR-112) |
| `workflows` / `triggers.crons` | Supported |
| `services` | Supported |
| `assets` (complete pipeline) | Supported |
| `durable_objects` / `migrations` / `exports` | Supported (same worker): `tag`/`new_classes`/`new_sqlite_classes`/`renamed_classes`/`deleted_classes`/`transferred_classes` (**cross-worker transfer (`script_name`) rejected**); `enable_ctx_exports` flag supports `ctx.exports` |
| `workers_dev` / `routes` / `custom_domain` | Partial: `workers_dev`→namespace path; custom domains optional; `preview_urls` rejected |
| `env.<name>` | Supported (independent app/ns per environment) |
| Automatic provisioning (create without id) | Rejected (must explicitly create) |
| **Hyperdrive** | Supported | Bound as `connectionString` (+ host/port/user/password/database); origin URL stored as an **envelope-encrypted registered resource** (`resource create --connection-string`), `--hyperdrive NAME=<resource>` / `wrangler.jsonc hyperdrive[{binding,id}]` resolved by name (ADR-129); **the platform does not provide connection pooling**: local reuse = holding a `cloudflare:sockets` connection inside **your own Durable Object** (reused for the DO lifetime; regular workers cannot reuse across requests, as empirically shown in ADR-130); shared pools require a self-managed pgbouncer-style protocol-terminating proxy; private-subnet source databases are subject to tenant public-only egress restrictions; dev uses Miniflare `hyperdrives` (`localConnectionString`, ADR-125/129)|
| **Vectorize** | Supported (ADR-158/159): registered resource (`--dimensions`/`--metric`, immutable) + facade `env.<BINDING>` (`insert/upsert/query/queryById/getByIds/deleteByIds/describe`); storage/retrieval uses SQLite official **vec1** (flat exact by default, `vectorize rebuild` builds IVFADC/OPQ ANN); **`dot-product` is not supported** (cosine/euclidean only); metadata filtering is post-filtering; resource = `index_name`; dev not supported (Miniflare has no implementation, explicit error) |
| `ai_search*` / `dispatch_namespaces` / `secrets_store_secrets` / `send_email` / `browser` / `images` / `containers` / `flagship` / `pipelines` / `vpc` / `placement` / `site` | Rejected |
| Unknown fields | Rejected (explicit error) |

## Packaging

- Go + esbuild (no Node); custom artifact format (module manifest + assets), no goal of matching Wrangler output;
- Use **a specific Wrangler version as the semantic baseline** for offline contract tests;
- Prioritize support for framework/Vite prebuilt artifacts (Next/OpenNext, SvelteKit, Astro, etc.).

## Finalized (ADR-153)

- **Exact `compatibility_flags` list**: authority is `internal/workerdcompat/manifest.json`, generated from a fixed upstream revision. Go's `KnownCompatibilityFlags` and Bun's `workerd-compat.generated.ts` both consume it. Unknown/experimental flags fail closed at deployment time; contract tests and the real-binary probe bind workerd pin `1.20260916.1`, compatibility ceiling `2026-09-23`, and the allowed list into one decision.
- **Framework prebuilt artifact acceptance**: `internal/wrangler TestFrameworkPrebuiltLayouts` covers OpenNext (`.open-next/worker.js` + `.open-next/assets`), SvelteKit (`.svelte-kit/cloudflare/_worker.js` + colocated assets), Astro (`dist/_worker.js/index.js` + `dist/client`): configuration mapping + `bundler.Build` packaging.
- **Out of scope**: see the rationale in the "Rejected" rows (Cache API/browser rendering/Email/Python, etc.); Vectorize is already supported (ADR-158/159).

_Last updated: 2026-09-23_
