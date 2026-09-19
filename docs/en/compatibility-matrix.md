# Compatibility Matrix

Status: **Supported** (available to regular applications) · **Partial** (with explicit boundaries) · **Rejected** (explicitly rejected at deployment/configuration time) · **Internal** (platform surface, not a tenant API).

## Cloudflare Runtime Surface

| Surface | Status | Notes |
|---|---|---|
| ES module Workers / `fetch` | Supported | `workerLoader` dynamically loads immutable versions |
| WebSocket | Supported | Proxied by user-runtime; 1012 during DO migration/deployment, clients reconnect (CF-compatible); **`env.DO.get().fetch(upgrade request)` connects directly to the owner via `/v1/do/connect` (`{owner,ticket}` query + ticket only valid on this shard, ADR-175)** |
| KV | Supported | Strongly consistent (reads are also forwarded to the owner, ADR-120; cell SQLite); no global edge replication; **`put` accepts string/ArrayBuffer/ArrayBufferView/Blob/ReadableStream (ADR-174)**; **write validation aligned with Cloudflare (ADR-176): key≤512B, metadata≤1KiB, expirationTtl≥60s, expiration must be in the future, value≤25MiB** |
| D1 | Supported | SQL + `batch`/`exec`; single writer; **Sessions/bookmarks explicitly rejected** (`withSession()`/`session()` throw errors, no read replicas, ADR-153); `meta` for `run()/batch()` includes `last_row_id`/`changed_db`, errors mapped to `D1_ERROR` (ADR-163) |
| R2 | Supported | S3-compatible + **presign (`createPresignedUrl`)** + **multipart (ADR-113)** + **cursor-paginated list (ADR-145)**; **full R2Object fields + `put` http/custom metadata/checksums + `head()` (ADR-163)**; **list supports `include` (backfills per-object metadata on demand) and `delimiter`/`delimitedPrefixes` (ADR-168)**; **`R2ObjectBody.body` + `writeHttpMetadata` (ADR-174/175: local Proxy wrapper, metadata written to caller Headers)**; no SSE-C/jurisdiction, no object versioning/conditional put |
| Queues | Partial | At-least-once + DLQ; `max_concurrency` **supported** (ADR-112, per-batch concurrency limit); `contentType=v8` rejected |
| Cron | Supported | Minute-aligned, best-effort, no catch-up runs |
| Durable Objects | Partial | Native facet + synchronous SQL; **bindings inside DO ✅ (ADR-090)**; only same worker class; deployments default to **lazy restart** (configurable via `do_eager_restart`/`session_policy`); WS migration/restart 1012; **DO RPC ✅ (ADR-162)**: `env.NS.get(id|name).method(...)`, tagged JSON + host→facet native JSRPC (≤8 MiB; no functions/stubs/streams); **legacy classes (not `extends DurableObject`) supported via facet wrapping (ADR-174)**; **non-2xx DO `fetch` responses are returned unchanged (status/content-type, ADR-174)** |
| Workflows | Partial | Self-built engine; supports create/get/status/lifecycle/delete/list + `step.do` (including `retries{limit,delay,backoff}`)/`sleep`/`waitForEvent` + `sendEvent`; does not support cross-worker; `locationHint` accepted but ignored |
| ASSETS | Supported | Object storage + versioning; complete asset pipeline |
| AI | Partial (optional) | **BYO OpenAI-compatible endpoint** (`env.AI.run`, `CELLHIVE_AI_URL/_KEY`); no platform-hosted catalog, no workers-ai catalog models |
| Service / Platform bindings | Supported | Version pinning + ACL; **worker↔worker RPC = same-instance native JSRPC (ADR-102)**; **DO RPC = owner-routed JSON+tagged (ADR-162)** |
| Vars / Secrets | Supported | Secrets envelope encryption |
| `nodejs_compat` | Supported | Provided by workerd; must be enabled |
| Cache API | Rejected | No edge cache |
| Vectorize / AI Search / Browser / Email / Analytics Engine | Rejected | Not provided by workerd (Hyperdrive is already supported; see below) |
| Python Workers | Rejected | Not supported |

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

- **Exact list of `compatibility_flags`**: see `internal/wranglercompat.KnownCompatibilityFlags` (mirror of `KNOWN_COMPAT_FLAGS` in `cli/src/validate.ts`); unknown flags fail closed at deployment time with `unknown_flag`. `TestKnownFlagsMatchDevCLI` ensures both sides stay consistent; `TestPinPairsWithCompatibilityDate` ties the workerd pin (`1.20260615.1`) and compatibility upper bound (`2026-06-22`) into a single decision.
- **Framework prebuilt artifact acceptance**: `internal/wrangler TestFrameworkPrebuiltLayouts` covers OpenNext (`.open-next/worker.js` + `.open-next/assets`), SvelteKit (`.svelte-kit/cloudflare/_worker.js` + colocated assets), Astro (`dist/_worker.js/index.js` + `dist/client`): configuration mapping + `bundler.Build` packaging.
- **Out of scope**: see the rationale in the "Rejected" rows (Cache API/browser rendering/Email/Python, etc.); Vectorize is already supported (ADR-158/159).

_Last updated: 2026-09-19_