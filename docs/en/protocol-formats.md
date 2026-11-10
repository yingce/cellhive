# Wire Formats and Protocol Definitions

This document defines data formats and interfaces across processes/nodes. All integers are **big-endian**, and all strings are UTF-8. `proto_version` is currently `v1` (ADR-034).

## 1. owner record (bucket `cells/<scope>/own.json`)

```json
{ "node": "n1", "role": "cell-agent|do-runtime", "session": "s1",
  "epoch": 1, "expiry": 1730000000000, "address": "10.0.0.1:7001",
  "admin_address": "10.0.0.1:8082", "proto_version": "v1" }
```

- `expiry` (unix ms) is **always ≤ the node lease expiry**;
- Acquire: `If-None-Match:*` (cold) or `If-Match:<etag>` (takeover/renewal).
- `own.json` is the logical owner key. COS/OSS implement conditional writes using native append positions and bounded generations; S3/MinIO control records use conditional PUT/tombstones. The physical encoding is not a plain JSON PUT; see [storage adapters](storage-and-s3.md). The old `owner.json` layout is not read.

## 2. Node lease (bucket `nodes/<node>.json`)

```json
{ "node": "n1", "session": "s1", "advertise": "10.0.0.1:7001",
  "expiry": 1730000000000, "proto_version": "v1",
  "load": { "owned_cells": 12, "resident_cells": 12, "rss_bytes": 123,
            "cpu_percent_x100": 500, "pressured": false, "memory_headroom": 900000,
            "shed_cells": 0, "restoring": 0, "sampled_ms": 1730000000000 } }
```

## 3. node-log (bucket `node-logs/<node>/<session>.json`)

```json
{ "node": "n1", "session": "s1", "epoch": 1,
  "followers": ["n2"], "status": "open|recovering|sealed" }
```

## 4. Native LTX (`cells/<scope>/ltx/e<epoch>/<level-4hex>/<min>-<max>.ltx`)

The only stored format is native celld/Litestream LTX, aligned with pinned celld `90b4301`. There is no format switch, legacy-format compatibility or separate manifest/index. Existing databases migrate via SQLite export/import.

The header is 100 bytes; multi-byte integers are big endian:

| Offset | Bytes | Field |
|---|---|---|
| 0 | 4 | magic `LTX1` |
| 4 | 4 | flags (writer uses standard NoChecksum; file CRC64 remains checked) |
| 8 | 4 | page size |
| 12 | 4 | commit (final database page count) |
| 16 | 8 | min TXID |
| 24 | 8 | max TXID |
| 32 | 8 | timestamp |
| 40 | 8 | pre-apply checksum |
| 48 | 8 | WAL offset |
| 56 | 8 | WAL size |
| 64 | 4 | WAL salt 1 |
| 68 | 4 | WAL salt 2 |
| 72 | 8 | node ID |
| 80 | 20 | reserved |

Epoch and kind are not header fields. Epoch comes from the object directory; **min TXID=1 identifies a snapshot**. A snapshot covers every final image page except SQLite's pending-byte lock page. Deltas retain the final image of each modified page and apply truncation using commit.

File order: 100-byte header → native page frames → six-byte zero page header → embedded page index → eight-byte index length → 16-byte trailer. Sorted page frames use native LZ4 encoding. The index stores varint page-number/offset/frame-length tuples terminated by zero. The trailer contains post-apply checksum and file CRC64. No WAL1/WAL2/WAL3, CIDX/CID2 or private 44-byte wrapper is stored.

Names use fixed 16-digit lowercase hexadecimal `<min>-<max>.ltx`. Only sparse levels `0000`, `0001`, `0002`, `0003`, `0009` are scanned. Ordinary PUT/multipart publishes a complete file and permits same-key idempotent retries; snapshots first complete and fsync a local spool. No snapshot-started/done, epoch-cut, seal, pin, tombstone or custom fence files are created.

A new epoch continues with a zero-page native `[cut+1,cut+1]` file carrying real page size/commit and WAL coordinates. The first application delta starts at cut+2. Recovery walks epochs backwards, accepting only exact predecessor cuts and excluding late predecessor tails crossing that cut.

Independent UTC-aligned 30-second, five-minute and one-hour schedules promote uncovered ranges L0→L1→L2→L3. L9 is a complete-chain snapshot. Compaction is additive and retains sources. Native epoch GC uses upstream stream-level `ltx/retired.json`; see [storage and GC](storage-and-s3.md).

## 5. bundle manifest (inside object storage `bundles/sha256/<aa>/<hash>`)

```json
{ "proto_version": "v1", "sha256": "<bundle-hash>",
  "entry": "index.js",
  "modules": [ { "name": "index.js", "type": "esm", "sha256": "...", "size": 123 } ],
  "compatibility_date": "2026-04-24", "compatibility_flags": [],
  "assets_ref": "<hash>", "bindings_ref": "<control-cell-key>" }
```

- Verify `sha256` for each item before loading modules;
- Version pointer: `deploys/<ns>/<worker>/<version>.json` → `{ bundle_sha256, bindings_ref, created_at }`.

## 6. Route projection (pulled by user-runtime)

```json
{ "projection_version": 42, "generated_ms": 1730000000000,
  "routes": [ { "host": "demo.workers.local", "path_prefix": "/hello-jsonc/",
                "ns": "demo", "worker": "hello-jsonc", "active_version": 3 } ],
  "reserved_ns": ["__system__", "__platform__"] }
```

- Pull-only + 5–10s TTL (ADR-031); `projection_version` increases monotonically.

## 7. Go↔Go internal calls (HTTP, `:7001`)

**No internal gRPC** (ADR-136); the logical operations below are all REST/HTTP endpoints (LTX segments are length-prefixed binary frames):

| Logical operation | Endpoint | Purpose |
|---|---|---|
| append | `POST /v1/peer/append`, `/v1/peer/append_batch` | owner→follower replication (LTX byte frames) |
| tail | `GET /v1/peer/tail` | Pull follower-retained segments during recovery |
| seal | `POST /v1/peer/seal` | recovery: seal follower |
| acquire / release | `POST /v1/internal/{claim,release}` | Ownership and graceful handoff |
| resolve | `GET /v1/internal/resolve` | owner resolution |
| restore | `GET /v1/internal/blob` (paged ranged) | Deliver snapshot+delta for cold activation/takeover |

See [`networking.md`](networking.md) and ADR-042 for the detailed protocol and frame format.

## 8. REST (workerd JS → cell-agent, `:7001`; same interface as external, ADR-030/031)

`/v1/kv/*`, `/v1/d1/*`, `/v1/queue/*`, `/v1/workflow/*`, `/v1/cron/*`; request headers carry `x-cellhive-internal-token` + `x-cellhive-scope` (scope declaration).

SQL capture internal hot path:

```text
POST /v1/internal/commit_binary?scope=<scope>&epoch=<epoch>
Content-Type: application/octet-stream
X-Cellhive-Followers: <comma-separated peer URLs>
Body: raw LTX segment (max 64MiB)
```

The response reuses `{ "mode": "fleet", "acked_by": "..." }`; JSON/base64 `/v1/internal/commit` is retained for compatibility.

## 9. Error codes

| code | Meaning |
|---|---|
| `invalid_scope` | Malformed scope |
| `unauthorized` | Internal token missing/invalid |
| `owner_live` | Another node holds a live lease (owner attached) |
| `claim_race` | Conditional-write race failed (re-resolve) |
| `epoch_mismatch` | epoch has changed (taken over) |
| `result_unknown` | Non-idempotent operation result unknown (**do not replay**) |
| `storage_unavailable` | Object storage unavailable |
| `overloaded` | Overloaded (P1 admission) |

_Last updated: 2026-09-19_