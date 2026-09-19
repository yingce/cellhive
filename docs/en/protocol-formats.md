# Wire Formats and Protocol Definitions

This document defines data formats and interfaces across processes/nodes. All integers are **big-endian**, and all strings are UTF-8. `proto_version` is currently `v1` (ADR-034).

## 1. owner record (bucket `cells/<scope>/owner.json`)

```json
{ "node": "n1", "role": "cell-agent|do-runtime", "session": "s1",
  "epoch": 1, "expiry": 1730000000000, "address": "10.0.0.1:7001",
  "proto_version": "v1" }
```

- `expiry` (unix ms) is **always ≤ the node lease expiry**;
- Acquire: `If-None-Match:*` (cold) or `If-Match:<etag>` (takeover/renewal).

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

## 4. LTX Segment (`cells/<scope>/ltx/e<epoch>/<seq>.ltx`)

Used to replicate SQLite commits. The P0 spike uses a **frame stream + segment header**; production can align with `superfly/ltx`.

Segment header (fixed 44 bytes):

| Offset | Length | Field |
|---|---|---|
| 0 | 4 | magic `"LTX1"` |
| 4 | 1 | `version` (segment format version) |
| 5 | 1 | `kind` (0=delta, 1=snapshot, 2=link) |
| 6 | 2 | flags (reserved; interpreted only for `kind=snapshot`: bit15=paged, bit0..14=part index) |
| 8 | 8 | `epoch` |
| 16 | 8 | `start_txid` |
| 24 | 8 | `end_txid` |
| 32 | 8 | payload length |
| 40 | 4 | CRC32C(payload) |

- Segment filename `<start_txid>-<end_txid>.ltx` (or `<txid>.snapshot`);
- `kind=2 link` is used for **snapshot→delta stitching** (checkpoint alignment);
- Upload: `Put` (temporary key → atomic rename), **no List on the hot path**.

SQLite WAL payload (`WAL1`) for `kind=delta`:

| Field | Encoding |
|---|---|
| magic | 4 bytes `"WAL1"` |
| page_size | u32 BE |
| transaction_count | u32 BE |
| Each transaction | frame_count u32 BE |
| Each frame | page_no u32 BE + db_size u32 BE + `page_size` bytes |

The last frame of each transaction has `db_size != 0` (SQLite WAL commit marker). The LTX header’s `start_txid..end_txid` covers the transactions in the payload in order.

The SQL capture production path switches to **`WAL2` page-map**: within a capture chunk, only the **last write** per page number is kept, and the payload contains only the final page image, without preserving intermediate transaction versions.

| Field | Encoding |
|---|---|
| magic | 4 bytes `"WAL2"` |
| page_size | u32 BE |
| commit | u32 BE (final DB page count)|
| tx_count | u32 BE (number of application transactions covered by this chunk)|
| page_count | u32 BE |
| Each page | page_no u32 BE + `page_size` bytes (ascending by page_no, unique)|

**`WAL3` (page-map v2, ADR-160)**: snapshot/compaction now writes `WAL3`—each page frame is first compressed with an **LZ4 block** (stored raw if it does not shrink), and the **frame position for each page** is placed in the payload header index, so paged reads can fetch and decompress a single page directly with one ranged read (without scanning the payload):

| Field | Encoding |
|---|---|
| magic | 4 bytes `"WAL3"` |
| page_size / commit / tx_count / page_count | u32 BE ×4 |
| reserved | u32 BE |
| Index (12B per page) | page_no u32 BE \| off u32 BE (offset within payload) \| stored u32 BE (low 31 bits=frame length, bit31=1 means LZ4)|
| frames | One per page (raw `page_size` or LZ4 block; always `page_size` after decompression)|

`WAL2` (v1, fixed frames, no compression) can still be decoded (backward compatibility); `PageLocs`/`FrameData` handle both uniformly.

**Paged snapshots (large cells)**: a complete snapshot (pages `1..commit`) may be split into multiple `kind=snapshot` segments that **share the same txid watermark**. Each segment payload is still `WAL2` and carries the same `commit`; page ranges do not overlap and are in ascending page-number order. Shard information is encoded in the **LTX header’s `flags`** (interpreted only for snapshots):

- bit15 = paged marker; bit0..14 = 0-based part index;
- Segment name `<end_txid>.p<part>.snapshot` (single-segment snapshots that fit remain `<end_txid>.snapshot`, `flags=0`, backward compatible).

restore selects the snapshot with the **largest watermark**, merges all of its parts, and then applies deltas by txid. The default per-segment budget is `ltx.DefaultSnapshotPartBytes` (8 MiB), which must be smaller than the cell-agent `maxSegmentBytes` (64 MiB).

**L1 compaction (large-cell takeover optimization)**: `cells/<scope>/ltx/e<epoch>/L1/` stores folded snapshot parts, and `L1/manifest.json` points to the current L1:

| Field | Description |
|---|---|
| `min_txid` / `max_txid` | txid range covered by this L1 |
| `objects` | L1 snapshot part object keys (in part order) |
| `commit` / `page_size` | Final page count / page size of the folded snapshot |

Takeover restore: when a manifest exists, read only its `objects` + L0 deltas with `txid > max_txid`; otherwise read the full L0 chain. L0 segments are not deleted for now (`Bucket` has no Delete interface) and are superseded by the manifest pointer.

`WAL1` is retained for scenarios that require transaction boundaries and for compatible decoding. Delta payloads are currently still uncompressed (small; see known-issues).

**L1 page index (`L1/index.bin`, ADR-160)**: `CID2` appends the **12B frame position per page** after the v1 page-number table (page_no u32 \| off u32 \| stored u32, bit31=LZ4), so cold start fetches only the pages it needs; `CIDX` (v1, page numbers only) remains readable.

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