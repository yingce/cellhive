# Links and Protocols

This document centrally defines "which protocol to use, and why" between CellHive components. It expands on [`cell-protocol.md`](cell-protocol.md) §12 and **ADR-026 / ADR-027**; **the gRPC plane has been removed (ADR-136), and internal communication is unified on HTTP**. The goals are: **minimal overhead on hot paths, REST/JSON for workerd(JS)/external interfaces, and REST + binary frame streams for Go↔Go**.

## 0. Principles

1. **Distinguish north-south from east-west traffic**: north-south traffic goes through the operations edge proxy (TLS + host-based routing); east-west traffic goes directly over the private network and does not go through the edge proxy.
2. **Choose protocols by "participant", not by performance**:
   - **Go↔Go** → HTTP/REST (the LTX hot path uses length-prefixed binary frames + HTTP 101 persistent streams, ADR-042);
   - **Any path involving workerd(JS)** → HTTP/REST/JSRPC;
   - **External/tenant-facing** → REST/JSON.
3. **Use frame streams for binary hot paths, never JSON** (to avoid base64 expansion and text parsing).
4. **Minimize hot paths**: hops, serialization, allocations, and round trips (connection reuse, group commit).

## 1. Recommended Summary Table

| # | Link | Endpoints | Frequency | Recommended Protocol | Rationale / Notes |
|---|---|---|---|---|---|
| 1 | Public Internet → Traefik → `user-runtime:8081` | external↔workerd | High | **HTTPS（REST）** | North-south; TLS terminates at Traefik |
| 2 | Public Internet/CLI → Traefik → `cell-agent:8082` | external↔Go | Low | **HTTPS（REST/JSON）** | admin/control plane; token authentication |
| 3 | `user-runtime`(JS) → `cell-agent:7001`（bindings） | JS↔Go | **High (hot)** | **REST/JSON**（long-lived keep-alive connections） | gRPC is not suitable on the JS side; use connection reuse |
| 4 | `user-runtime`(host adapter) → owner `do-runtime:8788`（DO/WS） | JS↔JS | **High (hot)** | **HTTP / JSRPC** | workerd-to-workerd calls; WS uses upgrade |
| 5 | `cell-agent` → `user-runtime:8088`（scheduled/queue/workflow dispatch） | Go↔JS | Medium | **REST/JSON** | Target is workerd |
| 6 | `cell-agent` ↔ `cell-agent`（control: acquire/release/seal/tail/recovery/claim） | Go↔Go | Low–Medium | **REST/JSON（HTTP/1.1）** | Connection pool reuse; no gRPC (ADR-136) |
| 7 | `cell-agent` ↔ `cell-agent`（**LTX append/tail hot path**） | Go↔Go | **High (hot)** | **Length-prefixed binary frames + HTTP 101 persistent stream**（failure fallback to `/append_batch`; ADR-042） | Avoid JSON expansion; multiple lanes + multiple in-flight batches |
| 8 | do-runtime supervisor(Go) → `cell-agent:7001`（WAL reporting / claim / restore） | Go↔Go | High (WAL) / Low | **REST/JSON**（WAL segments are binary frames） | Same HTTP plane as peers |
| 9 | Inside do-runtime: supervisor ↔ workerd | Same Pod | High | **Local**（loopback HTTP / JSRPC） | Not a network protocol; WAL capture is **file reading** |
| 10 | `cell-agent` → object storage | Go↔S3 | Medium | **S3 API（HTTPS）** | Only bucket accessor; in fleet mode, background batching, not on the ack path |
| 11 | Control plane → `user-runtime`（route projection delivery） | Go↔JS | Low | **REST/JSON**（pull-only + 5–10s TTL, no push） | See ADR-031 |
| 12 | `cell-agent` ↔ bucket（owner records / node leases / conditional writes） | Go↔S3 | Low–Medium | **S3 API（HTTPS）** | Point lookups, LIST prohibited; not peer traffic |
| 13 | Internal north-south（Traefik → backend） | Traefik↔backend | High | **HTTP(S)**（ClusterIP / service name） | Resolved by service name/DNS |

**Rule**: Internally, **everything is HTTP** (`#3`–`#8`, `#11`; `#7` is a binary frame stream); `#1/#2` are north-south HTTPS.

### Port Allocation（M-02 Final）

| Port | Protocol | Purpose |
|---|---|---|
| `:7001` | **REST/JSON + binary frames** | All internal traffic: shared by Go↔Go (peer LTX, DO WAL/claim/restore) and workerd(JS) bindings |
| `:8082` | REST/JSON | External admin/control plane (via edge proxy) |
| `:8081` | HTTP | user-runtime public loader (via edge proxy) |
| `:8088` | HTTP | user-runtime internal privileged dispatch (private network only) |
| `:8788` | HTTP | do-runtime private network (owner calls/WS) |

## 2. Hot Path Focus: DO Writes

```
workerd submit (synchronous SQL)
  → supervisor captures WAL (file reading, local)
  → reports raw HTTP byte frames to cell-agent (may cross nodes)
  → cell-agent writes e<epoch> prefix + durability proof (fleet ensemble)
  → host adapter output gate releases
  → return
```

Hot path optimizations:

- Local capture, **connection reuse**, **group commit** (combine multiple operations into one append/fsync);
- owner and follower **same-AZ affinity**;
- **raw byte frames** instead of JSON/protobuf;
- bucket upload is **background asynchronous** (not on the ack path in fleet mode).

## 3. Alternatives and Evolution

| Alternative | Description |
|---|---|
| **Connect(Buf)** | Unified definition for the same protobuf services; can unify internal and external APIs, but adds toolchain complexity and is not introduced by default |
| **HTTP/2** | Connection multiplexing; currently using HTTP/1.1 + connection pools + multiple lanes, with no evidence that it is a bottleneck |
| **Native gRPC** | Explicitly **not used** (ADR-136): Go↔Go also has no gRPC dependency |
| **Raw TCP frame stream / QUIC** | Faster form for the LTX hot path; interfaces remain unchanged, and benchmarks decide whether to move down the stack |

## 4. TBD

- Whether the LTX hot path should move down from HTTP 101 persistent streams to raw TCP frame streams (`#7`, interface unchanged) — decided by benchmarks.
- Route projection delivery is already **pull-only + ETag/revision** (ADR-115/116); push is not planned.

## 5. P0 Validation

- `#7/#8`: peer append round-trip p50/p99; group commit batching ratio; hedge hit/ineffective ratio (adaptive hedge has been implemented, off by default, ADR-164).
- `#3`: binding call p50/p99 (whether connection reuse is sufficient).
- `#4`: DO call/WS proxy latency.
- `#10`: whether bucket upload is truly not on the ack path.

_Last updated: 2026-09-14_