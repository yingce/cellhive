# Scaling and High Availability

## Layered Forms

| Layer | Component | Form | State |
|---|---|---|---|
| Fixed | `cell-agent` cluster | **≥2 nodes, stable identity, horizontally scalable** | Owns backend A SQLite + control plane |
| Elastic | `do-runtime` nodes | Can be added/removed, **local disk may be temporary** | DO working replicas; authoritative state is in cell-agent |
| Stateless | `user-runtime` | Deployment + HPA | Ingress + execution |
| Ingress | `Traefik` | Stateless replicas | TLS + host-based routing |

## HA Basics

- **cell-agent**: each node writes a `nodes/*` bucket lease (self-registration); owner records use bucket **conditional writes**; epoch fence; takeover occurs when a node lease expires;
- **write acknowledgment**: fleet mode = owner + **≥1 follower fsync** (2 nodes are sufficient); **S3 is not in the ack path**;
- **owner failure**: lease expires → another node takes over via conditional write (epoch+1);
- **do-runtime failure**: liveness is implicitly determined by **replication activity**; takeover candidates use platform service discovery; the DO owner record points to do-runtime.

## Graceful Handoff / Drain

- `cell-agent`: after obtaining the bucket **drain token**, migrates cells in batches (see [`cell-protocol.md`](cell-protocol.md) §7); concurrent shutdowns are serialized by the token;
- `do-runtime`: supervisor first stops new requests → waits for in-flight work → flushes WAL → `release(owner)` → stops workerd;
- `user-runtime`: workerd shuts down gracefully and waits for in-flight work; WS disconnects and clients reconnect.

Recommended `terminationGracePeriodSeconds`: user-runtime 30 / do-runtime 60 / cell-agent 120+.

## Autoscaling

- The node lease `load` exposes signals: `owned_cells`, `resident_cells`, `rss_bytes`, `cpu_percent_x100`, `pressured`, `memory_headroom`, `shed_cells`, `restoring`;
- **cell-agent scale-in floor = 2** (required for fleet proof); scale out based on owned_cells/load;
- `do-runtime` scales based on resident object count and memory; new nodes restore objects from cell-agent (paginated lazy loading);
- **single-node mode is degraded only**: DO is unavailable (no ensemble).

## Hotspots and Balancing

- Cell ownership is based on bucket conditional writes + placement hints (prefer low-load nodes);
- Scope-hash for DO coordination is only **best-effort affinity**, not a hard constraint (as decided in the ADR);
- Background rebalance migrates idle cells (batched, with limits).

## Discovery (Decoupled from the Orchestrator)

- cell-agent peers: bucket `nodes/*` (authoritative);
- callers → cell-agent: service name (built-in DNS in K8s/Compose) or seed list;
- do-runtime → cell-agent: configured endpoint/DNS/LB; candidates use platform discovery (fall back to registration/heartbeat when platform discovery is unavailable);
- See [`decisions.md`](decisions.md) ADR-027 for details.

## Finalized (ADR-151)

**autoscaler (signals, not orchestration)**
- Target: `ceil(owned_cells / CELLHIVE_CELLS_PER_NODE)`, floor `CELLHIVE_AUTOSCALE_MIN` (default 1), ceiling `CELLHIVE_AUTOSCALE_MAX` (0=unlimited); when any peer is `pressured`/`shed_cells>0`, use at least `nodes+1` (pressure takes precedence over the target).
- Evaluation interval `CELLHIVE_AUTOSCALE_INTERVAL` (default 30s); **cooldown** `CELLHIVE_AUTOSCALE_COOLDOWN` (default 5m): action **changes** are suppressed within the window as `hold/cooldown` and return `cooldown_remaining_ms`, preventing signal jitter from causing repeated scale-out/scale-in.
- Actual node additions/removals belong to external orchestration; the core only provides signals + a pluggable `Actuator` (default no-op/log).

**rebalance (batch size and throttling)**
- Throttling = loop interval `CELLHIVE_REBALANCE_INTERVAL` (default 0=off); batch size = `CELLHIVE_REBALANCE_MAX_MOVE` (default 32, maximum number of idle cells released in a single round).
- Only releases **idle** cells (no open handles) owned by this node; epoch/owner fences ensure no data loss.

**multi-AZ / failure domains**
- `CELLHIVE_PLACEMENT_AZ` (this node's AZ; empty=no preference). When set, **follower selection prefers a different AZ** (`selectFollowers`: cross-AZ first, same-AZ fallback, excluding self/expired/no `peer_url`), so a single-AZ failure does not take all replicas.
- Owners are still placed by scope hash/capacity; cross-AZ only affects replication targets and does not change the consistency model (follower fsync proof remains in the same quorum semantics).

_Last updated: 2026-09-14_