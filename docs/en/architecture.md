# Architecture Overview

## Positioning

CellHive is a self-hosted **Cloudflare Workers-compatible** runtime for teams that need data sovereignty/private deployment while continuing to use the Workers programming model and the `wrangler` workflow.

Design tenets:

1. **Do not modify workerd.** The compute layer uses stock workerd and only uses it through its public capabilities (`workerLoader`, bindings, capnp configuration, process flags).
2. **All state is a cell.** Each state unit is a named SQLite database; S3-compatible object storage is the long-term authority; bucket conditional writes determine the unique owner; no consensus service is required.
3. **Compute and state are layered.** stock workerd is responsible for execution; all persistent data goes through `cell-agent`.
4. **Fixed state layer + elastic execution layer.** `cell-agent` is a fixed cluster (nodes with identities); `do-runtime` is a distributed elastic execution layer, and its local disk may be temporary.
5. **Portable.** The runtime does not depend on privileges or FUSE; by default it can run on managed/restricted Kubernetes.

## Non-goals

- Global edge network, cross-region replication, Cloudflare account API compatibility;
- Modifying workerd, or introducing a second JS engine;
- Depending on NATS / kvrocks / Valkey / etcd / APISIX;
- A standalone gateway component (ingress responsibilities are handled by the operations edge proxy + user-runtime; see ADR-017; the platform does not distribute edge configuration, ADR-132);
- Supporting CF capabilities that workerd itself does not provide (Python Workers, Cache API, Vectorize, Hyperdrive, Browser Rendering, Email Workers, etc.).

## Component List

| Component | Implementation | Port | Role | Stateful | Requires custom PID1 supervisor |
|---|---|---|---|---|---|
| Edge proxy (Traefik/nginx/cloud LB) | Operations ingress (not an in-platform component) | 80/443 | TLS termination, host-based routing (tenant vs admin); **static configuration** (ADR-132) | No | No |
| `user-runtime` | workerd (`cmd/user-runtime`) | :8081 public / :8088 internal privileged | Worker routing/version resolution/header sanitization + tenant Worker loading and execution. **Implemented**: `:8081` public loader (route projection fetch + version bundle loading + facade env + header sanitization, **ADR-068**) and `:8088` internal dispatch (queue/scheduled handler; ADR-067/070), with static assets on the public side (ADR-069) | No | No |
| `do-runtime` | workerd (`cmd/do-runtime`) + Go supervisor (`cmd/do-supervisor`) | :8788 | Native Durable Object execution; **distributed and elastic**. **Implemented**: host-actor skeleton (facets + localDisk, ADR-077) + ownership/fencing/drain/residency (ADR-078) + supervisor capture/output gate (ADR-083) + alarm (ADR-079) + WS 1012/cross-node forwarding (ADR-080/084) + cross-node cold activation/per-object recovery (ADR-084) + bindings inside DO (ADR-090) + session policy (ADR-107) + owner epoch fencing/configurable lease/object index (ADR-108/109) | Local disk may be temporary (working copy) | **Yes** |
| `cell-agent` | Go (fixed cluster) | :7001 internal REST (Go↔Go also uses this port, ADR-136) / :8082 admin | cell data + replication + leases + control plane + owner resolution + timer dispatch + **sole bucket credentials** | Yes (owns SQLite itself) | No (handles drain itself) |
| Object storage | S3-compatible object storage | — | Authoritative state + code + assets | Yes (external) | — |
| `cellhive` CLI | Go | — | Packaging/deployment/operations | No | — |

> There is no `gateway`, `cell-router` (changed to a library), standalone `control`, standalone `scheduler`, `d1-runtime`, or `kv-runtime`.

## Topology

```
Public users ──TLS──▶ Traefik ──host routing──┬─▶ user-runtime :8081 (Worker routing/version/execution)
                                               └─▶ cell-agent :8082 (admin/control plane, token auth)

Internal (private network + internal token, **direct connection, not through Traefik**)
  user-runtime ─binding/scheduled/queue─▶ cell-agent :7001 (REST/JSON)
  cell-agent ─dispatch─▶ user-runtime :8088
  do-runtime ◀─owner routing/WS─ user-runtime (host adapter → cell-agent resolves owner)
  do-runtime ──WAL reporting/claim/recovery──▶ cell-agent :7001 (direct, HTTP)
  cell-agent ↔ cell-agent (ensemble/acquire/release, direct, HTTP :7001)
  cell-agent ──only──▶ object storage

Fixed: cell-agent cluster (≥2)   Elastic: do-runtime nodes (can be added/removed, temporary disk)

> Traefik only handles **north-south** traffic: public TLS + host routing (tenant → user-runtime :8081; admin/CLI → cell-agent :8082).
> All **east-west/internal** calls use **direct private-network connections and do not go through Traefik**; `cell-agent :7001` is never exposed externally.
```

## Layering Diagram

```
┌───────────────────────────────────────────────────────────────┐
│ Access layer   Traefik (TLS + host routing)                   │
├───────────────────────────────────────────────────────────────┤
│ Compute layer  user-runtime (workerd)  ingress/routing/version resolution/execution │
│                do-runtime   (workerd native DO + supervisor)  │
│                · host adapter: env.KV/DB/DO/Queue/... → private calls │
├───────────────────────────────────────────────────────────────┤
│ State/control  cell-agent (Go, fixed cluster)                 │
│                · cell storage/replication/leases/epoch · owner resolution library · timer dispatch │
│                · control plane (:8082) · sole object-storage credentials and data endpoints │
├───────────────────────────────────────────────────────────────┤
│ Authoritative storage  S3-compatible object storage           │
└───────────────────────────────────────────────────────────────┘
```

## Main Data Flows

### Normal fetch (no standalone gateway)

```
Client → Traefik(TLS, host) → user-runtime(:8081)
   loader: host/path → ns/worker; route → active version (read cell-agent route projection + cache)
         → sanitize trusted headers + generate request-id + intercept reserved namespaces
         → workerLoader loads immutable bundle by <ns>:<worker>:<version> → fetch()
```

### Binding calls (KV/D1/Queue/Workflows/Cron)

```
Worker(env.BINDING) → host adapter (binding-scoped, immutable props)
       → cell-agent(:7001) → SQLite reads/writes on owner cell + LTX replication to object storage
       → return after persistence proof
```

### Durable Object (cross-node)

```
Worker → env.NS.get(id).fetch() → host adapter
       → cell-agent resolves (ns,class,objectId) → owner **do-runtime node** address + epoch
       → owner do-runtime: workerd native facet execution (synchronous SQL), writes local working copy
       → supervisor captures WAL → reports to local/remote cell-agent
       → cell-agent: writes e<epoch> prefix + persistence proof → output gate → return
```

### Timers (cron / queue delay / workflow sleep / DO alarm)

All are unified as timers: due entries are stored in the SQLite of their owning cell; each `cell-agent` maintains local due entries for the timer cells it owns; a single fleet waker (bucket lease) covers due items whose owner has died. **No bucket scanning.**

## Responsibility Boundaries (`cell-agent` vs `do-runtime`)

| Dimension | `cell-agent` (fixed) | `do-runtime` (elastic) |
|---|---|---|
| Owns SQLite for backend A | ✅ | ❌ |
| Owns local working copy for DO | ❌ | ✅ (temporary disk, authority in cell-agent) |
| Lease/owner coordination, epoch, replication, RPO proof | ✅ | ❌ |
| Control plane / auth / identity / keys | ✅ (:8082) | ❌ |
| Owner resolution library + endpoint | ✅ Provides | Uses |
| Timer dispatch / waker | ✅ | ❌ |
| Object storage credentials | ✅ Sole holder | ❌ (via cell-agent) |
| Listens on | :7001 REST (including Go↔Go) / :8082 admin | :8788 private network |

## ownership Rules

> **owner = the node holding that cell's "working SQLite".**

| backend | owner node | recorder/coordinator |
|---|---|---|
| A (KV/D1/Queue/Workflows/Cron) | `cell-agent` node | `cell-agent` |
| B (Durable Object) | `do-runtime` node | `cell-agent` coordinates and writes owner records (including execution address + epoch) |

## Discovery Mechanism (Summary)

| Target | Mechanism |
|---|---|
| `cell-agent` cluster | Each node writes a `nodes/<node>.json` bucket lease (self-registration, holds bucket credentials) |
| Class A cell owner | Bucket owner record + local cache (point lookup, no LIST) |
| DO owner | Owner record (points to do-runtime address + epoch), written by cell-agent |
| `user-runtime` dispatch | Logical service name (mesh), any healthy replica |
| `do-runtime` liveness/candidates | **No dedicated heartbeat**: liveness is inferred implicitly from replication activity; takeover candidates use platform service discovery (see ADR-025) |

## Trust Boundaries

- `Traefik` only performs TLS and host-based routing, and does not perform business auth; admin host → `cell-agent :8082`.
- Tenant Workers loaded by `user-runtime`: **public egress only**, cannot see internal Fetchers/credentials; the platform loader runs before tenant modules and is responsible for routing and header sanitization.
- `do-runtime` is private-network only; it does not hold object-storage credentials.
- `cell-agent`: `:7001` internal data/resolution (shared by Go↔Go and workerd bindings) is **private-network only**; `:8082` admin goes through the edge, credentials = static operations token or OIDC/JWT (`cellhive_ns` restricts ns, ADR-131); data-plane paths cannot reach control-plane handlers.
- **Tenant isolation**: the `globalOutbound` of a tenant loaded worker = **public Internet only** (excluding RFC1918 / cell-agent); the internal token exists only in the host adapter and **never enters the tenant env**; each worker is configured with `limits` (cpu/subrequests) and a V8 heap limit. See the planned `security.md` for details.
- Tenant secrets: the control-plane cell stores envelope ciphertext; the root key is outside the cell; at load time, `cell-agent` decrypts and injects into `env`, and plaintext only enters the load envelope + workerd env.
- All internal calls carry the shared internal token; none are exposed to the public Internet.

## State Ownership

| State | Authoritative location |
|---|---|
| Worker bundle / version metadata | Object storage (content-addressed) + control-plane cell |
| KV / D1 / Queue / Workflow data | Each respective cell's SQLite, replicated to object storage via cell-agent |
| DO data | workerd actor SQLite (working copy) → replicated to object storage via cell-agent |
| cell owner / node lease | Object storage (conditional writes) |
| Route / version / identity / key metadata | Control-plane cell (**single database**, ADR-117; app is the `ns` column in the table) |
| R2 / ASSETS objects | Object storage |

## Relationship to Existing Projects

CellHive's design references multiple open-source projects in the same domain. What is borrowed is **design and contracts**, not implementation:

| Project | Borrowed | Not borrowed |
|---|---|---|
| **celld** (`denoland/celld`) | cell=SQLite, bucket conditional-write owner, epoch fence, LTX, RPO=0, persistent addressing, node lease scaling signals, graceful handoff, "indexes are non-authoritative and repairable" | Its V8-based execution engine; we do not fork/modify its binary (we use stock workerd) |
| **wdl** (`wdl-dev/wdl`) | `workerLoader` multi-tenant dynamic loading, binding host adapter, versioning/rollback, dual-socket privilege separation, DO host actor + facets, alarm shim, module-based documentation organization | Its Redis/Valkey state model, standalone gateway, standalone scheduler/workflows service |
| **LiteFS / superfly/ltx** | Reference for Go-side SQLite replication and LTX format implementation | — |

For the complete borrowed/not-borrowed notes and acknowledgements, see [`acknowledgements.md`](acknowledgements.md).

## Mapping Between Services and Supervisors

| Service | Wraps workerd | Has state leases/replication | Requires custom PID1 |
|---|---|---|---|
| `user-runtime` | Yes (workerd is PID1) | No | No |
| `do-runtime` | Yes (Go starts workerd as a child process) | Yes (workerd owns the working copy) | **Yes** |
| `cell-agent` | No (single Go process) | Yes (owns SQLite itself) | No |

`do-runtime` requires a custom PID1 because workerd owns the working SQLite, and shutdown must "drain/replicate/release leases first, then stop workerd"; cross-process ordering can only be guaranteed by the parent process (see [`durable-objects.md`](durable-objects.md) for details).

## SIGTERM and Drain Order (I-06 fix)

| Service | SIGTERM behavior | Timeout recommendation |
|---|---|---|
| `user-runtime` | workerd stops accepting new fetches; waits for in-flight handlers to complete (workerd has a default graceful shutdown window); unfinished WS connections are closed with 1012; no additional drain logic is required | `terminationGracePeriodSeconds: 30` |
| `do-runtime` (supervisor PID1) | ① Stop accepting new DO requests; ② wait for in-flight handlers + output gates to complete (max `DO_DRAIN_IN_FLIGHT_MS`, default 8000ms); ③ flush all pending WAL segments and wait for cell-agent persistence proof; ④ call cell-agent `release(scope)` to release owner; ⑤ kill workerd; ⑥ supervisor exits | `terminationGracePeriodSeconds: 60` (including drain margin) |
| `cell-agent` | Follow the graceful handoff flow in cell-protocol §7; after obtaining the drain token, migrate cells in batches | `terminationGracePeriodSeconds: 120+` |

`do-runtime` SIGTERM supervisor total timeout: `DO_SUPERVISOR_SHUTDOWN_MS` (default 30000); after it is exceeded, workerd is force-killed (data may lose the last WAL segment, but cell-agent can recover from a follower).

## Control Plane Request Routing (I-10 fix)

Control metadata resides in a **single** control cell (ADR-117; relational tables, with app as the `ns` column). **Traefik load-balances admin traffic to any cell-agent replica**; non-owner replicas use the owner resolution library to **internally forward write operations to the owner replica of the control cell** (internal HTTP, internal token; ADR-118), while read operations can be served from a local cache. Ordering of write operations such as deploy/rollback is guaranteed by the control cell owner (single writer). When the owner of the control cell dies, the standard cell-agent takeover flow takes over; during the brief unavailable period, admin write operations return 503.

## Deployment Model (Overview)

- **Fixed layer** `cell-agent`: ≥2 nodes, stable identities (StatefulSet/fixed nodes), holds state bucket credentials.
- **Elastic layer** `do-runtime`: can be added/removed, **local disk may be ephemeral** (state authority is in cell-agent), can use Deployment for on-demand scaling.
- **Stateless layer** `user-runtime`: Deployment + HPA (ingress + execution).
- Object storage must be in the same region/nearby and satisfy conditional write requirements.

### Not Bound to a Single Orchestrator

Core discovery **does not depend on K8s**: cell-agent peers use **bucket node leases**, owners use bucket records, and DO liveness uses replication activity. K8s only provides one implementation of the following capabilities, all of which are replaceable:

| Requirement | K8s | Non-K8s Alternative |
|---|---|---|
| Caller → cell-agent (logical service name) | Service/ClusterIP | DNS round-robin, internal LB (Envoy/HAProxy/nginx), seed list + client-side polling |
| **do-runtime → cell-agent entrypoint** (WAL/claim/restore) | Service/ClusterIP | **Seed list / DNS / internal LB / Consul**; or give do-runtime **scoped credentials restricted to read-only `nodes/*`**; or cell-agent pushes configuration in reverse (do-runtime does not hold bucket credentials and cannot self-discover) |
| do-runtime takeover candidates | Endpoints | Consul/Nomad catalog; or **do-runtime registers/heartbeats with cell-agent** (fallback); or a configured list |
| Ingress / TLS | Ingress | **Standalone Traefik deployment** (file provider), HAProxy/nginx |
| Stable node identity / address | StatefulSet / pod DNS | Static hostname/IP + `advertise` configuration |
| Local disk | emptyDir / local PV | Local SSD / ephemeral disk |

Therefore it can be deployed on: **Docker Compose, systemd units, Nomad, bare metal/VMs, any orchestrator**. See [`decisions.md`](decisions.md) ADR-011 / ADR-018 / ADR-025 / ADR-027.

_Last updated: 2026-09-19_