# Glossary

- **cell**: A named state unit with its own independent SQLite database; equivalent to a Durable Object. scope = `<namespace>/<class>/<id>`.
- **scope**: The persistent identifier of a cell: namespace + class + id.
- **cell-agent**: An in-house Go process (fixed cluster): owns and replicates cells for KV/D1/Queue/Workflows/Cron, and also handles the control plane, owner resolution, timer dispatch, and waker. It is the **only long-lived holder of object storage credentials** and the only persistent data endpoint.
- **do-runtime**: The **distributed elastic execution layer** that hosts native workerd Durable Objects; local disk may be ephemeral.
- **user-runtime**: A stateless workerd pool; north-south entry point (Worker routing/version resolution) + tenant code execution; `:8081` public, `:8088` internal dispatch.
- **owner**: The node that holds a given cell's "working SQLite". Class A = cell-agent node; Class B (DO) = do-runtime node.
- **owner record**: `cells/<scope>/owner.json` in the bucket, containing node/role/session/epoch/expiry/address.
- **epoch**: The monotonic generation for each activation/takeover of a cell; replicated data is written under the `e<epoch>` prefix, serving as a write-validity fence.
- **fence**: See epoch; writes from an old owner land in an abandoned prefix and are made harmless.
- **LTX**: The replication format for SQLite transaction logs (implementation follows LiteFS/`superfly/ltx`).
- **node lease**: `nodes/<node>.json`, proof that a node is alive (self-registered by cell-agent).
- **fleet**: A set of nodes sharing the same bucket.
- **ensemble / follower**: In fleet mode, the owner recruits up to 2 followers; **confirmation occurs once ≥1 follower has fsynced to disk** (≥2 nodes are sufficient).
- **output gate**: Before a write is acknowledged to the caller, durable proof must first be obtained.
- **RPO=0**: Acknowledged writes will not be lost due to a single-node failure.
- **waker**: The single fleet leader elected via bucket lease, acting as the fallback timer when the owner is dead/unreachable.
- **timer**: A unified abstraction for scheduled events (do-alarm/cron/queue-delay/queue-retry/workflow-sleep/timeout).
- **host adapter**: Platform code inside workerd that converts CF-style bindings into calls to cell-agent (binding-scoped, immutable props).
- **worker id**: `<ns>:<worker>:<version>`, the load identity for an immutable version.
- **route projection**: The mapping `host/path → (ns,worker) → active version`; pulled by user-runtime and cached with a 5–10s TTL.
- **control cell**: A cell that hosts control metadata (sharded by app).
- **state bucket / code bucket / assets / r2 / backup**: Role separation for object storage (default is a single bucket + prefixes).
- **Traefik**: Operations ingress, responsible for TLS and host-based traffic splitting (not an internal platform component).
- **admin host**: Management console entry point (Traefik → cell-agent :8082).
- **stock workerd**: The official Cloudflare release of workerd, unpatched and not forked.
- **bundle**: The code artifact packaged from a deployed version (modules + manifest), content-addressed (SHA-256).
- **envelope encryption**: Secrets are stored as ciphertext; the root key (KEK) resides outside the cell (env/KMS).
- **CAS / conditional write**: Bucket compare-and-swap / if-match writes, used for single-writer determination.

_Last updated: 2026-09-19_