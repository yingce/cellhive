# Deployment

## Services and Ports

| Service | Port | Access | Notes |
|---|---|---|---|
| Edge proxy (Traefik/nginx/cloud LB) | 80/443 | Public Internet | TLS + host-based routing; **static ops configuration** (not distributed by the platform, ADR-132) |
| `user-runtime` | :8081 public / :8088 internal | Edge / private network | Entry point + execution |
| `do-runtime` | :8788 | Private network | DO, elastic |
| `cell-agent` | :7001 internal REST / :8082 admin | Private network / edge(admin) | State + control (internal Go↔Go also uses :7001, no gRPC, ADR-136) |
| Object storage | — | **cell-agent only** (+ scoped direct upload) | Authoritative; do-runtime does not hold bucket credentials |
| `do-supervisor` (optional) | :18901 | Private network | DO output gate/RPO=0 (ADR-083), see below |

## Image

A single image contains all services (`deploy/Dockerfile`):

- Go binaries: `cell-agent`, `cellhive`, `user-runtime`, `do-runtime`, `do-supervisor`;
- **pinned stock workerd `1.20260916.1`** and **esbuild `0.28.2`**: fetched from exact npm platform tarballs and verified against checked-in SHA-512 digests before extraction. They are installed at `/usr/local/bin/workerd` and `/usr/local/bin/esbuild`; the latter is exposed as `CELLHIVE_ESBUILD` for production bundling. Overriding a version also requires the matching `*_INTEGRITY_SHA512`; changing only the version must fail closed;
- Platform JS: `workerd/` → `/app/workerd/` (`CELLHIVE_*_JS` already points there).
- Licenses and notices: `/usr/share/licenses/cellhive/{workerd,esbuild}/LICENSE` and `/usr/share/doc/cellhive/THIRD_PARTY_NOTICES.md`.

```bash
make docker-build                     # docker build -f deploy/Dockerfile -t cellhive:dev .
docker run --rm cellhive:dev workerd --version
docker run --rm --entrypoint /usr/local/bin/esbuild cellhive:dev --version
```

**Selecting a service**: `ENTRYPOINT` is a dispatcher (`deploy/entrypoint.sh`) — `command: ["user-runtime"]` (short name) or the full path both work; if no command is given, it runs `cell-agent`. This is required: Docker `command:` only overrides CMD. If ENTRYPOINT is set directly to `cell-agent`, changing it to `user-runtime` in compose will silently continue running cell-agent.

Build (ADR-159): Go binaries are now **CGo** (mattn/go-sqlite3 + static vec1), and must include SQLite build tags (`make build` wraps this; Dockerfile installs gcc); x86-64 defaults to `-march=x86-64-v3` (requires AVX2). For non-AVX2, use `-tags cellhive_vec1_portable`.

Health checks: `GET http://127.0.0.1:7001/readyz` (no auth; returns 503 when overloaded). `user-runtime` uses `GET /ready` (no auth; refreshes projections on demand; returns 503 when projections are stale, the cell is unreachable, or draining is in progress) and `POST /drain` (internal token; on SIGTERM the process calls `/drain` before a 3-second grace period). `do-runtime` uses `GET /ready` (503 while draining or when cell-agent is unreachable); on SIGTERM it invokes local `/v1/do/drain`, waits for in-flight work and lease release (up to 15 seconds), then stops workerd. Compose, Kubernetes, and Helm readiness probes use `/ready` (ADR-156).

## Docker Compose

```bash
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)
python3 scripts/deploy-preflight.py compose
docker compose -f deploy/compose/docker-compose.yml up --build -d
```

The default is a **single-node** stack: the cell-agent named volume holds SQLite and the authoritative filesystem bucket. User and DO runtimes have separate working volumes. An operator-managed edge must publish the public and admin endpoints.

For gated DO, set `CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788`, check `python3 scripts/deploy-preflight.py compose --profile rpo0` with the same environment, then run `docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d cell-agent user-runtime do-runtime-gated`. A bare `--profile rpo0 up` also starts the default ungated DO; without the placement override, requests still use it. To use MinIO, start `--profile s3 up -d minio`, create the bucket using `bin/s3init -endpoint http://127.0.0.1:9000 -access ... -secret ... -bucket cellhive`, and configure `CELLHIVE_BUCKET=s3://cellhive`, `AWS_ENDPOINT_URL=http://minio:9000`, and matching `AWS_*` credentials before starting cell-agent. Never commit actual keys to `.env.example`.

**Authority warning (2026-09-22):** The pinned Compose MinIO `RELEASE.2025-02-18T16-25-55Z` ignores `DeleteObject If-Match` in the real conditional-delete probe. The updated cell-agent refuses to start with that bucket; this MinIO example is not a working authority deployment. Use a store that passes the complete `make s3-test` contract instead. Do not disable the startup probe or replace atomic deletion with `HEAD+DELETE`.

The actual runtime exercise is `bash scripts/runtime-baseline-e2e.sh`; static Compose rendering is not an E2E test.

## Kubernetes

The Kustomize base is deliberately **one cell-agent plus one ungated DO runtime**. The ungated runtime has no authoritative capture, keeps `Recreate`, and does not promise DO state after Pod replacement. The production `rpo0` overlay runs **two gated DO Pods**: the ClusterIP Service load-balances arbitrary first hops, each Pod advertises its Pod IP in owner records, and non-owners forward to the owner. After an owner replacement, the winner restores the deterministic `host_id` actor/facets/sidecars through cell-agent before dispatch. The gated Deployment uses `RollingUpdate(maxUnavailable=1,maxSurge=1)` and a `minAvailable:1` PDB. user-runtime scales via HPA; `CELLHIVE_CELL_URL` names the stable StatefulSet Pod and agent leases advertise Pod IPs.

```bash
python3 scripts/deploy-preflight.py k8s --path deploy/k8s/overlays/rpo0
kubectl create namespace cellhive --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cellhive create secret generic cellhive-secrets \
  --from-literal=CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)"
# Replace cellhive:dev with a published image available to the cluster.
kubectl apply -k deploy/k8s/overlays/rpo0
kubectl -n cellhive rollout status statefulset/cell-agent
kubectl -n cellhive rollout status deployment/do-runtime-gated
kubectl -n cellhive rollout status deployment/user-runtime
```

Scale cell-agent beyond one Pod **only after** provisioning shared S3 and validating conditional create/CAS/delete, ranged reads, and presign. Multi-Pod DO takeover is supported only by the gated RPO=0 runtime: `CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788` targets the Service, Endpoints follow replicas, and owner records carry actual Pod addresses. The ungated base stays single-replica. Preflight rejects multiple filesystem-backed agents, an empty DO entrypoint, or an active runtime omitted from placement. Rendering is static only; run `bash scripts/k3d-runtime-e2e.sh` for a disposable real k3d acceptance covering two gated Pods, owner-Pod deletion/cold restore, and 120 logical DO writes during a rollout. NetworkPolicy explicitly permits cluster DNS and limits user-runtime port 8088 to agent Pods; configure cloud endpoints, DNS, and edge ingress for the target cluster.

## DO Output Gate / RPO=0 (`do-runtime-gated`, ADR-140)

In production, use `do-runtime-gated` instead of the ungated `do-runtime`: `do-runtime -render-only` renders capnp, and `do-supervisor` manages workerd and gates each response on a durability proof from cell-agent (also handling lease renewal and drain).

```bash
# compose (set CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788 and run preflight first)
docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d cell-agent user-runtime do-runtime-gated
# k8s
kubectl apply -k deploy/k8s/overlays/rpo0
```

- **Verified (real containers):** gated DO responses persist and write `cells/workerd/__do__/<host>.sqlite/ltx/e1/*.ltx`; `scripts/runtime-baseline-e2e.sh` covers same-volume process restart.
- **Real Kubernetes acceptance (2026-09-25 PASS):** `scripts/k3d-runtime-e2e.sh` proves two gated Pods, per-host-manifest cold restore after owner-Pod deletion (`1→2`), and an exact final count of `123` after 120 logical writes during RollingUpdate (no lost or duplicate commits). Ownership transition can return transient 5xx responses; the acceptance client retries and verifies the exact final count, so this is not a claim that every physical HTTP attempt is error-free. A single-node k3d run does not replace cross-host, cloud-store, or target-CNI validation.
- Manual: `do-supervisor -dir /data/state/do -owner http://cell-agent:7001 -listen :18901 -do-url http://127.0.0.1:8788 -workerd /usr/local/bin/workerd -config <capnp>`.

## systemd / Bare Metal

- One unit per service; `cell-agent` uses `Restart=always`;
- Discovery alternatives: in-cluster DNS/service names, or `/etc/hosts`;
- Entry point: deploy the edge proxy separately (file provider / nginx / HAProxy).

## Helm

```bash
helm lint deploy/helm/cellhive
helm template my deploy/helm/cellhive --set rootKey="$(openssl rand -base64 32)" >/dev/null
helm upgrade --install cellhive deploy/helm/cellhive --set existingSecret=cellhive-secrets
```

values overrides: image/replicas/resources/storage, `doRuntime.gate` (true → `do-runtime-gated`, DO RPO=0), `existingSecret` or inline `rootKey`, PDB, NetworkPolicy (`adminIngressFrom` appends admin ingress sources), ingress. The Chart is equivalent to `deploy/k8s/` (base + `overlays/rpo0`); `make k8s-render` / `make helm-lint` validate offline.

## One-Command Gate

```bash
make ci            # = bash scripts/ci.sh: gofmt/vet/test/build/js-test/cli-test/perf/rpo/s3/image/orchestration
```

When tools are missing (bun/docker/kubectl/helm/workerd), this step automatically skips them; with `REQUIRE_ALL=1`, skips become failures. The CI definition is in `.github/workflows/ci.yml`.

## Key Environment Variables

**For the full list, see [`configuration.md`](configuration.md)**. Production requires at least:

| Variable | Purpose |
|---|---|
| `CELLHIVE_ROOT_KEY` | **Required** (base64/hex ≥16B): all role tokens are derived from it via HKDF (ADR-137). If unset, `Validate()` rejects it (`CELLHIVE_ALLOW_INSECURE_DEFAULTS` is local-only) |
| `CELLHIVE_NODE_ID` / `CELLHIVE_ADVERTISE` | Stable node identity and private network address (`<ip>:7001`) |
| `CELLHIVE_BUCKET` (`s3://…`) / `AWS_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | Object storage; locally use `CELLHIVE_BUCKET_DIR` |
| `CELLHIVE_DO_RUNTIMES` | do-runtime list (DO placement), for example `do-runtime:8788` |
| `CELLHIVE_OIDC_JWKS_URL` (+ `_ISSUER`/`_AUDIENCE`) | Optional: JWT/OIDC for admin (multi-tenant) |
| `CELLHIVE_DURABILITY` / `CELLHIVE_BUCKET_WAIT` | Ack posture; `fleet` enforces RPO=0 |
| `CELLHIVE_BASE_DOMAIN` | Optional: built-in worker domain `<ns>-<worker>.<base>`; wildcard is configured at the edge |

## Deployment Order (Rolling Upgrade)

1. **reader-before-writer**: roll out the new reader first (able to read old data);
2. Wait for old readers to go offline;
3. Then enable the new writer;
4. Rollback safety: read/write version skew ≤1 (ADR-034).

## To Be Detailed

- Terraform templates (orchestration is currently provided by Helm + kustomize).

_Last updated: 2026-09-17_
