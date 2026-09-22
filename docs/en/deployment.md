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

Health checks: `GET http://127.0.0.1:7001/readyz` (no auth; returns 503 when overloaded). `user-runtime` uses `GET /ready` (no auth; refreshes projections on demand; returns 503 when projections are stale, the cell is unreachable, or draining is in progress) and `POST /drain` (internal token; on SIGTERM the process automatically calls `/drain` first, then exits after a 3s grace period); `do-runtime` uses `GET /ready` (returns 503 while draining or when cell-agent is unreachable) — compose/k8s/Helm readiness probes have all been changed to use `/ready` (ADR-156).

## Docker Compose

```bash
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)   # the only required secret (ADR-137)
docker compose -f deploy/compose/docker-compose.yml up --build
# or
make compose-up
```

- Services: `cell-agent` (FS bucket `/data/bucket`, healthcheck `/readyz`) + `user-runtime` + `do-runtime` (both `depends_on: service_healthy`); `restart: unless-stopped`;
- The `rpo0` profile additionally starts `do-runtime-gated`; in this case, pass `CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788` to compose (placement uses only the gated runtime; the ungated `do-runtime` becomes idle and can be ignored);
- Profiles: `s3` (MinIO, with `CELLHIVE_BUCKET=s3://cellhive` + `AWS_*`), `edge` (Traefik, ops-configured routes);
- `.env.example` is a template: **only `CELLHIVE_ROOT_KEY` is required** (other role credentials are derived from it).

Validation (without starting containers): `make compose-config`.

## Kubernetes

`deploy/k8s/` provides kustomize manifests (client-side rendering, no cluster required):

| Resource | Description |
|---|---|
| `cell-agent` StatefulSet (2 replicas, headless Service) | `CELLHIVE_NODE_ID=$(POD_NAME)`, `CELLHIVE_ADVERTISE=$(POD_IP):7001` (downward API); `terminationGracePeriodSeconds: 120`; PVC 20Gi; readiness `/readyz` |
| `user-runtime` Deployment (2 replicas) + Service + HPA | Stateless, scales to 10 at CPU 70%; `grace 30s` |
| `do-runtime` Deployment (2 replicas, headless Service) | `CELLHIVE_DO_ADVERTISE=$(POD_IP):8788`; disk uses `emptyDir` (local working copy; authoritative state is in the bucket/cell-agent) |
| `cellhive-config` ConfigMap / `cellhive-secrets` Secret | Non-sensitive configuration + `CELLHIVE_ROOT_KEY` (see `secret.example.yaml` for an example) |

Secrets can also use **file mounts** instead of inline env (Docker/K8s `_FILE` convention, ADR-150): mount a secret volume to `/run/secrets` and set `CELLHIVE_ROOT_KEY_FILE=/run/secrets/root-key`; `rootKey`/`existingSecret` remain the two Helm options.

```bash
make k8s-render        # kubectl kustomize deploy/k8s (local validation)
kubectl create ns cellhive
kubectl -n cellhive create secret generic cellhive-secrets \
  --from-literal=CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)"
kubectl apply -k deploy/k8s
```

Network policies: per [`security.md`](security.md), only the listed internal paths are allowed; `cell-agent :7001/:8082` is not exposed externally. Persistence: only `cell-agent` needs it (PVC + state bucket); do-runtime does not need a PV.

## DO Output Gate / RPO=0 (`do-runtime-gated`, ADR-140)

In production, use `do-runtime-gated` instead of the ungated `do-runtime`: `do-runtime -render-only` renders capnp, and `do-supervisor` manages workerd and gates each response on a durability proof from cell-agent (also handling lease renewal and drain).

```bash
# compose
docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d
# k8s
kubectl apply -k deploy/k8s/overlays/rpo0
```

- **Verified (real containers)**: DO calls through the gate return `count:2..5` (state is durable), writing to the bucket at `cells/workerd/__do__/<host>.sqlite/ltx/e1/*.ltx` + `owner.json`;
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
