# 部署

## 服务与端口

| 服务 | 端口 | 访问 | 备注 |
|---|---|---|---|
| 边缘代理（Traefik/nginx/云 LB） | 80/443 | 公网 | TLS + host 分流；**运维静态配置**（平台不下发，ADR-132） |
| `user-runtime` | :8081 公开 / :8088 内部 | 边缘 / 私网 | 入口 + 执行 |
| `do-runtime` | :8788 | 私网 | DO，弹性 |
| `cell-agent` | :7001 内部 REST / :8082 admin | 私网 / 边缘(admin) | 状态 + 控制（内部 Go↔Go 也走 :7001，无 gRPC，ADR-136） |
| 对象存储 | — | **仅 cell-agent**（+ scoped 直传） | 权威；do-runtime 不持桶凭据 |
| `do-supervisor`（可选） | :18901 | 私网 | DO 输出门/RPO=0（ADR-083），见下 |

## 镜像

单个镜像包含全部服务（`deploy/Dockerfile`）：

- Go 二进制：`cell-agent`、`cellhive`、`user-runtime`、`do-runtime`、`do-supervisor`；
- **pinned stock workerd**（`1.20260615.1`，构建期从 npm 拉取，可用 `--build-arg WORKERD_VERSION=` 改）——运行时是 `debian:bookworm-slim`（workerd 需 glibc）；
- 平台 JS：`workerd/` → `/app/workerd/`（`CELLHIVE_*_JS` 已指向那里）。

```bash
make docker-build                     # docker build -f deploy/Dockerfile -t cellhive:dev .
docker run --rm cellhive:dev workerd --version
```

**选择服务**：`ENTRYPOINT` 是一个 dispatcher（`deploy/entrypoint.sh`）——`command: ["user-runtime"]`（短名）或完整路径都行；不给命令则跑 `cell-agent`。这是必须的：Docker 的 `command:` 只覆盖 CMD，若把 ENTRYPOINT 直接设成 `cell-agent`，compose 里换成 `user-runtime` 会静默继续跑 cell-agent。

构建（ADR-159）：Go 二进制现在是 **CGo**（mattn/go-sqlite3 + 静态 vec1），必须带 SQLite 构建 tags（`make build` 已封装；Dockerfile 装 gcc）；x86-64 默认 `-march=x86-64-v3`（需 AVX2），非 AVX2 用 `-tags cellhive_vec1_portable`。

健康检查：`GET http://127.0.0.1:7001/readyz`（免鉴权；overloaded 时 503）。`user-runtime` 用 `GET /ready`（免鉴权；按需刷新投影，投影过期/cell 不可达/正在排空时 503）与 `POST /drain`（internal token；SIGTERM 时进程自动先 `/drain` 再宽限 3s 退出）；`do-runtime` 用 `GET /ready`（排空中或 cell-agent 不可达时 503）——compose/k8s/Helm 的 readiness 探针均已改用 `/ready`（ADR-156）。

## Docker Compose

```bash
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)   # 唯一必配的 secret（ADR-137）
docker compose -f deploy/compose/docker-compose.yml up --build
# 或
make compose-up
```

- 服务：`cell-agent`（FS 桶 `/data/bucket`，healthcheck `/readyz`）+ `user-runtime` + `do-runtime`（都 `depends_on: service_healthy`）；`restart: unless-stopped`；
- `rpo0` profile 额外起 `do-runtime-gated`；此时把 `CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788` 传给 compose（放置只用 gated，无门 `do-runtime` 变为 idle 可忽略）；
- profiles：`s3`（MinIO，配 `CELLHIVE_BUCKET=s3://cellhive` + `AWS_*`）、`edge`（Traefik，运维自配路由）；
- `.env.example` 是模板：**只有 `CELLHIVE_ROOT_KEY` 必需**（其余角色凭据由它派生）。

校验（不起容器）：`make compose-config`。

## Kubernetes

`deploy/k8s/` 提供 kustomize manifests（客户端渲染，无需集群）：

| 资源 | 说明 |
|---|---|
| `cell-agent` StatefulSet（2 副本，headless Service） | `CELLHIVE_NODE_ID=$(POD_NAME)`、`CELLHIVE_ADVERTISE=$(POD_IP):7001`（downward API）；`terminationGracePeriodSeconds: 120`；PVC 20Gi；readiness `/readyz` |
| `user-runtime` Deployment（2 副本）+ Service + HPA | 无状态，CPU 70% 扩到 10；`grace 30s` |
| `do-runtime` Deployment（2 副本，headless Service） | `CELLHIVE_DO_ADVERTISE=$(POD_IP):8788`；盘用 `emptyDir`（本地工作副本，权威在桶/cell-agent） |
| `cellhive-config` ConfigMap / `cellhive-secrets` Secret | 非敏感配置 + `CELLHIVE_ROOT_KEY`（示例见 `secret.example.yaml`） |

Secret 也可以用**文件挂载**代替内联 env（Docker/K8s `_FILE` 惯例，ADR-150）：挂载一个 secret volume 到 `/run/secrets` 并设 `CELLHIVE_ROOT_KEY_FILE=/run/secrets/root-key`；`rootKey`/`existingSecret` 仍是 Helm 的两种方式。

```bash
make k8s-render        # kubectl kustomize deploy/k8s（本地校验）
kubectl create ns cellhive
kubectl -n cellhive create secret generic cellhive-secrets \
  --from-literal=CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)"
kubectl apply -k deploy/k8s
```

网络策略：按 [`security.md`](./security.md) 只放行列出的内部链路；`cell-agent :7001/:8082` 不对外。持久化：只有 `cell-agent` 需要（PVC + state 桶）；do-runtime 无需 PV。

## DO 输出门 / RPO=0（`do-runtime-gated`，ADR-140）

生产用 `do-runtime-gated` 取代无门的 `do-runtime`：`do-runtime -render-only` 渲染 capnp，`do-supervisor` 托管 workerd 并把每个响应门控在 cell-agent 的持久性证明上（同时负责续租与 drain）。

```bash
# compose
docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d
# k8s
kubectl apply -k deploy/k8s/overlays/rpo0
```

- **已验证（真实容器）**：DO 调用经门返回 `count:2..5`（状态持久），落桶 `cells/workerd/__do__/<host>.sqlite/ltx/e1/*.ltx` + `owner.json`；
- 手动：`do-supervisor -dir /data/state/do -owner http://cell-agent:7001 -listen :18901 -do-url http://127.0.0.1:8788 -workerd /usr/local/bin/workerd -config <capnp>`。

## systemd / 裸机

- 每个服务一个 unit；`cell-agent` 用 `Restart=always`；
- 发现的替代：集群内 DNS/服务名，或 `/etc/hosts`；
- 入口：边缘代理独立部署（file provider / nginx / HAProxy）。

## Helm

```bash
helm lint deploy/helm/cellhive
helm template my deploy/helm/cellhive --set rootKey="$(openssl rand -base64 32)" >/dev/null
helm upgrade --install cellhive deploy/helm/cellhive --set existingSecret=cellhive-secrets
```

values 覆盖：镜像/副本/资源/存储、`doRuntime.gate`（true → `do-runtime-gated`，DO RPO=0）、`existingSecret` 或内联 `rootKey`、PDB、NetworkPolicy（`adminIngressFrom` 追加 admin 入站来源）、ingress。Chart 与 `deploy/k8s/`（base + `overlays/rpo0`）等价；`make k8s-render` / `make helm-lint` 离线校验。

## 一键门禁

```bash
make ci            # = bash scripts/ci.sh：gofmt/vet/test/build/js-test/cli-test/perf/rpo/s3/镜像/编排
```

缺工具（bun/docker/kubectl/helm/workerd）时该步自动 skip；`REQUIRE_ALL=1` 则转为失败。CI 定义见 `.github/workflows/ci.yml`。

## 关键环境变量

**完整清单见 [`configuration.md`](./configuration.md)**。生产至少需要：

| 变量 | 用途 |
|---|---|
| `CELLHIVE_ROOT_KEY` | **必填**（base64/hex ≥16B）：全部角色令牌由它 HKDF 派生（ADR-137）。未设则 `Validate()` 拒绝（`CELLHIVE_ALLOW_INSECURE_DEFAULTS` 仅限本地） |
| `CELLHIVE_NODE_ID` / `CELLHIVE_ADVERTISE` | 稳定节点身份与内网地址（`<ip>:7001`） |
| `CELLHIVE_BUCKET`（`s3://…`）/ `AWS_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | 对象存储；本地用 `CELLHIVE_BUCKET_DIR` |
| `CELLHIVE_DO_RUNTIMES` | do-runtime 列表（DO 放置），如 `do-runtime:8788` |
| `CELLHIVE_OIDC_JWKS_URL`（+ `_ISSUER`/`_AUDIENCE`） | 可选：admin 用 JWT/OIDC（多租户） |
| `CELLHIVE_DURABILITY` / `CELLHIVE_BUCKET_WAIT` | ack 姿态；`fleet` 强制 RPO=0 |
| `CELLHIVE_BASE_DOMAIN` | 可选：内置 worker 域 `<ns>-<worker>.<base>`，通配符由边缘配置 |

## 部署顺序（滚动升级）

1. **reader-before-writer**：先上新版 reader（能读旧数据）；
2. 等旧 reader 下线；
3. 再启用新版 writer；
4. 回滚安全：读写版本差 ≤1（ADR-034）。

## 待细化

- Terraform 模板（orchestration 现由 Helm + kustomize 提供）。

_最后更新：2026-09-17_
