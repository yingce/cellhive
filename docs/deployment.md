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
- **pinned stock workerd `1.20260916.1`** 与 **esbuild `0.28.2`**：构建期从精确 npm platform tarball 获取，并用 checked-in SHA-512 校验后才解包；镜像内分别为 `/usr/local/bin/workerd` 与 `/usr/local/bin/esbuild`，后者通过 `CELLHIVE_ESBUILD` 供生产打包路径使用。若显式覆盖版本，必须同时提供对应的 `*_INTEGRITY_SHA512`，不能只改版本号；
- 平台 JS：`workerd/` → `/app/workerd/`（`CELLHIVE_*_JS` 已指向那里）。
- 许可证与 notices：`/usr/share/licenses/cellhive/{workerd,esbuild}/LICENSE`、`/usr/share/doc/cellhive/THIRD_PARTY_NOTICES.md`。

```bash
make docker-build                     # docker build -f deploy/Dockerfile -t cellhive:dev .
docker run --rm cellhive:dev workerd --version
docker run --rm --entrypoint /usr/local/bin/esbuild cellhive:dev --version
```

**选择服务**：`ENTRYPOINT` 是一个 dispatcher（`deploy/entrypoint.sh`）——`command: ["user-runtime"]`（短名）或完整路径都行；不给命令则跑 `cell-agent`。这是必须的：Docker 的 `command:` 只覆盖 CMD，若把 ENTRYPOINT 直接设成 `cell-agent`，compose 里换成 `user-runtime` 会静默继续跑 cell-agent。

构建（ADR-159）：Go 二进制现在是 **CGo**（mattn/go-sqlite3 + 静态 vec1），必须带 SQLite 构建 tags（`make build` 已封装；Dockerfile 装 gcc）；x86-64 默认 `-march=x86-64-v3`（需 AVX2），非 AVX2 用 `-tags cellhive_vec1_portable`。

健康检查：`GET http://127.0.0.1:7001/readyz`（免鉴权；overloaded 时 503）。`user-runtime` 用 `GET /ready`（免鉴权；按需刷新投影，投影过期/cell 不可达/正在排空时 503）与 `POST /drain`（internal token；SIGTERM 时进程自动先 `/drain` 再宽限 3s 退出）；`do-runtime` 用 `GET /ready`（排空中或 cell-agent 不可达时 503）——compose/k8s/Helm 的 readiness 探针均已改用 `/ready`（ADR-156）。

## Docker Compose

```bash
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)
python3 scripts/deploy-preflight.py compose
docker compose -f deploy/compose/docker-compose.yml up --build -d
```

- 默认是**单节点开发/验证拓扑**：cell-agent 的命名卷存 SQLite 与权威 FS bucket；user-runtime 与 DO runtime 用各自工作卷（不共享 localDisk）。暴露业务入口/Admin 要由运维单独配置边缘代理。
- `rpo0` profile 使用 gated DO：`export CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788` 后先跑 `python3 scripts/deploy-preflight.py compose --profile rpo0`，再运行 `docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up --build -d cell-agent user-runtime do-runtime-gated`。不选定服务的 `--profile rpo0 up` 会同时启动默认无门 DO；preflight 会拒绝未切换放置目标的配置。
- S3：先启动 `--profile s3 up -d minio`、用 `bin/s3init -endpoint http://127.0.0.1:9000 -access ... -secret ... -bucket cellhive` 创建桶，再设置 `CELLHIVE_BUCKET=s3://cellhive`、`AWS_ENDPOINT_URL=http://minio:9000` 与同一组 `AWS_*` 启动栈。MinIO 镜像已固定版本；`latest` 不应作为部署基线。

> **状态权威限制（2026-09-22）**：上述 Compose/MinIO 固定镜像 `RELEASE.2025-02-18T16-25-55Z` 经真实条件删除探针发现忽略 `DeleteObject If-Match`；新版 cell-agent 会在启动时拒绝该桶，不能把此示例当成可运行的 S3 状态权威。须换满足完整 `make s3-test` 契约的对象存储再启动；不要关闭启动探针或使用非原子 `HEAD+DELETE` 替代。
- `.env.example` 为参数模板，根密钥在任何启动时必配；真实密钥不可提交。

静态拓扑校验：`python3 scripts/deploy-preflight.py compose`。完整真实运行验收：`bash scripts/runtime-baseline-e2e.sh`。

## Kubernetes

`deploy/k8s/` 提供 Kustomize base 与 `overlays/rpo0`；默认 base 是**单 cell-agent + 单 DO runtime**，避免每个 PVC 内的独立 FS bucket 被误当成共享桶，也避免将一个 headless Service 的随机 Pod DNS 结果当作稳定 DO 分片地址。user-runtime 可按 HPA 水平扩展。`CELLHIVE_CELL_URL` 固定指向 `cell-agent-0.cell-agent.cellhive.svc.cluster.local:7001`；节点租约的 `CELLHIVE_PEER_URL` 则使用 Pod IP，而非 loopback。DO Deployment 使用 `Recreate`，避免滚动发布期间单地址同时指向新旧 Pod。

```bash
python3 scripts/deploy-preflight.py k8s --path deploy/k8s/overlays/rpo0
kubectl create namespace cellhive --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cellhive create secret generic cellhive-secrets \
  --from-literal=CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)"
# 改为仓库外已发布的镜像（镜像版本/架构须匹配节点）；不要用本地 cellhive:dev。
kubectl apply -k deploy/k8s/overlays/rpo0
kubectl -n cellhive rollout status statefulset/cell-agent
kubectl -n cellhive rollout status deployment/do-runtime-gated
kubectl -n cellhive rollout status deployment/user-runtime
```

多 cell-agent 节点必须**先**将 `CELLHIVE_BUCKET=s3://...` 与凭据提供给所有节点，确认 bucket 条件创建、CAS、条件删除、range read、presign 均通过，再扩副本。单一 `CELLHIVE_DO_RUNTIMES=do-runtime:8788` 仍只能对应一个 DO Pod；扩 DO 需要稳定逐实例地址列表和配置/放置更新，不能直接调 `replicas: 2`。`scripts/deploy-preflight.py` 对这两种误配失败关闭。`kubectl kustomize`/`helm template` 是客户端渲染，不等于真实集群验证。

NetworkPolicy 将 user-runtime `:8088` 限为 cell-agent Pod；公开 `:8081` 由运维入口接入；gated DO overlay 也有单独的 NetworkPolicy。集群需支持 NetworkPolicy，实际云端 S3 端点、DNS 与代理入站需按网络环境配置。

## DO 输出门 / RPO=0（`do-runtime-gated`，ADR-140）

生产用 `do-runtime-gated` 取代无门的 `do-runtime`：`do-runtime -render-only` 渲染 capnp，`do-supervisor` 托管 workerd 并把每个响应门控在 cell-agent 的持久性证明上（同时负责续租与 drain）。

```bash
# compose（先设置 CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788 并运行 preflight）
docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d cell-agent user-runtime do-runtime-gated
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
