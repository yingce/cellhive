# AGENTS.md

CellHive 的 AI/编码代理工作约定。设计契约以 `docs/` 为准，**当前代码与 `docs/` 冲突时，先改文档或同步两者**。

## 项目要点

- 计算层：**stock workerd**，不修改 workerd；只用 `workerLoader`/bindings/capnp。
- 状态层：**一切皆 cell**（`<ns>/<class>/<id>`），SQLite + 对象存储为权威，bucket 条件写决定 owner，epoch fence，RPO=0。
- `cell-agent`（Go，固定集群）：状态 + 控制面 + owner 解析 + 计时派发 + **唯一桶凭据**。
- `do-runtime`（workerd + supervisor）：原生 DO，分布式弹性，**不持桶凭据**。
- `user-runtime`：入口（路由/版本）+ 租户执行；`:8081` 公开、`:8088` 内部。
- 入口无独立 gateway：Traefik（TLS/host）+ user-runtime loader。

## 代码约定

- 语言：**全栈 Go**（`cmd/`、`internal/`）；workerd 侧 JS/capnp 在 `workerd/`（目录约定见下）。
- **dev CLI 在 `cli/`（Bun + Miniflare，ADR-065）**：仅开发工具，不属生产产物；pin `workerd` 到平台版本（见 `cli/package.json` 的 `overrides`），且 Miniflare 版本须与 pinned workerd 同期。运行：`cd cli && bun run src/index.ts dev <dir>`。
- 只在 `internal/` 内跨包复用；`cmd/` 尽量薄。
- 对象存储：**热路径禁止 List**，只用点查与条件写；`List` 仅用于诊断/运维。桶内带前缀的文件一律用 `objectstore.Objects`（通用前缀白名单 + 穿越防护；见 `internal/objectstore`），不要裸用 `bucket.Bucket`。桶的顶层前缀是**保留命名空间**（`cells/`、`nodes/`、`fleet/`、`bundles/`、`assets/`、`dosupervisor/`，各属一个子系统）：保留前缀只能用 `objectstore.NewOwned(b, prefix, owner)` 由属主打开，泛用 `NewObjects` 会拒绝；新增前缀须登记进 `objectstore` 的注册表。
- 协议/常量放在 `internal/cell`、`internal/bucket`；错误用 `errors.Is` 判定。
- 新增依赖前先确认必要；优先标准库。

## 目录约定（命名要直观）

- 顶层：`cmd/`（薄 CLI/服务入口）、`internal/`（可复用包）、`workerd/`（workerd JS/capnp）、`cli/`（dev 工具，非生产）、`docs/`（现行文档）、`docs/archive/`（历史文档，如 `p0-tasks.md`/`p0-report.md`）。
- `workerd/` 分**生产**与**非生产**，不要把生产代码放进 spike/存档目录：
  - 生产：`user-runtime/`、`do-runtime/`（host actor/loader/wrapper）、`platform/`（两个 runtime 共用的平台 JS：`facades.js`/`bindings.js`/`bindings-wrapper.js`，ADR-090）、`wrapper/`（DO 组提交）。
  - 非生产：`spikes/<阶段>/`（探索性 spike，如 `spikes/p0/`）、`skeletons/`（已被取代的骨架 capnp）。
  - 详见 `workerd/README.md`。
- 新增 workerd 共用 JS 放 `workerd/platform/`；阶段探索放 `workerd/spikes/<阶段>/`。历史文档归档到 `docs/archive/`，不要留在 `docs/` 顶层。

## 必须遵守的决策

见 `docs/decisions.md`（ADR-001 ~ 177）。改设计要先改文档。

## 命令

```bash
make build   # go build ./...
make test    # go test ./...
make vet     # go vet ./...
make fmt     # gofmt -w .
```

## 测试要求

- 改动协议逻辑（owner/epoch/lease/复制）：补/跑单元测试。
- 改动文档：保持与 `docs/known-issues.md`、`docs/decisions.md` 一致。
- 禁止未经验证声称"完成/通过"；先跑命令拿证据。

## 不做

- 不 fork / 修改 workerd；
- 不引入第二套 JS 引擎；
- 不引入 NATS/kvrocks/etcd/APISIX 等（发现用 bucket + 内置 DNS）；
- 不在热路径 List bucket。
