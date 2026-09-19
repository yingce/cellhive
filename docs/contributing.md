# 贡献者阅读路径

本文给"改某处代码前该读什么、必须跑什么、要守住什么不变量"的最小清单。**文档与代码冲突时以文档为准并同步两者**；改设计先改 [`decisions.md`](./decisions.md)。

## 通用入口（所有人都先读）

1. [`architecture.md`](./architecture.md) — 组件、端口、信任边界、状态归属；
2. [`cell-protocol.md`](./cell-protocol.md) — owner/epoch/LTX/RPO=0 的权威模型；
3. [`decisions.md`](./decisions.md) — 为什么是这样，以及每条选择的代价；
4. 本文下方对应的**改动类型**条目。

**铁律**（任何改动都不能破）：

- **不 fork / 不修改 workerd**；只通过 `workerLoader`、bindings、capnp 扩展。
- **热路径禁止 List**；对象存储只做点查与条件写，`List` 仅用于诊断/运维。
- **授权先于转发**；租户只拿到 binding facade 与 scoped token，拿不到平台 secret。
- **一切持久化经 `cell-agent`**；runtime 不直接访问对象存储。
- 提交门禁：`bash scripts/ci.sh` 必须输出 `GATE: PASS`。

## 按改动类型

### 协议 / 复制 / 恢复（owner、epoch、LTX、快照、RPO=0）

- **先读**：[`cell-protocol.md`](./cell-protocol.md)、[`protocol-formats.md`](./protocol-formats.md)、[`storage-and-s3.md`](./storage-and-s3.md)；
- **要守住**：单写者 + 单调 epoch；ack 必须晚于耐久性证明；接管先恢复死 owner 的 open log；bucket 条件写是唯一仲裁；
- **测试锚点**：`internal/owner`、`internal/replica`、`internal/ltx`、`internal/recovery` 单测；`make rpo-test`（kill -9 后逐 key 校验 RPO=0）。

### Durable Objects（facet、alarm、WebSocket、migration、会话策略）

- **先读**：[`durable-objects.md`](./durable-objects.md)、[`workerd-integration.md`](./workerd-integration.md)；
- **要守住**：DO 是 cell（一个对象一个 lease）；facet 必须来自 `workerLoader.getDurableObjectClass()`；alarm 走 shim + 统一 timer；WS 迁移/重启 1012；
- **测试锚点**：`internal/doruntime`（真实 workerd e2e）、`make js-test`。

### binding / facade（KV、D1、R2、Queue、Workflow、AI、Hyperdrive、Vectorize）

- **先读**：[`bindings.md`](./bindings.md)、[`compatibility-matrix.md`](./compatibility-matrix.md)、[`wrangler-compat.md`](./wrangler-compat.md)；
- **要守住**：binding 名 = 已登记资源名；scope token 校验；读强一致（转发 owner）；对 CF 的拒绝项要**显式拒绝而不是静默接受**；
- **测试锚点**：`internal/userruntime`（真实 workerd）、`internal/server`、`make js-test`。

### 计时 / 调度（cron、queue、DO alarm、wake 索引、waker）

- **先读**：[`timers-and-dispatch.md`](./timers-and-dispatch.md)；
- **要守住**：due 记录在 cell SQLite（权威），bucket `wake/` 只是发现索引；**索引不落后于 timer**（先发布再提交）；派发 at-least-once、成功才标 fired；
- **测试锚点**：`internal/timer`、`internal/dispatch`、`internal/cron`、`internal/wake`。

### 控制面 / 发布 / 路由

- **先读**：[`control-plane.md`](./control-plane.md)、[`routing.md`](./routing.md)；
- **要守住**：版本不可变、发布原子、路由投影纯拉取；控制面写也走 `capturedWrite`（RPO=0）；
- **测试锚点**：`internal/control`、`internal/server`、`cmd/cellhive` e2e。

### 部署 / 配置 / 运维

- **先读**：[`deployment.md`](./deployment.md)、[`configuration.md`](./configuration.md)、[`operations.md`](./operations.md)；
- **要守住**：默认行为不回归；新增 env 必须有默认值并登记进 `configuration.md`；探针用 `/ready`；
- **测试锚点**：`make compose-config`、`make k8s-render`、`make helm-lint`、`make docker-build`。

### 可观测性 / 日志 / 追踪

- **先读**：[`observability.md`](./observability.md)、[`tracing.md`](./tracing.md)；
- **要守住**：新增指标先登记名称与数据源；追踪是旁路、best-effort、不影响请求；
- **测试锚点**：对应指标/span 的单元或 e2e。

### CLI / 打包 / wrangler 兼容 / dev

- **先读**：[`wrangler-compat.md`](./wrangler-compat.md)、[`dev-mode.md`](./dev-mode.md)；
- **要守住**：生产打包用 Go + esbuild（无 Node）；dev CLI（Bun + Miniflare）只是开发工具，不属生产产物，且必须与 pinned workerd 同期；
- **测试锚点**：`make cli-test`、`internal/bundler`、`internal/wrangler` 契约测试。

## 提交前检查

```bash
gofmt -l .          # 或 make fmt
make vet            # go vet
make test           # 单元测试（必须带 SQLite 构建 tags，make 已封装）
bash scripts/ci.sh  # 全门禁，输出 GATE: PASS
```

- 只提交**与本次改动相关**的文件；不要提交构建产物、密钥或本地数据目录。
- 改设计/协议：先补 [`decisions.md`](./decisions.md)（ADR），再改代码与对应文档。
- 新增文档：在 [`docs/README.md`](./README.md) 的分组地图里登记。

_最后更新：2026-09-19_
