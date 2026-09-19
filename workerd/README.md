# workerd 配置与平台 JS

目录约定：

- **`user-runtime/`**、**`do-runtime/`**：生产（`cmd/user-runtime` / `cmd/do-runtime` 运行期渲染）的 host actor / loader / wrapper。
- **`platform/`**：**两个 runtime 共用的平台 JS**（生产）：`facades.js`（binding facade）、`bindings.js`（WDL 式 RPC entrypoint 能力类，ADR-090）、`bindings-wrapper.js`（patch importable env）。
- **`spikes/p0/`**：**P0 可行性 spike**（历史/诊断，非生产）：`worker.js`/`config.capnp`（DO + WAL）、`loader.capnp`/`host.js`（workerLoader P0.7 spike）、`gate/`（输出门性能门）。
- **`skeletons/`**：早期**骨架 capnp**（`user-runtime.capnp` / `do-runtime.capnp`），已被运行期渲染取代，仅存档。
- **`wrapper/`**：DO 组提交（`groupcommit.js`）等。

生产入口只有 `user-runtime/`、`do-runtime/`、`platform/`；`spikes/`、`skeletons/` 不参与生产。

## 关键点（见 `docs/workerd-integration.md`）

- `workerLoader`：动态加载租户不可变 bundle（进程需 `--experimental`）。
- 租户 loaded worker 的 `globalOutbound` = **公网 only network**（不含 RFC1918 / cell-agent）。
- `do-runtime` 需要 `durableObjectNamespaces` + localDisk；host actor 用 `getDurableObjectClass()` 解析租户类。
- 平台代码（loader / host adapter / DO host actor）使用独立的 private network binding。
- 端口：user-runtime `:8081`（公开 loader）+ `:8088`（内部特权派发）。

## 待办

- [x] `user-runtime.capnp`（**由 `cmd/user-runtime` 运行期渲染**，见 `internal/userruntime`）：内部派发 `:8088` 已实现（queue/scheduled handler via workerLoader + wrapper RPC）；公开 loader `:8081` 已实现（路由投影拉取 + 版本加载 + facade env + 清头 + 静态资产，ADR-068/069）；`:8088` 支持 queue()/scheduled()（ADR-067/070）。
- [x] do-runtime：host actor（facets + `getDurableObjectClass`）+ localDisk + 归属栅栏/排空/驻留（**ADR-077/078**；`cmd/do-runtime` 运行期渲染，真实 workerd e2e）。
- [x] alarm shim（**ADR-079**）、**DO 协议端到端**（**ADR-080**）、**存储生命周期**（**ADR-081**）与 **migrations v2**（**ADR-082**），**do-supervisor + 输出门**（**ADR-083**）与 **跨节点冷激活/恢复**（**ADR-084**：可逆 scope + manifest + RestoreAll；双 runtime e2e）。
- [ ] `adapters/`：binding host adapter（KV/D1/R2/Queue/Workflows/Cron/DO）。
- [x] `wrapper/groupcommit.js`：DO 组提交 + 输出门（`workerd/wrapper/README.md`，ADR-051）。
- [ ] wrapper 生成（模块包装 + env 构造 + 保留模块前缀）——参考 WDL `runtime/load/wrapper-generate.js`（加载期重写模块、透明注入平台 shim，租户代码不改）。
