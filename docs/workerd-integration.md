# workerd 集成

## 原则

**使用 stock workerd，不修改一行 C++**。只通过官方公开面使用它：

- `workerLoader`（动态加载租户 bundle，进程需 `--experimental`）；
- capnp 配置（services / sockets / bindings / `durableObjectNamespaces` / localDisk / network）；
- bindings（`globalOutbound`、service、network、DO namespace 等）；
- 进程开关与兼容配置。

## 版本 pin

- **workerd 二进制固定某个确切版本**；`compatibility_date`/`flags` 的合法集合随该版本维护（一张表）；
- 载入的 bundle 必须满足该 workerd 支持的最大 `compatibility_date`；
- **不启用**上游 `$experimental` 的租户 flag；`nodejs_compat`/`nodejs_compat_v2` 默认策略随 date 决定；
- 契约测试固定在 pin 的版本上。

ADR-186 的代码基线已固定为 stock workerd `1.20260916.1`、esbuild `0.28.2`、Miniflare `5.20260916.0-alpha`，最大 compatibility date 为 `2026-09-23`。manifest 来自固定上游 revision `adda2635656d09e541b0feeea796da9d2a8bc10e`，Go 控制面和 Bun CLI 消费同一生成产物，并由真实二进制探针交叉验证。Docker Compose 与 `REQUIRE_ALL=1` 的最终验收状态以 [`testing.md`](./testing.md) 为准。

生成与验证命令（`$WORKERD_UPSTREAM` 必须是上述 revision 的本地 checkout）：

```bash
go run ./cmd/workerd-compat-gen \
  -source "$WORKERD_UPSTREAM" \
  -version 1.20260916.1 \
  -revision adda2635656d09e541b0feeea796da9d2a8bc10e \
  -out internal/workerdcompat/manifest.json \
  -ts-out cli/src/workerd-compat.generated.ts
CELLHIVE_WORKERD="$(command -v workerd)" bash scripts/workerd-compat-probe.sh
```

## 服务与配置

| 服务 | workerd 角色 | 配置要点 |
|---|---|---|
| `user-runtime` | 租户 loader | `workerLoader`；sockets `:8081`（公开 loader）+ `:8088`（内部特权派发）；租户 loaded worker **仅公网 outbound** |
| `do-runtime` | DO host actor + facet | `durableObjectNamespaces` + localDisk；`workerLoader` 加载同一 bundle 并 `getDurableObjectClass()`；supervisor 作 PID1 |
| （可选）`system-runtime` | 平台内部 worker | 私网 + 公网 outbound（如启用平台侧 workerd worker） |

## 加载流程

```
worker id = <ns>:<worker>:<version>   （不可变）
user-runtime loader：
  1. 从路由投影得到 worker id
  2. workerLoader.get(id, () => fetchBundle(id))
       · bundle 由内容寻址（SHA-256）从对象存储拉取（scoped 只读凭据，ADR-030）
  3. 生成 wrapper（JS 层）：
       · 包装租户模块导出（fetch/scheduled/queue/alarm/RPC）
       · 构造租户 env：只放用户声明的 vars 与用户命名的 binding stub；平台键为 0（ADR-185）
       · 保留 `cloudflare:workers` 等内建模块；对 `cloudflare:workflows` 等做 shim
  4. 调用 handler
```

- **env 预算（ADR-186，已实现）**：上游 1 MiB，预留 8 KiB，CellHive 上限 **1016 KiB**；按完整实际 env 校验并计入 V8 双字节开销。超限返回 `worker_env_too_large`，只含 `actual_bytes`/`max_bytes`。
- **WorkerCode 预算（ADR-186，已实现）**：传给 `workerLoader` 的最终形态（用户模块 + wrapper + 平台注入模块）上限 **64 MiB**；控制面在事务/active pointer 前拒绝，运行时在唯一共享 `checkedWorkerGet()` 中复核。超限返回 `worker_code_too_large`。
- **宿主秘密（ADR-186，已实现）**：平台 URL/token 通过 capnp `fromEnvironment` 只进入可信宿主 binding；user-runtime、do-runtime 与 do-supervisor 都给 workerd 构造显式子进程环境，不继承父进程环境。秘密不得出现在渲染 capnp、最终 WorkerCode、租户 env、参数或日志。
- **secret 边界**：secret 可加密存储和管理，但尚未注入 runtime env；因此不能把它计作上述 env 的一个来源。补齐注入时仍不得引入平台键或保留名称。
- **Workflow / 日志边界**：workflow 固定 op 回调由可信 internal host 创建、带 dispatcher-bound 身份的 `WorkflowBridgeTarget extends RpcTarget` 经 JSRPC 参数跨 `workerLoader` 传给 wrapper；它不是 `ServiceStub`，也不进入 tenant env。已在 pin `1.20260916.1` 重跑动态 Tail spike，仍被拒绝（`provided value is not of type 'Fetcher'`），故租户 `console.*` 平台采集当前关闭，不能以 env 传输回退。
- **模块前缀保留**：平台生成的模块名使用保留前缀（如 `__cellhive-`），租户不得占用。

## host adapter 与网络

- 每个 binding 生成 **binding-scoped facade**，props 不可变（`ns` + binding 类型/id）；
- facade 调 `cell-agent`（`:7001` REST/JSON），带 **scope 声明**（ADR-029）；
- **租户 loaded worker 的 `globalOutbound` = 公网 only**，不含 RFC1918/cell-agent；
- 平台代码（loader/host adapter/DO host actor）使用单独的 private network binding。

## 限制

- `limits`：每 worker `cpu_ms`/`subrequests`（workerd config）；V8 heap 上限（进程/isolate 级）；
- 不支持：Python Workers、Cache API、Browser Rendering、Email Workers、Analytics Engine；Vectorize 与 Hyperdrive 的 CellHive 兼容子集见兼容矩阵；
- 不保证兼容所有历史 `compatibility_date` 行为——按我们支持的 flag 集合为准。

## P0 验证项（与 DO/WAL 一起）

1. actor SQLite 是否 WAL、可被外部进程只读打开；
2. checkpoint/截断可检测、可对齐；
3. actor 文件布局（共享 metadata + 每 actor）；
4. `globalOutbound` 公网限制配置生效；
5. `workerLoader` 在当前 pin 版本下加载/驱逐行为正常；
6. env 预算校验与实际一致。

## 待细化

- capnp 配置的具体片段（user-runtime / do-runtime）；
- wrapper 生成细节与保留模块名前缀；
- 兼容 flag 表（跟随 pinned workerd）。

## 升级与回滚

1. 先部署能读取旧 artifact 与旧 DO working copy 的新 reader/runtime，再让控制面接受新 date/flag（reader-before-writer）。
2. workerd 二进制、`internal/workerdcompat/manifest.json`、`cli/src/workerd-compat.generated.ts` 与 Miniflare pair 必须作为一组升级；不得只换其中一项。
3. 回滚前枚举 active versions 并用目标旧 pin 验证 date/flag。任一 active version 依赖旧 pin 不认识的 date/flag 时，拒绝回滚，先 promote 到兼容版本。
4. esbuild 回滚只影响后续构建；已有内容寻址 bundle 不重写。此阶段没有 schema、cell/owner/epoch 或 RPO=0 协议迁移。

_最后更新：2026-09-22_
