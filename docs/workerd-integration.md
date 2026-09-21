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

ADR-186 已批准、正在实施的新基线固定为 stock workerd `1.20260916.1` 与 esbuild `0.28.2`。完成前，代码中的现行 pin 和最大 compatibility date 仍是运行事实；不得把设计值误报成已部署。新基线要求 compatibility manifest 从固定上游源码生成并由真实二进制交叉验证。

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

- **env 预算（ADR-186，实施中）**：上游 1 MiB，预留 8 KiB，CellHive 上限 **1016 KiB**；按完整实际 env 校验并计入 V8 双字节开销。
- **WorkerCode 预算（ADR-186，实施中）**：传给 `workerLoader` 的最终形态（用户模块 + wrapper + 平台注入模块）上限 **64 MiB**；控制面前置拒绝，运行时防御复核。
- **宿主秘密（ADR-186，实施中）**：平台 URL/token 通过 capnp `fromEnvironment` 只进入可信宿主 binding，不得出现在渲染 capnp、最终 WorkerCode、租户 env 或日志。
- **secret 边界**：secret 可加密存储和管理，但尚未注入 runtime env；因此不能把它计作上述 env 的一个来源。补齐注入时仍不得引入平台键或保留名称。
- **Workflow / 日志边界**：workflow 固定 op 回调由可信 internal host 创建、带 dispatcher-bound 身份的 `WorkflowBridgeTarget extends RpcTarget` 经 JSRPC 参数跨 `workerLoader` 传给 wrapper；它不是 `ServiceStub`，也不进入 tenant env。pin `1.20260615.1` 的动态 loaded worker Tail Worker 被拒绝（`provided value is not of type 'Fetcher'`），故租户 `console.*` 平台采集当前关闭，不能以 env 传输回退。
- **模块前缀保留**：平台生成的模块名使用保留前缀（如 `__cellhive-`），租户不得占用。

## host adapter 与网络

- 每个 binding 生成 **binding-scoped facade**，props 不可变（`ns` + binding 类型/id）；
- facade 调 `cell-agent`（`:7001` REST/JSON），带 **scope 声明**（ADR-029）；
- **租户 loaded worker 的 `globalOutbound` = 公网 only**，不含 RFC1918/cell-agent；
- 平台代码（loader/host adapter/DO host actor）使用单独的 private network binding。

## 限制

- `limits`：每 worker `cpu_ms`/`subrequests`（workerd config）；V8 heap 上限（进程/isolate 级）；
- 不支持：Python Workers、Cache API、Vectorize、Hyperdrive、Browser Rendering、Email Workers、Analytics Engine（与 workerd 暴露面一致）；
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

_最后更新：2026-09-22_
