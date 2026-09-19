# Durable Objects

> **状态（2026-09-15）**：**host-actor 骨架 + 归属/栅栏/排空/驻留已实现并验证**（ADR-077/078：facets + `workerLoader.getDurableObjectClass` + localDisk；`/v1/do/invoke`；shard、owner record + **单调 generation**（`owner-gen`）、pre-dispatch guard/renew、`/v1/do/drain`、`DO_PREVENT_EVICTION` resident/evictable；真实 workerd e2e）。
> **alarm（ADR-079）**：facet 原生 alarm 不可用（pinned `alarms are not yet implemented for SQLite-backed Durable Objects`；最新 2026-09-15 仍 `Facets currently cannot set alarms`）→ 平台 shim：`cellhive-do.js` 基类把 setAlarm/getAlarm/deleteAlarm 落到对象 storage 保留键，host 上报 cell-agent，统一 timer（`KindDOAlarm`）+ `DoAlarmDispatcher` → do-runtime `kind:"alarm"` 调 `alarm()`；真实 workerd e2e 通过。**CF 差异**：无 native retry 计数、`transactionSync` 内不支持、无 per-alarm lease。
> **WebSocket（ADR-080）**：`GET /v1/do/connect` 原样升级到 facet（hibernation API），重启/abort 先 `__chCloseAll(1012)` 再 `facets.abort()` → 客户端收 1012（真实 bun e2e）。**租户接线（ADR-175）**：`env.DO.get(id).fetch(升级请求)` 由 facade 检测 `Upgrade` 后，先向 cell-agent `GET /v1/do/connect` 取 `{owner,ticket}`（scoped token），再带 ticket 直连 owner 的 `/v1/do/connect`；do-runtime 允许 ticket 用于 connect 且校验仅本 shard。

> **legacy DO 类（ADR-174）**：只实现 `fetch/alarm/webSocket*`、未 `extends DurableObject` 的类由 do-runtime 的 facet 入口模块包装（`renderFacetModule`）后支持；`env.DO.get(id).fetch()` 的非 2xx 响应原样返回。
> **DO 客户端绑定（ADR-080）**：`env.DO.get(...).fetch()` → `scopeAuth("do")` 代理按 shard 放置到 `CELLHIVE_DO_RUNTIMES`；owner 冲突转发一次 + hint；`result_unknown` 语义。
> **存储生命周期（ADR-081）**：代码变更用 **facet 级惰性重启**（版本戳 → `facets.abort()` 单个对象，storage 不变）；`migrations` 仅允许 `new_classes`/`new_sqlite_classes`，`renamed/deleted/transferred` 拒绝；**`doStorageId`** 作为稳定存储身份（重部署不变）。
> **migrations v2（ADR-082）**：**对象注册表**（`GET /v1/do/objects` + cell-agent 聚合）；**rename**（`renamed_classes`，别名 `codeClass→storageClass`，不迁文件保数据）；**delete**（`deleted_classes` 标记 + `POST /v1/do/delete` 物理回收）；**主动重启**（`CELLHIVE_DO_EAGER_RESTART`，默认惰性）。
> **持久性（ADR-083，部分）**：`do-supervisor` 以 **shard（host actor 目录）粒度**捕获全部 `*.sqlite`（LTX 快照+增量 → fleet/bucket proof），并**门控** do-runtime 响应（未证明不 ack；失败 `result_unknown`）。**发现**：`.facets` 是 workerd 内部二进制格式，故不解析、按 shard 处理。
> **跨节点冷激活（ADR-084，部分）**：`scopeID` 可逆（base64url）+ manifest + **`RestoreAll`**（LTX `replica.Restore`+`restore.ApplyFile`，sidecar 逐字回写）；**双 runtime e2e** 通过（A 运行→捕获到 bucket→B 恢复→状态延续）。host 文件名跨进程确定性是"恢复到原路径"成立的前提。
> **DO 内 bindings ✅（ADR-090）**：平台导出 `WorkerEntrypoint` 能力类，`ctx.exports.X({props})` stub 放入 facet env；KV/D1/R2/Queue 走 stub、DO/Workflow 经 env-patch → **`this.env` 与构造函数形参 `env` 都可**。
> **`deleteAll()` shim ✅**：stock workerd 对 SQLite facet 的 `deleteAll()` 抛内部错误；基类改为清 KV + drop 用户表 + 清 alarm（`cellhive-do.js`）。
> **facet 发现 ✅**：`.facets` 格式已解析（magic + `[flag][len][name]`，条目 i→`<hash>.<i+1>.sqlite`）；**按需分页 ✅**（`CompactAll` + `PageFetcher.Materialize`，每页 ranged 读）；**接管 ✅**（`TestDoRuntimeTakeoverAfterCrash`）；**compat 套件 ✅**（`TestDOCompatSuite`，19 子测试）。
> **对象键 ✅**：`ctx.id.toString()` = `<hosthash>`、`ctx.id.name` = hostId（含 storage_id）→ host.js 上报 → supervisor 按 `storage_id/<class>/<objectName>` 复制 facet 文件；`RestoreObject` 按对象冷启动（ADR-084）。
> **P3 剩余（非功能缺口）**：① 运行期 SQLite VFS 懒读 —— **DO 侧** stock workerd 下不可实现（ADR-085，替代=冷启动按对象/按页 materialize）；cell-agent 侧已实现（ADR-160，`internal/pagedvfs`；捕获/快照前强制 hydrate）；② **C 类环境验证**（真实跨主机 RTT、云端对象存储、多主机混沌/接管）缺环境；③ **跨 worker `transferred_classes`** 有意拒绝（同 worker 支持，见 wrangler-compat）；④ worker↔worker 已是同实例原生 JSRPC（ADR-102），DO RPC 已支持 JSON+tagged（ADR-162）；剩余仅为 RPC 跨 `ReadableStream`/`RpcTarget` 传递。

## 定位

DO 与 KV 在**结果上等价**（对象存储权威 + bucket 租约/epoch + 持久寻址 + RPO=0），但**机制弱一档**：KV/D1 由 `cell-agent` 自己拥有 SQLite（精确控制事务与捕获），DO 的 SQLite 由 workerd 拥有、只在 `do-runtime` 本地作**工作副本**，持久化经 `cell-agent`。

这是"保留 stock workerd + 不修改 workerd + 保留 DO 同步 SQL"三条约束的必然代价。

## 为什么必须是原生 facet

Cloudflare/workerd 的 `ctx.storage.sql.exec()` 与 `storage.transactionSync()` 是**同步** API；KV/D1/Queue/Workflows/R2 都是异步 Promise API。

- 进程外存储 + 同步 SQL：workerd 不提供同步 JS→host 调用（无同步 FFI、无 SharedArrayBuffer/Atomics 阻塞），在不改 workerd 的前提下无法实现；
- 因此 DO 的 SQLite 必须留在 workerd 进程内 → 使用 **workerd 原生 Durable Object facet**；
- 其余异步绑定才可以走 `cell-agent`。

## do-runtime 的组成

`do-runtime` 是**分布式弹性执行层**：节点可随时增删，本地盘**可临时**（权威状态在 cell-agent）。

| 部分 | 作用 |
|---|---|
| **workerd** | 原生 DO host actor（平台自有 worker）：owner 路由入口 + facet 执行器；配置含 `durableObjectNamespaces` + localDisk；用 `workerLoader` 加载租户不可变 bundle 并 `getDurableObjectClass()` 解析用户类 |
| **Go supervisor（PID1）** | 拉起/监控/重启 workerd；向 cell-agent claim/续约 owner；SIGTERM 时 drain；**WAL 捕获模块**（原“sidecar”，**并入 supervisor，非独立程序**）把 WAL 段上报 cell-agent；暴露 `/state` |

supervisor **不持对象存储凭据**；捕获的 WAL 段一律交给 `cell-agent`。

## 请求生命周期（跨节点）

```
租户 Worker (user-runtime)
  env.NS.get(id).fetch(req) / RPC / WS
    → host adapter（binding-scoped，不可变 props）
  cell-agent 解析 owner：(ns, class, objectId) → owner **do-runtime 节点**地址 + epoch
    → 连接 owner do-runtime
  owner do-runtime：
    1. 校验 节点存活 + owner 记录 + epoch
    2. workerLoader 加载租户不可变 bundle（缓存）
    3. WorkerStub.getDurableObjectClass() 取用户 DO 类
    4. 以原生 facet 执行（input gate / 同步 SQL / 事务 / alarm / WS）
    5. 写本地工作副本 SQLite
    6. supervisor 捕获 WAL → 上报 cell-agent → cell-agent 写 e<epoch> 前缀 + 持久化证明
    7. 【输出门】等 cell-agent 确认本次提交已持久 → 返回
```

- **执行永远在 owner do-runtime 节点**；非 owner 只转发一次（并带回可信 owner 提示供缓存刷新）。

## DO RPC（ADR-162）

`env.NS.get(id)` / `env.NS.getByName(id)` 返回的 stub 除了 `fetch()`，还支持**直接调用租户 DO 的方法**：

```js
import { DurableObject } from "cloudflare:workers";

export class Room extends DurableObject {
  async addMessage(user, text) {
    const n = (await this.ctx.storage.get("n")) || 0;
    await this.ctx.storage.put("n", n + 1);
    return { n: n + 1, user, text, at: new Date() };
  }
}

export default {
  async fetch(req, env) {
    const stub = env.ROOM.getByName("room-1");
    return Response.json(await stub.addMessage("u1", "hi"));
  },
};
```

- **路径**：stub 把 `method`/`args` 编成 tagged JSON → `POST /v1/do/invoke`（`kind:"rpc"`，与 fetch 同一条 owner-hint/ticket/`409/5xx 不重放`通路）→ do-runtime host 用**原生 JSRPC** 调 facet 方法 → 结果 tagged 回传。host 调 facet 用 structured clone，因此 Map/Date/ArrayBuffer 等无损；跨进程那一段由 tagged 编解码承载。
- **支持的值**：`undefined`、`-0`/NaN/±Infinity、bigint、Date、RegExp、Map、Set、ArrayBuffer、TypedArray/DataView、Error（含 `cause`）、URL、URLSearchParams、共享引用/环。类实例降级为普通对象（与 structured clone 的可观察行为一致）。
- **限制**：函数、Symbol、Promise、WeakMap/WeakSet、`ReadableStream`/Response、`RpcTarget`、DO stub 不能通过；`args` 与结果各 ≤ **8 MiB**（tagged 编码后；二进制经 base64 膨胀 ~4/3）；方法名必须是标识符，且不能是 `fetch`/`alarm`/`constructor`/`__proto__`/`then`/`toString` 等保留名或 `__ch*` 平台方法；handler 抛错在调用方表现为带 `code`/`name`/`message`/`stack` 的 `Error`。RPC 没有 tenant `Request` 对象，故不携带 traceparent（仅 requestId 关联）。
- **DO 不对外可寻址**：公开 WebSocket 由租户 Worker（`user-runtime`）接住，再用 binding 代理到 owner；`Traefik`/公网不直接接触 do-runtime。
- **输出门**在 owner do-runtime 返回前完成，保证 RPO=0。

## cell-agent ↔ do-runtime 内部契约

`cell-agent` 给 DO 的**不只是 owner 地址**，而是完整的持久化读写端点。do-runtime **不碰对象存储**；本地 SQLite 只是工作副本；所有 LTX 读写都经 cell-agent。

| 能力 | 内部 API | 说明 |
|---|---|---|
| 解析 owner | `resolve(scope)` | 返回 owner do-runtime 地址 + epoch |
| claim / 续约 | `claim(scope, advertise)` / renew | 协调并写 owner 记录（bucket）；处理接管 |
| **LTX 写入接收** | `append(scope, epoch, segment)` / `sync(scope)` | 接收 supervisor 捕获的 WAL/LTX 段，写 `cells/<scope>/ltx/e<epoch>/` + 持久化证明 |
| **恢复读取** | `restore(scope, epoch)` | 下发快照 / epoch 链；do-runtime 物化本地 SQLite |
| 输出门确认 | `sync(scope)` | 返回"本次提交已持久"的证明 |
| alarm 兜底 | waker | owner 死后的遗留 alarm 由 cell-agent 侧 waker 接管 |

**写路径**：`workerd 提交 → supervisor 捕获 WAL 段 → cell-agent.append/sync → cell-agent 写 e<epoch> 前缀 + 证明 → 输出门 → 返回`
**读路径（冷激活/接管）**：`do-runtime → cell-agent.restore → cell-agent 读对象存储（epoch 链/快照）→ 下发 → 物化本地 SQLite`

物理数据位于对象存储的 `cells/<scope>/ltx/e<epoch>/...`，**唯一访问者是 cell-agent**。

## DO owner claim 协议（C-01 fix）

do-runtime 的 supervisor 主动向 cell-agent 发起 claim。流程：

1. **冷启动/激活触发（D 定稿，cell-agent 驱动）**：user-runtime 的 host adapter 收到对 DO `(ns, class, id)` 的第一次调用 → cell-agent 的 resolve 发现无 owner record（或已过期）→ cell-agent **选一个 do-runtime 候选节点**（平台服务发现/配置列表）→ 向该节点发 `activate(scope)` 通知；do-runtime 收到后向 cell-agent 发 `claim(scope, advertise)`。（do-runtime 自 claim 仅作为内部回退，非主路径。）
2. **cell-agent 执行 claim**：条件写 `cells/<scope>/owner.json`（`If-None-Match: *` 或 CAS）。内容：`{node, session, epoch+1, expiry, address=do-runtime-advertise}`。**cell-agent 是唯一写桶者**，代理 do-runtime 完成。
3. **claim 竞争**：若多个 do-runtime 同时发 claim，bucket 条件写保证只有一个成功；失败者从响应中读到胜者，转发请求。
4. **claim 成功响应**：cell-agent 返回 `{epoch, expiry}` 给 supervisor，supervisor 接受后开始 restore（若已有数据）再进入就绪。
5. **续约**：supervisor 持续向 cell-agent 发 renew（携带 session+epoch），cell-agent 通过 CAS 更新 expiry。**续约失败（cell-agent 拒绝）意味着 epoch 已变（被接管）**→ supervisor 必须自 fence 并重启。
6. **claim 超时**：输出门有超时（见 known-issues.md #C-07）；若无法在 deadline 内获得证明，返回 `result_unknown`。

## DO 冷激活 / 恢复流程（C-04 fix）

| 步骤 | 操作 | 说明 |
|---|---|---|
| 1 claim 成功 | cell-agent 写 owner record | epoch = prev+1 |
| 2 check restore needed | cell-agent 检查对象存储是否有 `e<prev_epoch>` 数据 | 全新 DO → 跳至步骤 5 |
| 3 restore stream | `cell-agent.restore(scope, epoch)` 向 supervisor 流式发送：先快照（若有）再 delta LTX 链 | supervisor 接收并写入本地临时文件 |
| 4 物化 SQLite | supervisor 通知 workerd（通过 localDisk 配置加载该文件）或替换本地 SQLite 文件后重启 workerd actor | workerd 读到正确状态 |
| 5 就绪 | supervisor 通知 cell-agent `ready(scope, epoch)` | 请求开始派发 |

- **恢复失败/中断**：supervisor 回到步骤 1 重新 claim（新 epoch）；cell-agent 丢弃不完整数据。
- **并发请求**：restore 期间到达的请求在 cell-agent 侧排队，直到 `ready` 为止。
- **大快照**：支持流式 paging（稀疏文件按需下载），supervisor 可在未完整恢复时就绪（仅已下载页可用）。

## DO 生命周期中的 in-flight 请求（C-07 fix）

| 情况 | 行为 |
|---|---|
| 请求 in-flight，owner lease 到期 | 输出门检测到 cell-agent 返回 epoch mismatch → 返回 `result_unknown` 给 host adapter |
| 非幂等请求，host adapter 收到 `result_unknown` | **不重放**；返回 `result_unknown` 给 Worker（由 Worker 决定重试） |
| 幂等 GET/HEAD 类，收到 stale-owner 错误 | host adapter 重新 resolve owner 并重试**一次** |
| 输出门等待证明超时 | cell-agent 返回 `timeout`；supervisor 回复 host adapter `result_unknown` |
| WS 连接，owner 迁移 | owner 以 **1012** 关闭 WS；**host adapter 收到 1012 → 重新 resolve owner → 建立新 WS → 继续代理**（对 browser client 透明）；stable operation id 用于幂等性 |

**output gate 超时默认 10s**（可配置 `DO_OUTPUT_GATE_TIMEOUT_MS`）；超时后 supervisor 自 fence，host adapter 得到 `result_unknown`。

## ownership 与持久寻址

- 粒度：**每个 DO 对象一个 owner/lease**（scope = `(ns, DoClass, objectId)`），（一个 cell 就是一个 DO）。
- **owner 记录指向 do-runtime 执行节点**（含 advertise 地址 + epoch），由 **cell-agent 写**。
- **无专门心跳**：do-runtime 存活由**复制活动隐式判定**——持续向 cell-agent 上报 WAL/续约即视为存活；停止上报 → owner 租约到期 → 可接管。
- **接管候选**走**平台服务发现**（K8s Endpoints / mesh）或按需探测；不维护自注册表。
- **router 缓存**：正常请求只查 cell-agent 的 owner 解析缓存；仅冷激活、缓存失效、接管时读 bucket（点查）。
- WebSocket `connect` 必须落在 owner；owner 迁移时以 **1012** 关闭，客户端携带**稳定 operation id** 重连。

## 复制：WAL 捕获 → cell-agent（默认）与 FUSE（可选）

### 默认：supervisor 捕获 WAL → cell-agent（可移植）

- supervisor 与 workerd 同节点、同卷，**只读**打开 actor SQLite 与 `-wal`；
- 读取并记录 WAL 的 salt/frame 位置，把新的已提交帧**上报给 cell-agent**（可能跨节点，走内网）；
- `cell-agent` 执行 cell 协议：写 `e<epoch>` 前缀 + 持久化证明；检测到 checkpoint/截断时，改为全量快照重新对齐；
- **输出门**：host adapter 返回前调用 cell-agent 的 `sync(scope)`。

### 可选：FUSE 卷（仅自管环境）

- 把 workerd localDisk 目录放到 FUSE 卷上，由 FUSE 层负责"写落本地 + 复制 + `fsync` 即持久"（LiteFS 模式）；
- 优点：写时拦截，`fsync` 返回即证明，免单独输出门；
- 代价：需 `/dev/fuse` + `SYS_ADMIN`/privileged（托管/受限集群常禁用）、"workerd on FUSE" 无先例、SQLite 语义与性能需验证。

> **默认 WAL 捕获 → cell-agent**（适配托管/受限集群、可移植）；FUSE 作为自管环境的可选加速，不在 P0 关键路径。

## 为什么能捕获却不是"拦截"

- **拦截（hook）**（SQLite `update_hook`/session extension）必须在持有连接的进程内 → 我们够不到 workerd 的连接；
- **观察（read）**：另开只读连接读取 WAL 文件（Litestream 方式）→ supervisor 采用此路；
- RPO=0 不靠"捕获事件"，而靠**门控**：handler 返回后读当前 WAL 末端，经 cell-agent 确认已持久（提交有序追加，覆盖刚提交的事务）。

## Alarms

- alarm 行存在于 actor SQLite（随复制一起走），不额外建 bucket 索引；
- 每个 owner 的 supervisor 为**自己拥有且常驻**的对象维护本地 due 结构，到点本地唤醒 → 零 bucket 交互；
- 单 fleet **waker**（cell-agent 侧的 bucket 租约）兜底 owner 已死/失联对象的遗留 alarm；
- 交付 at-least-once；迁移时 alarm 状态随对象工作副本复制到新 owner。

### alarm 恢复（C-05 定稿）

1. **supervisor 只读 workerd 的 alarm 表**，提取到期时间，上报 cell-agent：`alarm_upsert(scope, dueAt, token)` / `alarm_delete(scope, token)`。
2. cell-agent 在 **waker cell** 维护 due 索引（按时间分桶，**不扫 bucket**）。
3. waker 发现 "owner 已死 + 到期" 的项 → **定向触发该 DO 的 takeover**（claim + restore）到可用 do-runtime；新 owner 读 alarm 行派发；成功后原子推进/删除索引项。

> **状态（2026-09-15 更新）**：**已实现（ADR-079，机制改为 shim）**。原计划的"supervisor 只读 workerd alarm 表"被取代：pinned workerd 不支持 SQLite-backed DO 的 `setAlarm`，故 `cellhive-do.js` 基类把 `set/get/deleteAlarm` 落到对象 storage 保留键 `__cellhive_alarm`，host 上报 `/v1/internal/do/alarm/upsert` → 统一 `KindDOAlarm` timer（scope `ns/__timer__/do`）→ `DoAlarmDispatcher`（`internal/dispatch/doalarm.go`）→ 解析 owner → do-runtime 以 `kind:"alarm"` 调用 `alarm()`。**已实测**（本页"已验证"：`TestDoRuntimeAlarmShim`；`_cf_ALARM` schema 见 known-issues C-05）。
4. **带 alarm 的 DO 优先常驻**（放置策略），减少冷激活。
5. 若 P0 发现无法从 alarm 表可靠提取，回退：waker 定期向 live supervisor 询问各自最早到期 alarm，仅对 owner 已死者做 takeover。

### WebSocket 跨节点转发（ADR-084 ✅）

- 连接落在**非 owner** 节点时，`host.js` 把升级后的 socket **代理**到 owner 的 do-runtime（`proxyConnect`：`fetch(owner/v1/do/connect)` 取 `response.webSocket`，用 `WebSocketPair` 双向透传帧）。
- **不 resume**：owner 侧 `facets.abort()` 的 **1012** 经代理**透传**给客户端（`TestDoRuntimeWebSocketCrossNodeForward`：B 代理到 A，A abort → 客户端收 `CLOSE:1012`）。
- owner 死亡 → claim 过期后新节点接管（冷激活）后本地建 socket；客户端按 CF 语义重连。

### WebSocket 语义（A 定稿，**CF 兼容**）

- **部署 / promote / 迁移都重启 DO**：旧 owner 以 **1012** 关闭 WS；**客户端重连**；新 owner **重新构造** DO，**handler 重启**（`webSocketOpen` 会重新触发）。
- **不做 resume**：平台**不保证**跨重启的会话连续；DO 会话状态必须落存储（hibernation 模型），内存状态不保留。
- user-runtime 的 host adapter 仅负责：检测 1012/断连 → 重新解析 owner → 让客户端重连（**不自行恢复会话**）。
- 与 Cloudflare 行为一致（部署即重启对象）。

## DO 生命周期（deploy 侧）

- 支持新声明式 `exports`（`type: durable-object` + `storage: sqlite`）与旧命令式 `migrations`（`new_classes` / `new_sqlite_classes`）；
- **拒绝**：`renamed_classes`、`deleted_classes`、`transferred_classes`、跨 worker `script_name`（同 worker class 才支持）；
- 详见 [`wrangler-compat.md`](./wrangler-compat.md)。

## P0 验证项

1. workerd 的 actor SQLite 是否为 **WAL 模式**；
2. 能否被**另一进程只读打开**并读到已提交事务；
3. checkpoint/截断行为是否可被 supervisor 检测并对齐；
4. **跨节点**"捕获→cell-agent→证明"的 p50/p99（写 ack 热路径）；
5. do-runtime **冷激活/恢复**时间（按对象大小，临时盘每次都可能冷启）；
6. do-runtime 增删时 DO 迁移的无损与耗时。

若 1–3 不成立：退化为周期快照（RPO=间隔）或（自管环境）FUSE。

## 与 KV 的对照（重要）

| 维度 | KV / D1（cell-agent） | DO（workerd 原生 + cell-agent 持久化） |
|---|---|---|
| 工作 SQLite 写者 | cell-agent（Go，精确控制） | workerd（do-runtime 本地） |
| 捕获 | 自己控制事务/WAL（可 hook、可调 checkpoint） | supervisor 外部只读观察 WAL |
| 持久化端点 | cell-agent 自己 | cell-agent（do-runtime 无桶凭据） |
| 权威在对象存储 | 是 | 是 |
| RPO=0 | 门控在 cell-agent 内 | 门控在 host adapter + supervisor + cell-agent |
| owner 节点 | cell-agent 节点 | **do-runtime 节点** |
| 用户可见结果 | 基准 | **等价**（数据在对象存储、可接管、RPO=0） |
| 机制 | 完全可控 | 弱一档，依赖 WAL 可观察性与跨节点上报 |

## 风险

- workerd 的 WAL/checkpoint 语义未验证（P0 消除）；
- actor 存储为多文件（共享 metadata + 每 actor），一致性复制更复杂；
- **跨节点上报在写 ack 热路径**上，需内网低延迟与容量规划；
- 临时盘 ⇒ 冷激活需从 cell-agent/对象存储恢复，冷启动成本是弹性代价；
- 自研 PID1 supervisor 需正确编排跨进程关停顺序。

_最后更新：2026-09-19_
