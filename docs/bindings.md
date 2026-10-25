# 绑定映射

> **原生扩展（2026-10-15）**：原生 KV `expiration_ttl` 支持 1 秒起；Cloudflare 管理兼容入口仍 >=60 秒。原生 `incr` 返回 `value_i64` 十进制字符串供 JS `BigInt()` 无损读取，保留旧数字 `value`。Queue 生产者幂等独立保留首次发送后的 24h，metadata 指纹规范化 JSON、数字不经过 float64；DLQ/重放完整保留 metadata 与业务幂等键，转移使用独立 receipt。delay 不属于逻辑指纹，重复发送不会更改首次消息的投递时间。详见 [合同](superpowers/specs/2026-10-15-kv-queue-native-semantics.md)。

Worker 通过 `env` 看到 Cloudflare 形态的 binding；平台用 **host adapter**（workerd 内的平台代码）把它转成对 `cell-agent` 的调用。binding 由不可变 props 唯一绑定到具体 cell。

## 映射表

| CF binding | 后端 | API 形态 | 存储 | owner |
|---|---|---|---|---|
| **KV** | KV cell | 异步 | SQLite（`__kv__` cell） | cell-agent 节点 |
| **D1** | D1 cell | 异步（`prepare/bind/all/first/run/raw/batch/exec`） | SQLite（`__d1__` cell） | cell-agent 节点 |
| 　└ binding/id | `binding` 只是 Worker 里的 env 变量名（如 `DB`）；`database_id`（API 的 `id`）才是 D1 资源/data cell 的 id，运行时和部署校验都以它为准。不要再把 binding 名当成 database id。 | | | |
| 　└ 契约保真 | `prepare()`/`bind()` 是租户 isolate 内同步本地对象，可直接传给 `batch()`；TypedArray 按 BLOB 往返；`run()` 返回 `results: []`；`exec().count` 是 SQL 语句数；`meta` 含常用 `changes/last_row_id/changed_db/duration/rows_read/rows_written`；`withSession()/session()/getBookmark()` 已提供强一致 txid bookmark；`dump()` 返回脱敏 SQLite 快照（32MiB 上限）；输入限制对齐 SQL 100KiB/参数100/值和结果行2MiB/查询30s；D1 connection 还设置列数、LIKE、compound、表达式、VDBE、函数参数、trigger 深度和 ATTACH limits，并用 SQLite authorizer 拦截临时对象/动态扩展；禁止 `BEGIN/COMMIT/ROLLBACK/ATTACH/VACUUM/load_extension`；错误 `err.name=D1_ERROR` + `err.code`（ADR-163）。不提供 Cloudflare read replica 路由/region 语义。 | | | |
| **R2** | 对象存储 S3 兼容 | 异步 | `r2/<ns>/<bucket>/...` + `r2meta/...`（metadata sidecar） | 对象存储 |
| 　└ 契约保真 | `R2Object/R2ObjectBody` 全字段（`key/version/size/etag/httpEtag/uploaded/httpMetadata/customMetadata/checksums` + `text/json/arrayBuffer/blob`）；`put` 支持 `{httpMetadata,customMetadata,md5,sha256}`（校验）；`head()`；list `truncated/cursor` + `include`（按需 metadata）+ `delimiter`/`delimitedPrefixes`（ADR-163/168） | | | |
| **Queues 生产者** | Queue cell | 异步 | SQLite（`__queue__` cell） | cell-agent 节点 |
| **Queues 消费者** | `queue()` handler | 派发 | 同上 | 由 cell-agent 派发 |
| **Workflows** | Workflow cell + 引擎 | 异步 | SQLite（`__workflow__` cell） | cell-agent 节点 |
| **Cron** | cron 投影 cell | `scheduled()` | SQLite（`__cron__` cell） | cell-agent 节点 |
| **Durable Objects** | do-runtime 原生 facet | **同步 SQL**；绑定 `env.DO.get(...).fetch()` 与 **`getByName(...).method(...)` DO RPC**（ADR-080 shard/owner/hint/WS 1012；ADR-162 RPC tagged JSON + 原生 JSRPC） | workerd actor SQLite（工作副本）→ cell-agent 复制 | **do-runtime 节点** |
| **ASSETS** | 对象存储（版本化） | 异步 | `assets/<ns>/<worker>/<token>/...` | 对象存储 |
| **Service bindings** | workerd JSRPC | 同步/异步 | — | 目标 Worker |
| **Vars / Secrets** | vars、当前 Worker 版本的 managed secrets 与用户命名 binding stub 在加载期注入 `env`；secret 与同名 var 冲突时覆盖 var；secret 变更创建并发布新版本（ADR-204） | — | secrets 在 control cell 按版本保存信封密文 | cell-agent |
| **AI** | BYO OpenAI 兼容端点（`env.AI.run`） | 异步 | — | cell-agent（`CELLHIVE_AI_URL`/`CELLHIVE_AI_KEY`；无平台托管目录） |

## host adapter 模型

- 每个 binding 在加载期生成一个 **binding-scoped facade**，props 不可变（`ns` + binding 类型/id）；
- 租户代码只拿到 facade；**拿不到** internal token、后端地址、通用 Fetcher；
- facade 由平台侧 RPC entrypoint stub 持有可信 private transport（ADR-184），调用 `cell-agent`（`:7001` REST/JSON）；租户 loaded worker 的 `globalOutbound` 仍为 public-only；
- facade 只带 **`x-cellhive-scope-token`**（平台加载期本地 HMAC 计算、无过期，ADR-074）；`ns/kind/name` 在同一 token 内，cell-agent 校验签名 + `HasBinding`。**不再带**广权限 internal token。
- KV `get()`/`getWithMetadata()` 在同一个 stub、同一 microtask 内自然并发时合并为一次 `POST /v1/kv/get-batch`（每批最多 256 key）；不设置等待定时器，单次读取只延后到 microtask checkpoint。cell-agent 只做一次 owner 路由，并在同一 SQLite 连接/只读事务内按输入顺序点查；缺失、重复、metadata 与 `text/json/arrayBuffer` 语义保持不变。

## 同步 vs 异步

| | API | 说明 |
|---|---|---|
| KV/D1/R2/Queue/Workflows/Cron/ASSETS | **异步 Promise** | 可走进程外 `cell-agent`（`cell-agent` 拥有 SQLite） |
| **DO `ctx.storage.sql` / `transactionSync`** | **同步** | 必须留在 workerd 进程内 → 原生 facet；持久化经 supervisor→cell-agent（见 [`durable-objects.md`](./durable-objects.md)） |

## 不支持（显式拒绝）

`Vectorize`、`AI Search`、`Dispatch Namespaces`、`Secrets Store`、`send_email`、`Browser Rendering`、`Images`、`Cache API`、`Python Workers`、`Analytics Engine` 等（与 workerd 暴露面一致）。**Hyperdrive 已支持**（`connectionString` + host/port/user/password/database；origin URL 以注册资源形式**信封加密**存于控制面，按名解析，ADR-125/129）。

**本地连接复用（ADR-130）**：平台不做池化；workerd 的普通 handler **不能**跨请求复用连接（请求级 I/O），但 **Durable Object 可以**——把 `connect()` 的连接存到 DO 实例字段即可在 DO 生命周期内复用（含驱动自带的池）。形态：

```js
import { DurableObject } from "cloudflare:workers";
import { connect } from "cloudflare:sockets";
export class DbPool extends DurableObject {
  async fetch(req) {
    if (!this.conn) {                      // 每个 DO 实例一条（或 N 条）连接
      this.conn = connect("db.internal:5432");
      this.w = this.conn.writable.getWriter();
      this.r = this.conn.readable.getReader();
    }
    // ...用 this.w/this.r 跑查询协议，跨请求复用...
  }
}
```
连接随 DO 生命周期（驱逐/重部署/owner 变更时重建；`/v1/do/abort` 即断）。默认只允许 public 出网；`CELLHIVE_TENANT_OUTBOUND=public,private` 可放开私有网段源库（放松 I-09，运维决定）。

## Assets 服务（ADR-069，已实现）

loader 解析出活跃 version 后，若 `assets_sha` 非空则**资产优先**：路径归一化 + 目录 index、按扩展名 content-type、ETag/`If-None-Match`→304、`_headers`（路径块叠加响应头）、`_redirects`（301/302 + `200` 重写 + splat）、未命中**回退 worker**。字节经 cell-agent 内部 `GET /v1/internal/asset`（不直连桶）。**路由配置（ADR-071）**：`not_found_handling`（`none`/`404-page`→`/404.html` 404/`single-page-application`→`/index.html` 200）、`run_worker_first`（bool 或路径前缀列表，worker 404 后回退资产）。CLI `--assets-not-found/--run-worker-first/--run-worker-first-path`。已对真实 workerd e2e。

## Cron → scheduled()（ADR-070，已实现）

`Version.Crons`（CLI `--cron`）→ `Projection.CronTargets()`；cell-agent 对 `KindCron` 定时器（scope 约定 `<ns>/__cron__/<worker>`）在派发时补 `worker`+`bundle_sha` → user-runtime `POST /v1/timers/dispatch` → workerLoader 加载 → `scheduled(event, env, ctx)`。**消费者配置/死信（ADR-072）**：`Consumer{Queue,MaxRetries,DeadLetterQueue,MaxBatchSize,MaxConcurrency}`；runner 达上限入 DLQ（同 ns `Send`）或丢弃告警；`max_concurrency` 为每队列**同时在飞的批次数**上限（接受 0..100，默认 0/1=串行；超额配置拒绝，历史值在 runner 限幅，ADR-112/292）。CLI `--consumer <queue>[:maxRetries[:dlq[:maxConcurrency]]]`。**调度器（ADR-076）**：`internal/cron` 按 5 字段 UTC 表达式物化 slot 为 `KindCron` 定时器（`CELLHIVE_CRON_INTERVAL` 默认 30s，幂等、不补跑）。

## 待细化

- 每种 binding 的 REST 端点/错误码总表（已分散在各 ADR）；
- service binding：版本冻结（ADR-104 部署时 pin）+ **目标侧允许列表**（ADR-144，`cellhive service-acl`；同 ns 默认允许，跨 ns 需授权；目标写 `ns/worker`）。entrypoint 级 ACL 未细分。

## Queue 消费者派发（ADR-067，已实现）

## Vectorize（ADR-158，已实现）

索引 = **注册资源**（kind `vectorize`）+ **不可变配置** `{dimensions(1..1536), metric(cosine|euclidean|dot-product)}`（信封加密存控制面）：

```bash
cellhive vectorize create acme docs --dimensions 768 --metric cosine
cellhive vectorize insert acme docs --file vectors.ndjson     # {"id","values","metadata"?,"namespace"?} 每行一条
cellhive vectorize query  acme docs --vector "0.1,0.2,..." --top-k 5 --return-values --return-metadata all
cellhive vectorize info   acme docs                           # dims/metric/vectorCount/namespaces/metadata indexes
```

绑定（Workers 侧，CF 兼容）：

```js
export default {
  async fetch(req, env) {
    await env.INDEX.upsert([{ id: "a", values: [...], metadata: { lang: "en" }, namespace: "eu" }]);
    const r = await env.INDEX.query([...], { topK: 5, returnValues: true, returnMetadata: "indexed", filter: { lang: "en" } });
    const byId = await env.INDEX.queryById("a", { topK: 3 });
    const got  = await env.INDEX.getByIds(["a"]);
    const del  = await env.INDEX.deleteByIds(["b"]);
    const d    = await env.INDEX.describe();          // { dimensions, metric, vectorCount, ... }
    const lv   = await env.INDEX.listVectors({ count: 100, cursor });
  },
};
```

- **config**：`vectorize: [{ binding: "INDEX", index_name: "docs" }]` —— **env key = binding 名，资源 = `index_name`**（scoped token 以 index 名铸造；校验门用 `index_name` 查登记）；`--vectorize INDEX=docs`（wrangler 前缀/`--config` 均可）。
- **score 语义（与 CF 一致）**：`cosine` = 余弦相似度（**越大越近**）；`euclidean` = 距离（**越小越近**）；`dot-product` = 内积（越大越近）。`returnMetadata: "none" | "indexed" | "all"`（legacy 布尔 `true`→`all`）；`topK` 默认 5，无 payload ≤100、带 values/metadata ≤50。
- **filter**：`$eq/$ne/$in/$nin/$lt/$lte/$gt/$gte`、隐式 `$eq`、点号嵌套路径、多键隐式 AND、字符串范围（前缀搜索）。`namespace` 是独立分区（先于 metadata 过滤）。
- **存储与检索（ADR-159，CGo + SQLite 官方 `vec1`）**：索引 cell（`<ns>/__vectorize__/<index>`）里 `vectors` 表是**真源**（float32 `raw` + metadata + namespace），`vec USING vec1(embedding, ns)` 是派生 ANN 索引。默认是 **flat 精确扫描**（已比旧 Go KNN 快约 8×：20k×256/K=10 约 **6.3ms**）；`cellhive vectorize rebuild` 用 `vec1_train` 建 **IVFADC+OPQ** 模型（`--buckets/--quantizer/--codesize/--nprobe`），查询约 **0.22ms**（同数据，≈210× 于旧实现）；`cellhive vectorize drop-ann` 回到 flat。`describe`/`stats` 的 `ann` 字段显示当前模型。
- **指标/过滤差异**：`dot-product` **不支持**（vec1 只有 l2/cos），`create` 时明确报错；namespace 是 vec1 的 metadata 列，**下推到索引**；其余 metadata 过滤是**后置过滤**（4× 过采样，不保证一定返回满 topK——CF 是先过滤）；metadata index **不强制**（保留目录以兼容 CF，indexed string 的 64B 截断未模拟）。
- **运维**：`GET /v1/vectorize/stats`（ADR-157 通用 stats 面：dims/metric/vector_count/namespaces/metadata_indexes/disk/max_vectors）、`GET|POST|DELETE /v1/vectorize/metadata-index(es)`、`cellhive vectorize info|stats`；`resource delete` 吊销后绑定调用 fail closed（403）。
- **dev**：Miniflare 只有配置 schema（workerd 无 vectorize 服务），`cellhive dev` 遇到 vectorize 会**明确报错退出**；本地调试用 `cellhive vectorize` 打真实命名空间。

**每域统计 `GET /v1/<kind>/stats`（ADR-157）**：全部只读、只读元数据（PRAGMA/stat/schema/有界列表），不解码行数据。通用字段来自 `cellstore.DiskStats`：`page_size`/`page_count`/`size_bytes`/`freelist_pages`/`freelist_bytes`/`main_bytes`/`wal_bytes`/`total_bytes`/`journal_mode`（WAL 单列，因为 cellstore 关闭了 autocheckpoint，`page_count` 会低估）。各域附加：
- **KV**：`expires_indexed`、`expired`、`next_expiry_ms`（`kv_expires` 部分索引）、`rows_estimate`（dbstat 叶页 `ncell`，超 20000 页跳过并给 `estimate_note`）；`?exact=1` 才给 `keys=count(*)`。
- **D1**：`tables`/`table_names`/`indexes`/`auto_indexes`/`schema_version`/`user_version`/`sqlite_version`；`?tables=1` 追加每表 `pages`/`payload_bytes`/`rows_estimate`（dbstat，超阈值跳过）。cellstore 内部表（`kv`/`cell_meta`）与 `sqlite_*` 不计入 `tables`。
- **Queue**：`depth`/`visible`/`leased`、`oldest_visible_ms`（滞后指标）、`max_attempts`、`dead_letter_queue`/`dead_letter_depth`/`dead_letter_visible`/`dead_letter_oldest_visible_ms`、`disk`。
- **R2**：默认返回从启用后跟踪的持久估算 `objects`/`bytes`（`estimated:true`、`baseline:"from_first_tracked_write"`），点查账本，不回填已有对象。`?scan=1` 或指定 `prefix`/`limit` 返回原有 `truncated`/`cursor`/`listing_ms` + `multipart_uploads`/`multipart_parts`；此模式使用有界 List（默认 1000、上限 10000），不是精确总量；`r2/.mpu/` 暂存区对用户 List 不可见。
- **Workflow**：`disk` + `instances_estimate`（dbstat）；`?exact=1` 给 `instances`+`by_status`。
- **Hyperdrive**：`registered`/`has_config`（绝不返回 origin URL）。
- **DO**：`indexed_objects`/`classes`/`workers`/`available`（对象索引 ADR-109 关闭时 `available:false`）。

**资源吊销（ADR-156）**：`resource delete` 会删控制面登记 + 派生 binding 行并写入 `revoked_resources` 墓碑；绑定解析**先查墓碑**，因此即使不可变版本元数据里仍写着该 binding（或该 worker 被重新 deploy）也**保持 fail closed**，直到重新 `resource create` 才解禁。数据 cell（KV 条目/队列消息/DO SQLite）**不删**；队列消费者因资源不再被 `ResourcesByKind` 列出而停止。

**批次形状与逐消息结果（ADR-154/155）**：`queue(batch, env, ctx)` 的 `batch` 保持**数组形状**（`length`/索引，向后兼容）并额外提供 `batch.messages`、`batch.queue`、`batch.ackAll()`、`batch.retryAll({delaySeconds})`；每条消息有 `message.ack()` 与 `message.retry({delaySeconds})`，未显式处理的在成功返回时**隐式 ack**，超过 `max_retries` 的请求重投进 DLQ。`message.body` 按 content-type 类型化（JSON→对象、text→字符串、其它→字节数组）。延迟生产：`env.QUEUE.send(body, {delaySeconds})`（`visible_at_ms` 前不可见）。

cell-agent 的 `queue.Runner` 周期性从**控制面登记的 queue 资源**取轮询集（`control.ResourcesByKind("queue")`，冷路径），对每个队列 `Claim` 一批（lease），POST 到 `CELLHIVE_DISPATCH_URL` 的 **`/v1/queues/dispatch`**；成功 `Ack`、失败整批 `Retry`（**at-least-once**）。body 含 `(namespace, queue, worker, bundle_sha, version, messages)`（worker/bundle/version 由 `Projection.QueueTargets()` 解析，ADR-067/112/127），user-runtime 直接加载该活跃版本并调用其 `queue()` handler。body 不带 bindings；user-runtime 在加载时按 `(ns, worker, version)` 调 `GET /v1/internal/worker/bindings` 取该版本的 binding spec 注入 env（ADR-128，失败开放），故 `queue(batch, env)` 与 fetch 的 env 一致。配置：`CELLHIVE_QUEUE_INTERVAL`（默认 1s，0 关闭）。**已实现**：user-runtime `/v1/queues/dispatch`（ADR-067）；重试上限 + 死信（`DeadLetterQueue`，ADR-072，`internal/queue/runner.go`）。

_最后更新：2026-09-24_

## 绑定实现（ADR-090，RPC entrypoint env；ADR-185 已取代其平台 env 结论）

租户 bindings 不再是 HTTP facade：平台 worker 导出 `WorkerEntrypoint` 能力类，`ctx.exports.X({props})` 生成 props 绑定 stub 放进**被加载租户 worker 的 env**。

- **KV / R2 / Queue**：entrypoint stub（`workerd/platform/bindings.js`）；R2 `get()` 返回 `RpcTarget` body。**D1** 的数据库调用仍走 props-bound entrypoint，但 generated wrapper 用 `makeD1FromStub()` 在租户 isolate 重建同步 `prepare()`/`bind()` 语句对象，避免 ORM `batch()` 收到不可序列化的 `RpcPromise`。租户 fetch 与其 DO 的 `this.env` 与构造函数形参 `env` 都可见这些绑定。
- **DO namespace / Workflow**：CF API 需**同步** `idFromName`，不能跨 RPC → 用 generated `bindings-wrapper.js` patch importable `env`。两宿主统一（do-runtime `bindings-wrapper.js`；user-runtime `queue-wrapper.js`/`workflow-wrapper.js`）。
- **ServiceBinding**：`env.SVC.fetch()` 与 `env.SVC.<method>()`（Proxy 转发）→ cell-agent `/v1/service/{fetch,run}`（scope kind=service）→ user-runtime `/v1/services/{fetch,run}` → 目标 worker / 其命名 entrypoint（同名空间）；`binding.entrypoint` 指定目标 entrypoint。
- **R2 list**：`env.BUCKET.list({prefix,limit,cursor,startAfter})` → `/v1/r2/list` 返回 `{objects,truncated,cursor}`（cursor 独占、尺寸来自 list 响应，ADR-145）。
- **R2 presign**：`env.BUCKET.createPresignedUrl(key,{expiresIn})` → cell-agent `/v1/r2/presign` → bucket `PresignGet`（自托管后端返回短时读 URL）。
- **日志 Tail**：`workerd/platform/log-tail.js` 由可信宿主作为真实 `Fetcher` 挂到动态 Worker 的 `WorkerCode.tails`；它捕获 `console.*`/异常并写入平台 ring/可选 OTLP。身份、log token、URL 与 private transport 不进入 tenant env（ADR-185）。
- **AI（BYO）**：`env.AI.run(model, inputs)` → `<AI_URL>/chat/completions`（OpenAI 兼容，`AI_KEY` Bearer），返回 `{response,model,usage}`；`inputs.messages` 或 `{prompt}`。

## Hyperdrive：驱动与兼容（2026-10-21）

CellHive 的 Hyperdrive 与 Cloudflare 的语义差异：**默认只下发源库 `connectionString`**，不做边缘连接池/代理/全球加速。

### 可选：ProxySQL 代理模式

默认 `direct`（源库地址，行为不变）。需要共享池/统一入口时开启：

```bash
CELLHIVE_HYPERDRIVE_MODE=proxy
# 只服务一种协议
CELLHIVE_HYPERDRIVE_PROXY=postgres://proxysql:6133
# 或同时服务两种协议（一个变量，按 scheme 选择）
CELLHIVE_HYPERDRIVE_PROXY=postgres://proxysql:6133,mysql://proxysql:6033
```

- binding 的 host/port 会改写为协议池，**database/user/password/query 保持不变**；
- PostgreSQL 和 MySQL 使用 ProxySQL 的不同监听端口（6133/6033）；
- 未配置某协议的 entry 时，该协议 binding fail closed；
- 裸 `host:port` 仍按 PostgreSQL 兼容处理；MySQL 必须写 `mysql://`；
- 多个 Hyperdrive 资源使用同一 database 名时，可用 `CELLHIVE_HYPERDRIVE_DB_MODE=binding` 生成 `hd_<resource>`；MySQL 保留原 schema；
- `CELLHIVE_HYPERDRIVE_MANAGED_USERS=1` 时，CellHive 为每个 binding 在 origin 创建 `hd_<hash>` 角色并只把生成凭据下发给 worker；origin URL 只留在加密的 control 配置里，需要 `CREATE USER`/`GRANT` 权限。
- MySQL 驱动在 workerd 下需要 `disableEval: true`（workerd 禁止 `new Function`），并通常需要 Node builtin 的 CJS require shim；已用 `mysql2` + ProxySQL 验证；
- 设置 `CELLHIVE_PROXYSQL_ADMIN` 后，Hyperdrive 资源 create/delete 会自动 upsert/delete ProxySQL 的 `mysql_servers`/`mysql_users` 或 `pgsql_servers`/`pgsql_users`，并 `LOAD ... TO RUNTIME`；`CELLHIVE_PROXYSQL_SAVE=1` 时同时落盘。
- ProxySQL 的 admin 凭据、backend 池参数、query rules 仍由运维管理；自动同步失败目前只记日志，不回滚资源创建。
- 改 `CELLHIVE_HYPERDRIVE_MODE` 后需要重启 cell-agent。要在 stock workerd 上使用真实 Postgres 驱动：

1. version 必须带 `compatibility_flags: ["nodejs_compat"]`；
2. tenant 出网能到数据库（本地/私有库需要 `CELLHIVE_TENANT_OUTBOUND` 包含 `private`/`local`）；
3. 推荐用 **postgres.js**（`workerd` 条件导出，纯 ESM）：
   ```js
   import postgres from "postgres";
   const sql = postgres(env.HYPERDRIVE.connectionString, { max: 4 });
   const rows = await sql`select 1 as ok, current_database() as db`;
   await sql.end({ timeout: 2 });
   ```
   esbuild：`--conditions=workerd --external:cloudflare:sockets --external:node:*`。

4. **node-postgres (`pg`)** 也能用，但它依赖 CJS `require`；需要用 `pg-cloudflare` 的 `workerd` 条件导出，并提供一个小 require shim：
   ```js
   // pgshim.js — 必须在 import "pg" 之前执行
   import * as events from "node:events";
   import * as util from "node:util";
   import * as crypto from "node:crypto";
   import * as buffer from "node:buffer";
   import * as stream from "node:stream";
   import { CloudflareSocket } from "pg-cloudflare";
   globalThis.require = (name) => ({ events, util, crypto, buffer, stream, net: {}, tls: {}, dns: {}, fs: {}, path: {}, os: {}, readline: {}, "pg-cloudflare": { CloudflareSocket } })[name] || {};
   ```
   ```js
   import "./pgshim.js";
   import { Client } from "pg";
   const c = new Client(env.HYPERDRIVE.connectionString);
   await c.connect();
   const r = await c.query("select 1 as ok, current_database() as db");
   await c.end();
   ```
   esbuild：`--platform=neutral --main-fields=main,module --conditions=workerd --external:cloudflare:sockets --external:node:* --external:events --external:util --external:crypto --external:buffer --external:stream --external:net --external:tls --external:dns --external:fs --external:path --external:os --external:readline`。

以上两条均已在真实 Postgres 17 上验证 `select 1, current_database()`；连接池（`max`）是 **per-isolate** 的，不是 CF 的全局 Hyperdrive 池。

### 结果预算与 R2 metadata 边界（ADR-292）

D1 结果按最终保留载荷线性累计，32 MiB 总预算包含数值和 BLOB 展开为整数数组后的载荷；不包含全部 Go 容器开销，也不是 JSON wire size 上限。逐行检查请求取消。

R2 GET/HEAD/list 的 metadata 仅在 ETag/size 匹配对象版本时返回，size/ETag 以对象为准。无 metadata 的覆盖会清理观察到的旧 sidecar（包含相同内容），条件删除不会清掉并发的新版本；已知不支持条件删除的 provider 在存在旧 sidecar 时拒绝覆盖。每次 PUT 增加 sidecar 点读，存在旧 sidecar 时另需条件删除。数据与 metadata 不是原子发布，迟到发布及同内容并发仍有边界，失败响应可能已更新数据；不保证完整 Cloudflare R2 并发原子语义。
