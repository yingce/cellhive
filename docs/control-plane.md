# 控制面

> ADR-304 Bearer JWT 的无状态 issuer 修订**实施与验收中，未部署**；此前有状态测试不证明本次修订通过。旧静态 header/OIDC 不保留兼容路径。

## 定位与形态

- **单一管理 API**：namespace JWT 管理授权 namespace 的全部业务；`issuer/*` 且显式 `ns=["*"]` 允许全部全局业务及运维读取、GC、全量指标。`token/admin` 保留全局 bootstrap 权限；issuer CRUD 已移除。请求 ns 参数不能替代授权。
- **并入 `cell-agent`**（ADR-012）：单 Go 二进制；控制面监听 `:8082`（经运维自备的反代/云 LB），与数据面 `:7001` 分监听、分鉴权（Go↔Go 与 JS↔cell 共用 :7001；无 gRPC 平面，ADR-136）。
- secrets：`POST /v1/control/secret`（put）、`GET /v1/control/secret`（get）、`DELETE /v1/control/secret`、`GET /v1/control/secrets`（只列 key/更新时间）；CLI `cellhive secret put|get|delete|list`（ADR-147）。
- **每域资源管理（ADR-157，推荐）**：`POST|GET|DELETE /v1/<kind>/resources`（`kind=kv|d1|queue|r2|workflow|hyperdrive`），kind 由路径固定、`scope` 缺省 `<ns>/__<kind>__/<name>`；`GET /v1/<kind>/stats` 给只读运维统计（见 `docs/bindings.md`）；队列死信重放为 `POST /v1/queue/dead-letters/replay`。CLI 对应 `cellhive kv namespace|d1|r2 bucket|queue|workflows|hyperdrive create|list|delete|stats`。以下 `/v1/control/*` 形式保留为**兼容别名**（同 handler）。
- 资源吊销（别名）：`DELETE /v1/control/resource?namespace=&kind=&name=[&force=1]`（被引用的 worker 仍在声明该 binding 时 409 `resource_in_use` + `referenced_by`，`force=1` 强制；**不删数据 cell**，登记墓碑让解析立即 fail closed，重新 `resource create` 解禁）；CLI `cellhive resource delete|rm|revoke <ns> <kind> <name> [--force]`（ADR-156）。
- 队列运维：`GET /v1/control/queue/status?namespace=&queue=`（`depth`/`visible`/`leased` + 死信队列名与深度）、`POST /v1/control/queue/replay-dlq?namespace=&queue=&limit=`（死信重投回主队列，至少一次）；CLI `cellhive queue status|replay-dlq`（ADR-156）。
- **元数据**存**单个** control cell（`__platform__/__control__/main`，关系表；ADR-117），app 是 `ns` 列。

## 管理对象

| 对象 | 说明 |
|---|---|
| 应用 / 命名空间 | 租户边界 |
| Worker | 名称（Wrangler `name`） |
| 不可变版本 | 每次 deploy 分配版本号；promote/rollback 切指针 |
| 路由 | host 模式 / 路径 / 自定义域 → 生成路由投影 |
| 绑定元数据 | 该 Worker 的 KV/D1/R2/Queue/Workflow/DO 绑定；deploy 时冻结物理 id |
| 密钥 | 信封密文 + 外部根密钥 |
| vars | 环境变量 |
| 审计 | 控制写操作日志 |

## 部署流水线

```
cellhive deploy
  1. Go + esbuild 本地打包 → 内容寻址 bundle（SHA-256）+ 资产
  2. 上传 bundle/assets 到对象存储
  3. 解析 wrangler 配置，校验并冻结绑定元数据（引用已存在的资源）
  4. 分配不可变版本号
  5. 原子提交（WATCH/事务）：写入版本元数据 + 切换路由 active version
  6. user-runtime 在 TTL 内拉取到新投影并生效（ADR-031）
```

## 资源生命周期（拒绝自动 provisioning）

Wrangler 的"无 id 自动创建资源"被**拒绝**（wrangler-compat.md）：资源必须由控制面显式创建。CLI 需提供：

| 命令（示例） | 作用 |
|---|---|
| `cellhive app create <ns>` | 建应用/命名空间 |
| `cellhive resource create <ns> kv <name>` | 登记 KV namespace（= 一个 `__kv__` cell） |
| `cellhive resource create <ns> d1 <name>` | 登记 D1 数据库 cell |
| `cellhive resource create <ns> queue <name>` | 登记队列 cell |
| `cellhive resource create <ns> r2 <name>` | 登记 R2 虚拟桶 |
| `cellhive resource create <ns> workflow <name>` | 登记 workflow 定义 |
| `cellhive resource create <ns> hyperdrive <name> --connection-string <url>` | 登记 Hyperdrive origin（信封加密，ADR-129） |
| `cellhive vectorize create <ns> <index> --dimensions N --metric cosine\|euclidean\|dot-product` | 登记 Vectorize 索引（配置信封加密；`resource create <ns> vectorize <index>` + body `config` 等价，ADR-158） |
| `cellhive resource delete <ns> <kind> <name> [--force]` | 吊销资源登记（墓碑立即生效；数据 cell 保留；ADR-156）；每域等价形式 `cellhive <kind> delete <ns> <name> [--force]`（ADR-157） |
| `cellhive <kind> create\|list\|stats` | 每域登记与运维统计：`kv namespace` / `d1` / `r2 bucket` / `queue` / `workflows` / `hyperdrive`（`stats`/`info` 输出见 `docs/bindings.md`，ADR-157） |
| `cellhive queue status <ns> <queue>` / `queue replay-dlq <ns> <queue> [--limit]` | 队列积压/死信可见与死信重放（ADR-156） |
| `cellhive secret put <ns> <worker> <KEY>` | 写密钥（信封加密） |
| `cellhive deploy / promote / rollback / releases / domain / tail` | 发布与运维 |

- deploy 引用不存在的资源 → **明确报错并提示对应 create 命令**；
- 资源创建 = 在控制面登记 scope（cell 首次访问时惰性激活）。
- **name / scope 必须可寻址（ADR-269）**：`name` 是默认 cell id、registry 主键和 binding/token 名，必须是单个 cell 组件（不能含 `/` 或 `\`，不能是 `.`/`..` 或含 `..`）；`scope` 只能省略（配置型资源/无 id 声明）、给完整 `<ns>/__<kind>__/<id>`，或给裸 cell id（如 `sessions`）。需要"名字与物理 cell 不同"时用显式 `--scope`，不要把 `/` 写进 name（如 `name: vwork-74/CHTEST` 会在创建时 400，不会留下不可用/不可删的登记）。

## 无状态 Issuer 签发（ADR-304，修订实施与验收中）

- `kid=issuer/<pattern>` 直接编码精确/末尾前缀 `*`/`*` 上限，固定 `0 < exp-iat <= 86400` 秒。恰好 32 位十六进制旧 issuer ID 拒绝，无回退。
- 无 issuer CRUD、owner 点查、active record、持久化确认或 namespaces 注册表授权。旧 issuer 表可保留为惰性数据，不做破坏性迁移；资源注册与 RPO=0 不变。
- key 为 `HKDF-SHA256(decoded_root_key,nil,"cellhive/jwt/v1/"+kid,32)`。同一 vwork key 签 namespace（`ns` 数组）或 binding（精确 `ns` 字符串与 `kind/name`）；无 actions/key_version，固定 `worker/version/resource` 仅平台签发。
- 无即时单 issuer 撤销；JWT 靠 exp 到期，泄露 key 需轮换 root 并重新分发才能止损。会话无状态绑定父 pattern 和有效期，见 [安全模型](security.md)。
- namespace JWT 覆盖全部授权业务；`issuer/*` 且显式 `ns=["*"]` 覆盖全局业务、运维、GC、全量指标。`:8082/v1/control/metrics` 鉴权过滤；`:7001/metrics` 为私网免鉴权例外。CF 仅 `:8082`，bridge/owner 转发保留上限。

### CLI Bootstrap、Provision 与签发

以下派生仅在可信运维环境执行，root 文件由 secret 挂载提供；无显式 JWT/profile token 时 CLI 自动签5分钟全局 bootstrap admin JWT。关闭 shell tracing，不记录 key/JWT。

```bash
export CELLHIVE_ADMIN_URL=https://cellhive.example.com
export CELLHIVE_ROOT_KEY_FILE=/run/secrets/cellhive-root-key
unset CELLHIVE_TOKEN CELLHIVE_ADMIN_TOKEN
key="$(cellhive creds issuer 'vwork-*')"
```

`creds issuer` 不访问网络或注册记录，输出原始32字节 key 的无填充 base64url。通过 secret 管理将 key 交付可信签发方；后者不持 root。TTL 固定最大86400秒，`token --ttl` 默认5m。

```bash
export CELLHIVE_TOKEN="$(cellhive token --issuer 'vwork-*' --key "$key" --ns vwork-42 --ttl 5m)"
cellhive token --issuer 'vwork-*' --key "$key" --ns vwork-42 --kind kv --name CACHE --ttl 5m
```

`--key` 可能出现在进程列表，仅在可信主机执行。签发与服务端 issuer 认证均不查询记录。namespace 模式不传 kind/name，binding 模式精确传二者。`cellhive creds [role]` 与 `creds issuer <pattern>` 输出 key，不是 Bearer；issuer create/list/get/enable/disable/delete 已移除。

### Bundle 授权

持久化 `bundle_grants` 记录 namespace 与 SHA 的授权关系；知道 SHA 或对象已存在不代表有权部署它。namespace 身份原生 bundle 上传必须显式传 `namespace`（CLI 自动传），上传成功后捕获写入 grant；CF 上传同样授予所属 namespace。部署同时验证对象存在与 namespace grant，不能复用别的 namespace 未授权的 SHA。

## Deploy 服务端功能/兼容性拦截（ADR-065）

CLI 只是**快速反馈**；`deploy` 的校验以**服务端为权威**（防绕过、防不同 CLI 版本）。校验逻辑放在**共享库**，`deploy` 端点与 CLI preflight 共用。

`POST /v1/control/deploy`（含 bundle 上传）校验：

1. **bundle**：`bundle_sha` 的内容寻址对象存在；`assets` 引用存在。
2. **兼容日期**：`compatibility_date` ≤ 平台支持上限（已知 **2026-09-23**，实测）；超限报错并给出上限。
3. **compatibility_flags**：必须为 pinned workerd 的已知集（未知 flag 报错）。
4. **绑定**：type 必须在**支持矩阵**内；不在则拒绝（`images/browser-rendering/send_email/ai_search/dispatch_namespaces/secrets_store/containers/...`；`vectorize`/`hyperdrive` 已支持，vectorize 的资源引用 = `index_name`，ADR-158）。
5. **资源引用**：binding 指向的 KV/D1/R2/Queue/… 必须已由控制面登记（**拒绝自动 provisioning**，ADR-014）；否则提示对应 `cellhive <kind> create`。
6. **DO 生命周期**：仅 `created`/`sqlite`（含 `new_classes/new_sqlite_classes`）；rename/delete/transfer/`script_name` 拒绝。
7. **未知字段**：显式拒绝（字段路径 + 原因 + "是否计划支持"）。

错误形状 `{error, message, field_path?}`，稳定 error code：`unsupported_binding`、`compat_date_too_new`、`unknown_flag`、`binding_unregistered`、`unknown_field`。

> 由 `cellhive dev`（Bun+Miniflare，ADR-065）允许但平台不支持的绑定，会在这里被拦；dev/CLI 用同一校验库提前警告，避免"dev 绿、deploy 拒"。

> **状态（2026-09-15）**：已实现。校验逻辑在 `internal/wranglercompat`（附加单元测试）；`POST /v1/control/deploy` 返回 `deploy_rejected` + `findings`（字段路径/错误码）；资源登记用 `control.HasBinding`，bundle 存在性用对象存储点查（`artifactStore().GetBundle`，未命中即 `missing_bundle`）。测试见 `internal/server/control_test.go`。

## 控制面请求路由

- 反代/云 LB 把 admin 流量负载均衡到**任意 cell-agent 副本**；
- 非 owner 副本把业务请求转发到 owner 的 admin 地址，保留原始 Bearer 授权并由目标重新验证；原生内部控制镜像仅接受指定内部身份，不提供 CF API；
- 一般控制面读可缓存；issuer 授权无状态，不查询 owner。deploy/rollback 仍由 control cell owner 单写；
- control cell owner 死亡 → 标准接管流程；短暂不可用期间 admin 写返回 `503`。

## 鉴权与身份

- 统一 Bearer HS256 JWT，默认 audience `cellhive`；不接 OIDC，不接受静态凭据/旧 header；
- namespace 权限来自已验证 JWT 与 `kid` pattern 上限，不依赖 namespaces 注册表或请求参数；平台不接管 vwork 登录；
- 运行时的 binding 隔离（ADR-029）与此独立。

## 待细化

- CLI 命令集与参数（与 Wrangler 输入的映射细化）；
- 边缘证书门控由运维自配（平台不下发，ADR-132）；域名校验已移除（ADR-133，登记即授权）。


## 配置（ADR-131）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_BASE_DOMAIN` | 空 | 内置 worker 域 `<ns>-<worker>.<base>`；空 = 关闭内置域。边缘侧请为 `*.<base>` 自行配置 wildcard 与转发（平台不下发边缘配置，ADR-132） |
| `CELLHIVE_AUTO_CREATE_APP` | `1` | deploy 时幂等补建 ns（一键跑业务） |
| `CELLHIVE_TOKEN`（CLI） | 空 | Bearer HS256 JWT；namespace 范围或 bootstrap admin，必须刷新；优先于 profile |

CLI：`cellhive domain add|ls|rm`；登记自定义域即授权并可路由，不做 DNS 校验；`route add` 要求 host 已登记到同一 namespace。

## Schema v2（控制面 cell：`__platform__/__control__/main`）

> 设计定稿 2026-09-17（ADR-131；域名校验已按 ADR-133 移除）。行为实现（软删/purge/loader 改动）已落地；
> 表结构以本节为准。数据 cell（`<ns>/<class>/<id>`）不在本 schema 内。

### 租户与版本
```sql
CREATE TABLE IF NOT EXISTS apps (
  ns         TEXT PRIMARY KEY,
  created_ms INTEGER NOT NULL,
  deleted_ms INTEGER NOT NULL DEFAULT 0            -- 软删（purge 异步、列表过滤）
);
CREATE TABLE IF NOT EXISTS workers (
  ns         TEXT NOT NULL,
  name       TEXT NOT NULL,
  active     INTEGER NOT NULL DEFAULT 0,
  previous   INTEGER NOT NULL DEFAULT 0,
  storage_id TEXT NOT NULL DEFAULT '',             -- DO 存储身份（跨版本稳定）
  host_label TEXT NOT NULL DEFAULT '',             -- 内置域 label（唯一）
  created_ms INTEGER NOT NULL DEFAULT 0,
  deleted_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE UNIQUE INDEX IF NOT EXISTS workers_host_label ON workers(host_label) WHERE host_label <> '';

CREATE TABLE IF NOT EXISTS versions (             -- 不可变快照（JSON 为真值）
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL,
  bundle_sha TEXT NOT NULL, assets_sha TEXT NOT NULL DEFAULT '',
  storage_id TEXT NOT NULL DEFAULT '', session_policy TEXT NOT NULL DEFAULT '',
  bindings TEXT NOT NULL DEFAULT '[]', vars TEXT NOT NULL DEFAULT '{}',
  consumers TEXT NOT NULL DEFAULT '[]', crons TEXT NOT NULL DEFAULT '[]', assets TEXT,
  compat_date TEXT NOT NULL DEFAULT '', compat_flags TEXT NOT NULL DEFAULT '[]',
  created_ms INTEGER NOT NULL, actor TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number)
);
CREATE INDEX IF NOT EXISTS versions_assets ON versions(assets_sha) WHERE assets_sha <> '';

-- 规范化绑定（派生自 versions.bindings）：HasBinding / 服务 pin / GC 走索引
CREATE TABLE IF NOT EXISTS bindings (
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL, name TEXT NOT NULL,
  type TEXT NOT NULL, id TEXT NOT NULL DEFAULT '', class_name TEXT NOT NULL DEFAULT '',
  entrypoint TEXT NOT NULL DEFAULT '', pin_sha TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number, name)
);
CREATE INDEX IF NOT EXISTS bindings_lookup ON bindings(ns, type, name);
CREATE INDEX IF NOT EXISTS bindings_pin    ON bindings(ns, id, pin_sha);

CREATE TABLE IF NOT EXISTS version_bundle_refs (
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL, sha TEXT NOT NULL,
  PRIMARY KEY (ns, worker, number, sha)
);
CREATE INDEX IF NOT EXISTS bundle_refs_sha ON version_bundle_refs(sha);
```

### 域名与路由（传统 `(host,path)`；登记即授权）
```sql
CREATE TABLE IF NOT EXISTS hosts (                 -- 域名归属（登记即授权，ADR-133）
  host       TEXT PRIMARY KEY,                     -- 规范化（小写/去端口/去尾点）
  ns         TEXT NOT NULL,
  worker     TEXT NOT NULL DEFAULT '',             -- builtin: 指向的 worker；custom: ''
  kind       TEXT NOT NULL DEFAULT 'custom',       -- builtin|custom
  created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS hosts_ns ON hosts(ns);

CREATE TABLE IF NOT EXISTS routes (                -- 传统 (host,path) → worker（path 是挂载点）
  host TEXT NOT NULL, path TEXT NOT NULL DEFAULT '', ns TEXT NOT NULL, worker TEXT NOT NULL,
  PRIMARY KEY (host, path)
);
CREATE INDEX IF NOT EXISTS routes_ns        ON routes(ns, host, path);
CREATE INDEX IF NOT EXISTS routes_ns_worker ON routes(ns, worker);
```
规则：
1. **内置域** `<ns>-<worker>.cell.internal`（`host_label` 唯一）由系统在 deploy/promote 生成：
   `hosts(kind='builtin')` + `routes(host, '', ns, worker)`，用户不可写。
2. **自定义域**注册即生效（ADR-133：不做 DNS 校验；能管理该 ns 的凭据就是其域名的权威）；host 已由其它 ns 登记时返回 409，不存在 pending/verify 竞争流程；
3. 写 `routes` 前必须有对应 `hosts` 行且 `hosts.ns = 请求 ns`（并发用
   `ON CONFLICT(host,path) DO UPDATE … WHERE ns=excluded.ns` 兜底）。
4. 删 worker 按 `routes_ns_worker` / `hosts(ns)` 清理对应行。
5. **前缀匹配按路径段边界**：`/api` 匹配 `/api` 与 `/api/x`，不匹配 `/apix`；`path=''` 或 `'/'` 表示整个 host。
6. **路由 = 挂载点（总是剥离）**：loader 选路后**一次性**把路由的 path 前缀从请求路径去掉（query/Host 不变），资产与 worker 都看到剥离后的路径；剥离后为空视为 `/`。例如 `route add app.test /api api` 后，worker 收到的 `/api/users` 是 `/users`。
7. **删除范围由 path 参数是否存在决定**：`DELETE /v1/control/route` 未传 `path` 时删除该 namespace/host 下所有路由；传 `path`（包括空字符串）时仅删除 `(host,path)` 精确匹配的路由。CLI 对应 `route rm <namespace> <host> [--path <prefix>]`。

### 资源 / 密钥 / DO 类 / 锁 / GC / purge
```sql
CREATE TABLE IF NOT EXISTS resources (
  ns TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  scope TEXT NOT NULL DEFAULT '', created_ms INTEGER NOT NULL,
  cfg_dek BLOB, cfg_nonce BLOB, cfg_ct BLOB,       -- 密封配置（hyperdrive origin 等）
  PRIMARY KEY (ns, kind, name)
);
CREATE INDEX IF NOT EXISTS resources_kind ON resources(kind, ns, name);

CREATE TABLE IF NOT EXISTS secrets (
  ns TEXT NOT NULL, worker TEXT NOT NULL, key TEXT NOT NULL,
  wrapped_dek BLOB, value_nonce BLOB, ciphertext BLOB, updated_ms INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, key)
);

CREATE TABLE IF NOT EXISTS do_classes (
  ns TEXT NOT NULL, worker TEXT NOT NULL, code_class TEXT NOT NULL,
  storage_class TEXT NOT NULL DEFAULT '', deleted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, worker, code_class)
);

CREATE TABLE IF NOT EXISTS delete_locks (
  ns TEXT NOT NULL, worker TEXT NOT NULL, actor TEXT NOT NULL DEFAULT '',
  at_ms INTEGER NOT NULL, exp_ms INTEGER NOT NULL, PRIMARY KEY (ns, worker)
);
CREATE TABLE IF NOT EXISTS deploy_idempotency (
  ns TEXT NOT NULL, worker TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL,
  created_ms INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (ns, worker, key)
);
CREATE TABLE IF NOT EXISTS gc_marks (
  kind TEXT NOT NULL, id TEXT NOT NULL, marked_ms INTEGER NOT NULL, PRIMARY KEY (kind, id)
);

CREATE TABLE IF NOT EXISTS purges (                -- 删除作业（幂等、可重试）
  ns TEXT NOT NULL, worker TEXT NOT NULL DEFAULT '',   -- '' = 整个 ns
  state TEXT NOT NULL DEFAULT 'pending',               -- pending|running|done|failed
  requested_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker)
);
```

### 审计与 rev
```sql
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ns TEXT NOT NULL, at_ms INTEGER NOT NULL,
  actor TEXT NOT NULL DEFAULT '',          -- JWT sub; historical static-admin rows retained
  actor_kind TEXT NOT NULL DEFAULT '',     -- historical values retained; not authorization
  on_behalf_of TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL, target TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_ns    ON audit(ns, at_ms);
CREATE INDEX IF NOT EXISTS audit_actor ON audit(actor, at_ms);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);   -- 'rev'
```

### 迁移
已实现：`schema` → `migrateColumns`（逐列 `ALTER TABLE … ADD COLUMN`，读取 `PRAGMA table_info` 跳过已存在列）→ `schemaV2`（新表与索引）。
因此**既有 cell 可原地升级**，无需重建；`bindings` 由 deploy 写入，老版本的绑定查询回退 `versions.bindings` JSON。

### 拆分准备（不在本次范围）
拆分 control 时另建 `__platform__/__dispatch__/main`（cell-agent 拥有）承载派发索引
（queue/cron/workflow/DO class → worker+version+bundle），使 cell-agent 不读控制面表；
现在**不建**。

_最后更新：2026-09-25_
