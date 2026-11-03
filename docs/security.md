# 安全模型

> ADR-304 **已实现，本地验收通过，未部署**：Go/JS/CLI/vwork、DO ticket 授权收紧和完整 runtime baseline 已完成本地验收；不代表生产切换完成，旧实现不构成兼容承诺。

## 信任边界

| 组件 | 边界 |
|---|---|
| 边缘代理（运维） | 只做 TLS 与 host 粗分流，**不做业务鉴权**；平台不下发边缘配置（ADR-132） |
| `user-runtime` | 入口 + 执行租户代码（跨命名空间 service binding 需目标 ns 授权，ADR-144）；租户 loaded worker **仅公网出网**；平台 loader 先于租户模块执行 |
| `do-runtime` | 仅私网；**不持对象存储凭据** |
| `cell-agent` | `:7001` 内部数据/解析仅私网；`:8082` admin/CF API 使用 Bearer HS256 JWT。CF API 不挂载到 `:7001` 或 `:8083`；内部原生控制镜像仅允许指定内部身份。内部调用使用用途隔离的角色 JWT（ADR-304） |
| 对象存储 | 长期凭据**只在 cell-agent**；bundle/assets 读走短期 scoped 凭据（ADR-030） |

## 多租户隔离（ADR-029）

1. **网络隔离（主防线）**：user-runtime 的 capnp 中，租户 loaded worker 的 `globalOutbound` = **公网 only network service**（可用 `CELLHIVE_TENANT_OUTBOUND` 显式加 `private`/`local`，ADR-130），默认**不含 RFC1918 / cell-agent 地址**。租户 Worker **无法**直接访问内网 `:7001/:8082`。
2. **token 不进租户 env**：internal token 只存在于 **host adapter（平台代码）与 cell-agent 之间**；租户只拿到 binding facade，拿不到 token/后端地址。
3. **scope 声明校验**：host adapter 调 cell-agent 时声明 `(ns, binding 类型/id)`；cell-agent 校验，越权即拒。binding 本身由 adapter 不可变 props 唯一绑定到具体 cell。
4. **纵深防御**：per-binding 短期 JWT 仅授权具体 namespace/kind/name；平台固定版本字段继续执行 ADR-302 版本授权与 resource fence，不改查当前 active 映射。可信 host 负责刷新，广权限 internal token 和签名 key 不进租户 loaded worker。

> 密钥：控制面 cell 存**信封密文**，根密钥在 cell 之外（env/KMS）；运行时读取执行视图时由 cell-agent 解密并注入 Worker/DO `env`，明文只进入该 load envelope 与 workerd env，不进入 bundle、渲染配置、参数或日志。同名 secret 覆盖 var。

## 管理后台（ADR-036/131）

- 单一管理后台（无独立 Web UI，= admin API），租户身份（ns/app）由请求携带；
- 普通 HTTP 仅接受 `Authorization: Bearer <JWT>`，固定 HS256；不接受静态凭据、旧 admin/scope header 或 OIDC/JWKS 回退。vwork 自身登录不受此变更影响。
- namespace JWT 授权所属 namespace 的全部业务，包括发布、CF 资源数据、secret、日志与按 namespace 过滤的监控；不引入 actions 或 metrics-reader 角色。issuer 上限允许且 token 显式包含 `ns=["*"]` 时，还可执行全平台运维读取和 GC，包括 admin 监听的全量 `/metrics`；精确或前缀 namespace 授权不具备这些全平台权限。**issuer registry 的创建、读取、列表、修改与删除始终仅允许 bootstrap admin JWT，通配 issuer 也不能管理 registry**。
- admin host 由运维限制暴露范围；经 TLS 和 JWT 认证开放的业务入口与私网内部监听分离；
- 所有控制写操作留**审计日志**（记录 actor/sub）。

## 网络策略

| 源 → 目标 | 允许 |
|---|---|
| 公网 → Traefik → user-runtime:8081 | ✅ |
| 公网/CLI → Traefik → cell-agent:8082 | ✅（鉴权后） |
| user-runtime → cell-agent:7001 | ✅ 私网 |
| cell-agent → user-runtime:8088 | ✅ 私网 + **dispatch 角色令牌**（ADR-075，缺省 401） |
| cell-agent ↔ cell-agent:7001 | ✅ 私网 |
| do-runtime → cell-agent:7001 | ✅ 私网 |
| 任意 → cell-agent:7001（公网） | ❌ |
| 租户 Worker → 私网（任意） | ❌ |

## 配额与 admission（ADR-035）

- 已实现 per-namespace 写速率限制；cell-agent 过载返回 `503 overloaded`；
- 已实现单 DO WAL 上限、消费节点 lease 的 `pressured`/`shed_cells` backpressure 与 queue DLQ 上限；
- worker CPU/subrequest 与 V8 heap 的 per-worker 强制上限尚未实现；当前只有 deploy-time WorkerCode/env 预算。

## 内部角色令牌（ADR-075）

- `/v1/peer/*` 用 `peer` 令牌；`/v1/internal/*`+`/v1/control/routes`+`/v1/diagnose` 用 `internal` 令牌；user-runtime 的 `/v1/queues|timers/dispatch` 用 `dispatch` 令牌。
- **唯一 Bearer JWT、无静态回退**。`internal`/`peer`/`dispatch`/`log` 使用不同派生 key，服务端根据已验证的 kid 限定用途，无重复 role claim。只向调用组件交付所需 JWT；确需本地刷新签名的可信组件仅获所需用途 key，不向所有组件扩散 root。
- **安全默认**：`Validate()` 在未设 `CELLHIVE_ALLOW_INSECURE_DEFAULTS=1` 时拒绝缺失/dev 根密钥（否则 admin 面可被已知凭据访问，ADR-134/137）。
- **诚实说明**：无 PKI 时角色隔离 = 独立 secret，非密码学身份；mTLS/内部 CA 为后续更强方案（未做）。

## JWT 与持久化 issuer（ADR-304，本地验收通过，未部署）

- header 必须为 `alg=HS256, typ=JWT, kid=<key-id>`；公共 claims 为 `ver=1,type,sub,aud,iat,exp`，默认 audience 为 `cellhive`，时间为 Unix 秒。未知算法/类型、歧义 header、无效时间、错误 audience 或签名一律拒绝。
- `namespace` 的 `ns` 是非空字符串数组，仅支持精确名称、末尾 `*` 前缀、单独 `*`。issuer 上限、token 范围、请求范围逐层收窄；超出上限拒绝整张 token，不静默裁剪。列表、日志、指标和跨资源引用同样受限。
- `binding` 的 `ns` 是精确字符串，`kind/name` 也必须精确；不能部署或管理 issuer。`worker/version/resource` 固定映射字段仅平台签发。namespace JWT 调 binding 也必须显式指定资源，执行注册表解析和 resource fence。
- 单个 vwork issuer key 可签 `vwork-*` 前缀、单 App namespace 或受限 binding JWT，不强制两把 key；无 `actions`、无 `key_version`。key 绝不随请求传输，持有 JWT 不能再签发。
- issuer 元数据位于 `__platform__/__control__/main` SQLite：随机不可复用 ID、显示名称、namespace 上限、enabled、最大 TTL（`0 < TTL <= 86400` 秒）、创建/更新/删除时间，不存明文 key。`kid=issuer/<immutable-id>`；同名重建分配新 ID，旧 token 不复活。
- 每次验证必须权威读取有效记录（可初版不缓存）；不存在、禁用、软删除、超 TTL 或越界均 fail closed。删除不可恢复；创建/禁用/删除为控制 cell 捕获写，满足持久化确认后成功。不得信任非 owner 本地 SQLite。
- 若使用正缓存，授权最多持续权威读取开始后的 5 秒；错误不得续期，过期查询失败拒绝。本地管理写失效缓存并阻止旧在途读回填；不得声称即时全局撤销。
- JWT key = `HKDF-SHA256(decoded_root_key, nil, "cellhive/jwt/v1/" + kid, 32)`。旧 `ScopeSecret` 派生仅保留给 Hyperdrive 等非 HTTP 凭据，不用于 JWT。
- bootstrap 用 root 派生的 `token/admin` key 签短期 admin JWT，创建 registry 与故障恢复；key 本身不是 Bearer。该系统身份不是可删除的业务 issuer，**业务 issuer 缺记录无旁路**。
- owner 转发保留原始授权并在目标重新验证；CF bridge 不得换成全权凭据而丢失 namespace 上限。issuer 权威点查使用独立内部认证，避免递归。
- assets/Tail 会话使用用途隔离 key，授权不超原 token，issuer 撤销约束同样适用。真实 stock Wrangler Tail 的会话 query token 是协议例外，不是普通用户 header 缺失时的回退。
- **ADR-178 明确例外**：私网 `:7001/metrics` 保留免鉴权全量 pull，仅允许可信采集器通过私网与防火墙/NetworkPolicy 访问，不得公网暴露或允许租户出网触达；不需要 JWT 刷新器、代理或额外服务。业务 namespace 视图 `/v1/control/metrics` 必须 JWT 鉴权并过滤，平台级或无法归属的序列不能泄漏给受限租户。admin 监听的全量 `/metrics` 仅允许 admin 或显式 `*` namespace JWT。长期运行的鉴权 API 客户端仍须刷新 JWT，此要求不适用于私网 metrics pull 例外。

## 验收状态

ADR-304 本地验收已通过，证据见 [测试](testing.md) 与 [已知问题](known-issues.md)；未部署，不宣称生产切换完成。新 DO grant 重验 parent，执行 metadata 已签名；已签发 ticket 的撤销边界最多30秒，不是即时全局撤销。

## 根密钥来源（ADR-150）

- `CELLHIVE_ROOT_KEY`（内联）→ `CELLHIVE_ROOT_KEY_FILE`（挂载文件，Docker `_FILE`/K8s secret volume 惯例）→ 仅在 `CELLHIVE_ALLOW_INSECURE_DEFAULTS` 下用 dev 根；文件不可读/为空即失败关闭。
- **云 KMS（Vault/AWS KMS/Aliyun KMS）未接入**：以上两者是"取根密钥字节"的接缝，接入 KMS 只需在该接缝实现一次解密/拉取；当前无云凭据环境，记为残余。

## mTLS / 内部 CA（决定：暂不做，ADR-150）

- 内部角色隔离依赖独立用途 key 与私网隔离；ADR-304 将传输凭据统一为短期 JWT。`:7001` 不公开，`:8082` 仅通过受控 admin 入口提供服务。
- **不做 mTLS 的理由**：需要一整套 CA/签发/轮换/信任分发（当前固定集群无 PKI），且网络隔离 + 独立 secret 已是主防线；引入 mTLS 收益边际、运维面显著变大。
- 何时再评估：跨信任域部署（cell-agent 与非受信网络互通）、或合规要求证书身份时；可在 Bearer JWT 之外增加传输层证书校验。

_最后更新：2026-09-25_
