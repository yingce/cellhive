# 安全模型

## 信任边界

| 组件 | 边界 |
|---|---|
| 边缘代理（运维） | 只做 TLS 与 host 粗分流，**不做业务鉴权**；平台不下发边缘配置（ADR-132） |
| `user-runtime` | 入口 + 执行租户代码（跨命名空间 service binding 需目标 ns 授权，ADR-144）；租户 loaded worker **仅公网出网**；平台 loader 先于租户模块执行 |
| `do-runtime` | 仅私网；**不持对象存储凭据** |
| `cell-agent` | `:7001`(REST，Go↔Go 与 workerd bindings 共用) 内部数据/解析**仅私网**；`:8082` admin 经边缘 + 凭据（静态 token 或 OIDC JWT）；数据面路径不可达控制面处理器。内部调用按**角色令牌**鉴权（`peer`/`internal`/`dispatch`，由 `CELLHIVE_ROOT_KEY` 派生，ADR-075/137） |
| 对象存储 | 长期凭据**只在 cell-agent**；bundle/assets 读走短期 scoped 凭据（ADR-030） |

## 多租户隔离（ADR-029）

1. **网络隔离（主防线）**：user-runtime 的 capnp 中，租户 loaded worker 的 `globalOutbound` = **公网 only network service**（可用 `CELLHIVE_TENANT_OUTBOUND` 显式加 `private`/`local`，ADR-130），默认**不含 RFC1918 / cell-agent 地址**。租户 Worker **无法**直接访问内网 `:7001/:8082`。
2. **token 不进租户 env**：internal token 只存在于 **host adapter（平台代码）与 cell-agent 之间**；租户只拿到 binding facade，拿不到 token/后端地址。
3. **scope 声明校验**：host adapter 调 cell-agent 时声明 `(ns, binding 类型/id)`；cell-agent 校验，越权即拒。binding 本身由 adapter 不可变 props 唯一绑定到具体 cell。
4. **纵深防御**：per-binding 签名 scoped token（namespace + binding 类型/id），即使隔离被绕也只能碰该绑定（**ADR-074**）。**平台本地计算、无过期**：撤销靠 `HasBinding` 每请求校验 + `SCOPE_SECRET` 轮换；**广权限 internal token 不进租户 loaded worker**，facade 只带 scoped token。

> 密钥：控制面 cell 存**信封密文**，根密钥在 cell 之外（env/KMS）；加载期在 cell-agent 内解密注入 `env`，明文只进 load envelope + workerd env。

## 管理后台（ADR-036/131）

- 单一管理后台（无独立 Web UI，= admin API），租户身份（ns/app）由请求携带；
- 鉴权两种：
  - **静态运维凭据** `CELLHIVE_ADMIN_TOKEN`（`x-cellhive-admin-token`）；
  - **OIDC/JWT**（ADR-131）：`CELLHIVE_OIDC_JWKS_URL` 配置后，Bearer JWT 必须验签且**必须含 `exp`**（无 `exp` 直接拒绝，ADR-135）；`cellhive_ns` 声明把调用者限定到命名空间，平台级操作需 `*`；列表/读端点按 ns 过滤，越权 403；JWKS 刷新单飞 + 在途用缓存密钥（慢 IdP 不阻塞校验）。
- **绝不暴露给租户或公网**：仅 admin host + 私网可达；
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

## 配额与 admission（P1，ADR-035）

- per-namespace 写速率限制；cell-agent 过载返回 `503 overloaded`；
- 单 DO WAL 上限；worker `limits`（cpu/subrequests）+ V8 heap 上限；
- 消费节点 lease 的 `pressured`/`shed_cells` 做 backpressure；
- queue DLQ 上限。

## 内部角色令牌（ADR-075）

- `/v1/peer/*` 用 `peer` 令牌；`/v1/internal/*`+`/v1/control/routes`+`/v1/diagnose` 用 `internal` 令牌；user-runtime 的 `/v1/queues|timers/dispatch` 用 `dispatch` 令牌。
- **唯一凭据、无回退**；比较为常量时间、fail-closed。所有角色凭据由**单一 `CELLHIVE_ROOT_KEY` HKDF 派生**（域分离，ADR-137），生产只配一个 secret；`CELLHIVE_ADMIN_TOKEN` 可选独立覆盖。
- **安全默认**：`Validate()` 在未设 `CELLHIVE_ALLOW_INSECURE_DEFAULTS=1` 时拒绝缺失/dev 根密钥（否则 admin 面可被已知凭据访问，ADR-134/137）。
- **诚实说明**：无 PKI 时角色隔离 = 独立 secret，非密码学身份；mTLS/内部 CA 为后续更强方案（未做）。

## scoped token 范围与委派签发（ADR-074/181）

- 绑定端点只认 `x-cellhive-scope-token`；claims `{ns,kind,name,iss?,exp_ms?}`。
- **ns 始终精确**（隔离边界）；`kind`/`name` 支持段级 glob（`*` 任意、`pre*` 前缀），**逐段锚定**（`acme` 不匹配 `acmex`）。
- **平台令牌**（`iss` 空）：loader 用 `SCOPE_SECRET` 本地签；每请求 `HasBinding` 校验。
- **委派令牌**（`iss` 非空）：可信租户平台用派生 issuer key（`HKDF(SCOPE_SECRET,"issuer/"+iss)`；`cellhive creds issuer <name>` 打印，或 `cellhive token ... --iss <name> --ttl 5m` 直接签）签发；**强制 `exp`**、按 ns/scope 授权（不再要求已登记 binding）。
- **诚实边界（重要）**：委派 key **不按 ns 收窄**——`IssueKey` 只由 `iss` 派生，故持有者可签任意 ns 的令牌（这正是"入口管多个 ns、可委派所有 ns"的设计）。平台只强制 **请求 ns == token ns**，**不能**阻止委派方自己选 ns；per-user/跨 ns 隔离由委派方自律。因此 **issuer key 泄漏 = 其被授权覆盖的全部 ns 沦陷**，止损只有轮换 `SCOPE_SECRET`/root（issuer 版本/allowlist 未实现）。
- **边界**：token 派生的资源（kv/vectorize/service/do）要求具体 name（通配无法解析 cell）；对外数据面入口（`ExternalHandler`）与 API key 管理未实现。

## 已实现

- capnp `globalOutbound`/network：`workerd/user-runtime/userruntime.go` 渲染 + `CELLHIVE_TENANT_OUTBOUND`（ADR-130）；
- 控制面鉴权中间件（角色令牌 + OIDC/JWT）与审计日志（含 actor/on_behalf_of/request_id，ADR-131）；
- scoped token 撤销：`HasBinding`（平台令牌）每请求校验 + `SCOPE_SECRET` 轮换（ADR-074）；委派令牌靠强制短 `exp` + issuer key 轮换（ADR-181）。

## 根密钥来源（ADR-150）

- `CELLHIVE_ROOT_KEY`（内联）→ `CELLHIVE_ROOT_KEY_FILE`（挂载文件，Docker `_FILE`/K8s secret volume 惯例）→ 仅在 `CELLHIVE_ALLOW_INSECURE_DEFAULTS` 下用 dev 根；文件不可读/为空即失败关闭。
- **云 KMS（Vault/AWS KMS/Aliyun KMS）未接入**：以上两者是"取根密钥字节"的接缝，接入 KMS 只需在该接缝实现一次解密/拉取；当前无云凭据环境，记为残余。

## mTLS / 内部 CA（决定：暂不做，ADR-150）

- 现状：内部角色隔离 = **独立派生 secret（HMAC/常量时间比较）+ 私网隔离**（ADR-075/137）；`cell-agent :7001/:8082` 不对外。
- **不做 mTLS 的理由**：需要一整套 CA/签发/轮换/信任分发（当前固定集群无 PKI），且网络隔离 + 独立 secret 已是主防线；引入 mTLS 收益边际、运维面显著变大。
- 何时再评估：跨信任域部署（cell-agent 与非受信网络互通）、或合规要求"密码学身份"时；接口层（`token` header）已抽象，届时可加证书记录。

_最后更新：2026-09-17_
