# 路由与版本解析

## 入口

没有独立 gateway。南北向入口 = **运维自备的边缘代理（TLS + host 粗分流）** + **user-runtime loader（平台代码）**。

```
公网 ──TLS──▶ 边缘代理（Traefik/nginx/云 LB）─┬─▶ user-runtime :8081（租户）
                                             └─▶ cell-agent :8082（admin/CLI）
```

- 边缘代理：TLS 终止、host → 后端的粗分流；**平台不下发边缘配置**（ADR-132），运维静态配置（含内置域通配符 `<base>`）；
- user-runtime loader：**Worker 级路由 + 版本解析 + 清头 + request-id + 保留命名空间拦截**（先于租户模块执行），并做 host 门控（未注册 host 404）。

## 路由投影

**路由投影** = 控制面维护的映射：

```
host / path  →  (namespace, worker)  →  active version
```

- 存于 control cell（按 app 分片）；由 `cell-agent` 提供读取（`:8082` admin 面，内部读取亦可走 `:7001`）；
- **下发方式：纯拉取 + TTL**（ADR-031，无 push）；host 缓存负缓存/单飞/TTL 重验证/失败退避（ADR-115），撤销靠轮询 `/v1/control/routes` 的 ETag/revision，**撤销 <5s 生效**（ADR-116）；
- 陈旧窗口内个别副本仍服务**旧不可变版本**，请求仍能正常完成（无正确性问题）。

## 请求路径（普通 fetch）

```
Client → Traefik(TLS, host) → user-runtime :8081
  loader：
    1. host/path → (ns, worker)          # 路由投影
    2. (ns, worker) → active version      # 路由投影
    3. 拦截保留命名空间（__system__/__platform__ 等）
    4. 剥离/覆盖客户端伪造的可信头；注入 request-id
    5. 构造 worker id = <ns>:<worker>:<version>
    6. workerLoader 按 id 加载不可变 bundle → 执行 fetch(env, ctx)
```

## 版本解析的三个来源（别混）

| 解析 | 回答 | 谁做 |
|---|---|---|
| **公开路由版本** | 这个 host/path 用哪份代码？ | user-runtime loader（读投影） |
| **绑定冻结版本** | service binding 指向哪个目标版本？ | user-runtime（读 caller metadata 的冻结版本） |
| **派发版本** | cron/queue/workflow 用哪个版本？ | cell-agent（active 或冻结） |

## host 形态

| 类型 | 例子 | 后端 |
|---|---|---|
| 内置 worker 域 | `<ns>-<worker>.<CELLHIVE_BASE_DOMAIN>` | user-runtime |
| 自定义域 / `routes` | 任意 host（+ 可选路径前缀） | user-runtime |
| admin/CLI | admin host（或独立域名） | cell-agent :8082 |

- **内置域**：`deploy`/`promote` 时系统生成 `hosts(kind='builtin')` + `routes(host,'',ns,worker)`；`<base>` 通配符与证书由边缘/运维配置（ADR-131/132/133）；
- **自定义域 = 登记即授权**（ADR-133）：`cellhive domain add <host> <ns> <worker>` 后**立即生效**，不做 DNS/CNAME/TXT 校验；host 归属冲突时已注册（verified）409、内置域空间拒绝认领；
- **挂载前缀总是剥离**：route 含占位 `(host,path)` 时，worker 看到的路径 = 请求路径去掉挂载前缀；`path=''`/`'/'` 表示整 host；按**段边界**匹配（`/api` 不匹配 `/apix`，ADR-131）；
- `workers_dev` → 命名空间路径（见 [`wrangler-compat.md`](./wrangler-compat.md)）；`preview_urls` 不支持。

## 保留命名空间

平台内部命名空间（如 `__system__`/`__platform__`）**不对外可路由**；loader 在解析阶段拦截并拒绝。

## 已实现

- 投影数据格式与缓存：`internal/control`（hosts/routes 表）+ `internal/server`（`/v1/control/routes`、ETag/revision）+ loader 缓存（ADR-115/116）；
- 拉取端点与鉴权：内部 token（`:7001`/`:8082`）；
- 自定义域绑定：`cellhive domain add|ls|rm` → `POST/DELETE /v1/control/domain`、`GET /v1/control/domains`（ADR-131/133）；
- 边缘：**不做下发**（ADR-132），由运维静态配置。

_最后更新：2026-09-17_
