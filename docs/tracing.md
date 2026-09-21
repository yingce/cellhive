# 追踪（OpenTelemetry OTLP）

CellHive 使用**标准 OpenTelemetry OTLP/HTTP** 导出分布式追踪（ADR-167）。CellHive **不自存 trace**：只把 span 发给你配置的 OTLP 后端（OpenObserve / OTel Collector / Tempo / Jaeger / 云），**换后端只改环境变量**。

- 实现：`internal/telemetry`（官方 `go.opentelemetry.io/otel` SDK + `otlptracehttp`）；workerd 侧 `workerd/platform/telemetry.js`。
- 默认**关闭**（`CELLHIVE_OTLP_ENDPOINT` 为空 = 无 span、无额外开销）。

```
cell-agent (Go) ──┐
do-supervisor(Go)─┼── OTLP/HTTP (:4318 /v1/traces 或 /api/<org>/v1/traces) ──▶ 后端
loader/host (JS) ─┘        ▲
   └── POST /v1/internal/telemetry/spans ── cell-agent 统一导出
```

## 1. 配置

| 变量 | 默认 | 说明 |
|---|---|---|
| `CELLHIVE_OTLP_ENDPOINT` | 空（关） | OTLP/HTTP 端点。可以是 base（自动补 `/v1/traces`）或**完整** `/v1/traces` URL。`http://` 自动按 insecure 处理 |
| `CELLHIVE_OTLP_HEADERS` | 空 | 导出请求头，`k=v,k2=v2`（值可含空格，按第一个 `=` 切分） |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | 入口新 trace 的头部采样比例；上游 `traceparent` 的 sampled 位优先（`ParentBased`） |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | OTLP Resource `service.name`（user-runtime 的入口采样也用该比例） |

启动日志出现 `otel export enabled endpoint=… ratio=…` 即生效。

## 2. 采了哪些 span

| 组件 | span | 关键属性 |
|---|---|---|
| cell-agent | `http.server`（每请求） | `http.route`、`http.status_code`、`cellhive.namespace/kind/name/scope` |
| cell-agent | `cell.durability_proof` | scope（`Capture.Wait` 持久化证明耗时） |
| cell-agent | `peer.append` | scope（每次向 follower 发副本） |
| do-supervisor | gate / restore | — |
| workerd loader | `http.server`（入口） | `http.method`、`cellhive.namespace/worker` |
| do-runtime host | `do.invoke` / `do.gate` | `cellhive.namespace/worker/class/do_kind` |

**传播与串联**（一条 trace 内）：

```
client ──traceparent──▶ loader http.server ──header──▶ cell-agent http.server ──ctx──▶ cell.durability_proof
                              │
                              └─(worker 内的 props-bound binding 调用)──▶ cell-agent http.server
                              │
                              └─(DO facade 带 traceparent)──▶ cell-agent(do_proxy) ──spec──▶ host do.invoke
                                                                                              └─▶ host do.gate ──header──▶ supervisor do.gate
```

- **能串**：worker→cell-agent 绑定调用、worker→DO（`do.invoke`）、DO→`do.gate`→supervisor `do.gate`、`cell.durability_proof`（都在同一 trace，且 DO/gate 父子关系正确——由真 workerd 测试断言）。
- **断点 1（DO 内的绑定调用）**：facet 里 `this.env.KV/D1/...`（props-bound entrypoint）**不带** DO 的 traceparent，会自成一条 trace。原因：workerd 给 host actor、facet、平台 entrypoint 各自独立的 isolate `globalThis`，而 CF 形状的绑定 API 没有"按请求传参"的口子（user-runtime 能串，是因为 loader 就是那个 isolate）。测试：`internal/doruntime TestDoRuntimeBindingInsideDO`（当前断言为空）。
- **断点 2（peer 复制）**：`peer.append` 跑在捕获流水线（`context.Background()` 派生 + group commit 合并多请求）上，**不是**某个请求 trace 的子 span；按"批"独立成 trace。
- 租户 isolate 的 `facades.js` 回退路径不上报 JS span，但其 `pfetch` 仍带 `traceparent`，所以 cell-agent 的服务端 span 仍能挂到该 worker 的 trace 上。
- 采样为**入口头部采样**：未采样只透传、不产生 span。

## 3. 对接 OpenObserve（重点）

OpenObserve 的 OTLP 入口（官方文档）：

- 自托管：`http(s)://<host>:5080/api/<org_name>/v1/traces`（`<org_name>` 默认 `default`）
- Cloud：`https://api.openobserve.ai/api/<org_name>/v1/traces`
- 鉴权：`Authorization: Basic base64(userid:password)`

### 3.1 一键示例（compose profile）

仓库自带 `tracing` profile：

```bash
# 1) 生成 Basic 头（默认账号 root@example.com / Complexpass#123）
export CELLHIVE_OTLP_HEADERS="authorization=Basic $(printf %s 'root@example.com:Complexpass#123' | base64 -w0)"
# 2) 指向 OpenObserve 的自托管 OTLP 入口
export CELLHIVE_OTLP_ENDPOINT=http://openobserve:5080/api/default
export CELLHIVE_TRACES_SAMPLE_RATIO=1        # 先全采，验证通了再调小
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)

docker compose -f deploy/compose/docker-compose.yml --profile tracing up --build
```

说明：
- `CELLHIVE_OTLP_ENDPOINT` 只写到 `/api/default` 即可（导出器会自动补 `/v1/traces`）；写全 `…/api/default/v1/traces` 也接受。
- `CELLHIVE_OTLP_HEADERS` 的值可含空格（`Basic ` 后是 base64），逗号分隔多个头。
- OpenObserve 也需要 `5080` 端口；compose 已映射。UI 默认 `http://localhost:5080`，登录用 `ZO_ROOT_USER_EMAIL/PASSWORD`（默认见 compose）。

### 3.2 已有 OpenObserve（不在 compose 内）

```bash
export CELLHIVE_OTLP_ENDPOINT=http://<openobserve-host>:5080/api/default
export CELLHIVE_OTLP_HEADERS="authorization=Basic $(printf %s '<user>:<pass>' | base64 -w0)"
export CELLHIVE_TRACES_SAMPLE_RATIO=0.1
# 重启 cell-agent / user-runtime / do-supervisor（带这些环境变量）
```

Cloud 用户：`CELLHIVE_OTLP_ENDPOINT=https://api.openobserve.ai/api/default`（HTTPS 自动走 TLS）。

### 3.3 验证（已实测）

1. 访问一个租户路由（或任意 `/v1/...`/`/healthz` 请求）产生流量；
2. OpenObserve UI → **Traces** 能看到 `service_name=<CELLHIVE_SERVICE_NAME>` 的 trace；
3. 或直接查 API（traces 的 stream 名是 `default`）：

```bash
NOW=$(date +%s%6N); START=$((NOW-3600000000))
curl -s -u '<user>:<pass>' \
  "http://<host>:5080/api/default/default/traces/latest?start_time=$START&end_time=$NOW&from=0&size=10"
# → {"total":N,"hits":[{"trace_id":"...","first_event":{"service_name":"cellhive-cell-agent","operation_name":"http.server"},...}]}
```

> 注意：`POST /api/<org>/_search` 是**日志**搜索 API，用它查 traces 会报 `Search stream not found`；traces 用 `…/{stream}/traces/latest`（或 UI）。

4. 若没数据：确认启动日志有 `otel export enabled endpoint=… ratio=…`、`CELLHIVE_TRACES_SAMPLE_RATIO > 0`、Basic 头正确（`printf %s` 不要带换行）、`5080` 网络可达；OpenObserve 侧可看容器日志是否出现 `POST /api/<org>/v1/traces 200`（UA `OTel OTLP Exporter Go/…`）。

> 只导出 **traces**；OpenObserve 的 logs/metrics 入口（`/api/<org>/v1/logs|metrics`）不在这条 trace 验收链路中。当前也不采集租户 console 日志；指标走 Prometheus `/metrics`。

## 4. 其他后端

**OTel Collector**（推荐生产用，便于再分发到多后端）：

```yaml
receivers: { otlp: { protocols: { http: { endpoint: 0.0.0.0:4318 } } } }
exporters:  { otlphttp/openobserve: { endpoint: http://openobserve:5080/api/default, headers: { authorization: "Basic ${BASIC}" } } }
service:    { pipelines: { traces: { receivers: [otlp], exporters: [otlphttp/openobserve] } } }
```
CellHive 指向 `CELLHIVE_OTLP_ENDPOINT=http://collector:4318`（无 headers）。

**Tempo**：`CELLHIVE_OTLP_ENDPOINT=http://tempo:4318`（无需鉴权）。
**Jaeger v2**：`CELLHIVE_OTLP_ENDPOINT=http://jaeger:4318`。
**云 OTLP**：填云厂商的 OTLP endpoint + `x-otlp-api-key`/`authorization` 头。

## 4.5 日志关联与多租户（推荐）

- **trace ↔ 日志**：当前不采集租户 `console.*`（动态 `workerLoader` 的 native Tail Worker 配置在 pinned stock `workerd 2026-06-15` 上被拒绝，旧 `log-tail.js` 已删除），因此不能从 trace 跳到同请求的租户 console 日志。平台可信日志若带 trace 字段，仍可由后端关联。
- **多租户**：节点 push 到**内网 Collector**（`deploy/observability/`），由 Collector 脱敏、按 `cellhive.namespace` 路由、tail-sample，再落 OpenObserve；租户查询走**后端 org/stream + 用户角色**，平台不暴露查询面，也不把 OTLP 凭据给租户。细节见 [`observability.md`](./observability.md) 的"多租户与对外查询"。

## 5. 边界与残余

- 见 §2 的两个串联断点：**DO 内 props-bound 绑定调用**（新 trace）、**peer.append**（按批的独立 trace）。
- JS 只覆盖**平台 worker**（loader 入口、do host invoke/gate）；租户 isolate 的 `facades.js` 回退路径没有 internal token，不上报 JS span（但服务端 span 仍挂得上）；DO `/v1/do/connect`·abort 与 compaction/upload 后台循环无独立 span。
- JS span 时间戳为毫秒精度（`Date.now()*1e6`）。
- 采样是入口头部采样；`CELLHIVE_TRACES_SAMPLE_RATIO=0` 时新 trace 不采样（上游显式 `-01` 仍会采样）。
- 端点为空时全程 no-op；导出为后台批处理（BatchSpanProcessor，5s 超时 + 有界丢弃），不阻塞请求路径。
- **日志（可选）**：`CELLHIVE_OTLP_LOGS=off|tail|all` 用同一端点/headers 导出 OTLP **logs**（`/v1/logs`）；`tail` 只在 `cellhive tail` 订阅期间导出，且订阅会**广播到全部活节点**（ADR-173）。详见 [`observability.md`](./observability.md) §日志。

## 相关

- 指标/日志/告警：[`observability.md`](./observability.md)
- 全部环境变量：[`configuration.md`](./configuration.md)
- 决策：[`decisions.md`](./decisions.md) ADR-167、ADR-146（traceparent 透传）

_最后更新：2026-09-19_
