# 配置与环境变量

权威来源是代码：`internal/config/config.go`（cell-agent）、`cmd/user-runtime/main.go`、`cmd/do-runtime/main.go`、`cmd/cellhive/main.go`（CLI）、`internal/workerdbin`、`internal/bundler`。本文档按子系统列出**全部**环境变量、默认值与作用；与代码冲突时以代码为准。

## 解析规则

| 类型 | 规则 |
|---|---|
| 字符串 | 未设置或空串 → 默认值 |
| 布尔 | `true`/`1` 为真，其余（含 `false`/`0`）为假；例外：`CELLHIVE_DO_PREVENT_EVICTION` 只接受**恰好** `true`/`false`（否则启动失败） |
| 时长 | **统一用 Go 语法**：`100ms`、`30s`、`5m`、`24h`；非法值回退默认。没有 `_MS`/`_S` 后缀的整数变量 |
| 字节 | 纯整数（字节）或 `512MB`/`2GiB`/`1g`；KB/MB/GB 十进制，KiB/MiB/GiB 二进制 |
| 列表 | 逗号分隔，逐项去空白，空项忽略 |
| 成对/速率 | `a:b`（如 `CELLHIVE_LOG_BUFFER=1000:200`）或 `rps/burst`（如 `CELLHIVE_NS_RATE=100/200`） |

**凭据模型（ADR-137）**：只设**一个** `CELLHIVE_ROOT_KEY`，各角色令牌用 HKDF-SHA256 按域分离派生，因此所有组件只要共享同一个 root 就自动一致，无需逐个配置：

```
peer / internal / dispatch / log / admin / scope / do-ticket / secrets-root
     = HKDF-SHA256(ROOT_KEY, "cellhive/token/<role>")
```

`CELLHIVE_ADMIN_TOKEN` 仍可单独覆盖 admin 凭据（便于运维独立轮换）。未设 root 且未开 `CELLHIVE_ALLOW_INSECURE_DEFAULTS` 时启动失败。

**迁移提醒**：ADR-136/137 删除或改名的变量（如 `CELLHIVE_TOKEN_*`、`CELLHIVE_S3_*`、`CELLHIVE_NS_RPS`、`CELLHIVE_*_MS`/`_S`）如果仍被设置，各组件启动时会打印 WARN 并给出替代名（`config.LegacyEnvWarnings`），不会静默生效。

## 安全 / 凭据

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_ROOT_KEY` | 空 | 平台根密钥（base64 或 hex，≥16 字节，建议 32）；派生全部角色凭据。生产**只配这一个**（无文件时的首选） |
| `CELLHIVE_ROOT_KEY_FILE` | 空 | 根密钥**文件**路径（Docker/K8s secret 挂载；读取后 trim）。优先于 dev 回退、低于内联 `CELLHIVE_ROOT_KEY`；文件不可读/为空则**失败关闭**（ADR-150） |
| `CELLHIVE_ADMIN_TOKEN` | 空→派生 | 可选：单独覆盖控制面 admin 凭据 |
| `CELLHIVE_ALLOW_INSECURE_DEFAULTS` | `false` | 允许本地开发根（`DevRootKey`）。生产必须 false |
| `CELLHIVE_OIDC_JWKS_URL` | 空 | 设置后 admin 可额外用已验证 JWT bearer（ADR-036/131） |
| `CELLHIVE_OIDC_ISSUER` / `_AUDIENCE` | 空 | JWT iss/aud 校验（JWT 必须含 `exp`） |

## 对象存储

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_BUCKET` | 空 | `s3://<bucket>` 用 S3 兼容存储；空则用文件系统桶 |
| `CELLHIVE_BUCKET_DIR` | `./.cellhive/bucket` | 文件系统桶目录。必填 |
| `AWS_ENDPOINT_URL` | 空 | S3 端点（MinIO/云；标准 AWS 变量名） |
| `AWS_REGION` | `us-east-1` | 区域 |
| `AWS_ACCESS_KEY_ID` | 空 | 访问凭据 |
| `AWS_SECRET_ACCESS_KEY` | 空 | 密钥 |
| `CELLHIVE_S3_PATH_STYLE` | 有自定义端点→`true`，否则 `false` | path-style；默认按是否有自定义端点推断（本地/兼容存储常用 path-style，云端常用 virtual-host） |

## cell-agent：进程与监听

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_NODE_ID` | `node-1` | 节点稳定身份（owner/handoff key），必填 |
| `CELLHIVE_SESSION_ID` | 启动纳秒 | 本次会话 id，重启即新 |
| `CELLHIVE_ADVERTISE` | `127.0.0.1:7001` | 对内广告地址（owner 转发目标）；内部只有 REST `:7001` |
| `CELLHIVE_PEER_URL` | `http://127.0.0.1:7001` | 本节点 REST 回指地址（peer 复制目标） |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | 本地工作目录根（spool、DO 盘等由此派生） |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 一次性运行目录根；各 runtime 用其子目录 |
| `CELLHIVE_REST_ADDR` | `:7001` | 内部 REST（Go↔Go 与 workerd bindings 共用，无 gRPC） |
| `CELLHIVE_ADMIN_ADDR` | `:8082` | admin/控制面监听 |
| `CELLHIVE_DO_RUNTIMES` | 空 | do-runtime 列表（DO 放置）；空则禁用 `/v1/do/invoke` |

## 持久性 / 复制 / RPO

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_DURABILITY` | `auto` | ack 姿态：`auto`/`fleet`/`bucket`；`fleet` 必须 follower fsync，绝不静默 ack |
| `CELLHIVE_BUCKET_WAIT` | `true` | true=等桶上传后 ack（RPO=0）；false=入队即 ack；`fleet` 必须 true |
| `CELLHIVE_LEASE_TTL` | `10s` | owner 租约，必须为正 |
| `CELLHIVE_PEER_LATENCY` | `0` | 合成单向延迟（压测，往返 ×2） |
| `CELLHIVE_PEER_PIPELINE` | `4` | 每 lane 在途批次数，1=关流水线 |
| `CELLHIVE_PEER_HEDGE_MS` | `adaptive` | 副本 hedge（ADR-164）：先写 primary；`adaptive`/`auto`（默认）超 `max(250ms, 4×最近最慢 append)` 再发第二份；`0`=只发 primary（单份）；`>0`=固定等待 ms |
| `CELLHIVE_PEER_HEDGE_MAX_MS` | `2000` | 自适应 hedge 等待上限（backstop） |
| `CELLHIVE_OTLP_ENDPOINT` | 空=关 | OpenTelemetry OTLP/HTTP 导出端点（如 `http://collector:4318` 或 OpenObserve `http://openobserve:5080/api/default`；完整 `/v1/traces` URL 也接受）。空 = 完全关闭、零开销（ADR-167；见 [`tracing.md`](./tracing.md)） |
| `CELLHIVE_OTLP_HEADERS` | 空 | OTLP 导出请求头，`k=v,k2=v2`（鉴权用） |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | 入口新 trace 的头部采样比例（上游 `traceparent` 的 sampled 位优先） |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | OTLP Resource 的 `service.name`（user-runtime 里入口采样用同一变量） |
| `CELLHIVE_OTLP_LOGS` | `off` | OTLP logs 导出：`off`=关；`tail`=只有 `cellhive tail` 订阅的 worker 导出；`all`=全部（ADR-172；需 `CELLHIVE_OTLP_ENDPOINT`） |
| `CELLHIVE_UPLOAD_SHARDS` | `0`→NumCPU(≤8) | 并行桶上传通道 |
| `CELLHIVE_CAPTURE` | 启用 | `off`/`0`/`false` 关闭捕获=写仅本地无复制证明 |
| `CELLHIVE_CAPTURE_GROUPCOMMIT` | `0`→2ms | WAL 合并窗口 |
| `CELLHIVE_CAPTURE_PIPELINE` | `0`→8ms | 提交延迟阈值，超过则多 chunk 在途 |
| `CELLHIVE_CELL_WAL_CHECKPOINT` | `64MiB` | 捕获 cell WAL 截断阈值，`0` 关闭 |
| `CELLHIVE_FORGET_ON_LOSS` | `true` | 失所有权删本地文件（false 需配盘预算） |

## cell-agent：本地盘与内存预算

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_MAX_OPEN_CELLS` | `0` 无限 | 缓存句柄上限 |
| `CELLHIVE_MAX_RESIDENT_CELLS` | `0` 无限 | 常驻句柄上限 |
| `CELLHIVE_CELL_IDLE` | `0` 不清理 | 空闲句柄关闭 |
| `CELLHIVE_CELL_DISK_MAX` | `0` 无限 | cell 文件字节预算（LRU 删非 owned） |
| `CELLHIVE_CELL_DISK_SWEEP` | `1m` | janitor 间隔 |
| `CELLHIVE_DISK_HIGH` | `0` 关 | 高水位→overloaded 拒 claim |

## cell-agent：归属 / 伸缩

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_CELLS_PER_NODE` | `100` | 每节点容量目标 |
| `CELLHIVE_PLACEMENT_WEIGHT` | `0`→CPU | ownership 份额 |
| `CELLHIVE_PLACEMENT_AZ` | 空 | 本节点故障域（rack/zone）；设置后 follower 选择**优先不同 AZ**（ADR-151）。空=无偏好 |
| `CELLHIVE_REBALANCE_INTERVAL` | `0` 关 | 再平衡循环 |
| `CELLHIVE_REBALANCE_MAX_MOVE` | `32` | 单轮最多释放 |
| `CELLHIVE_AUTOSCALE_MIN` | `1` | 目标下限 |
| `CELLHIVE_AUTOSCALE_MAX` | `0` 不限 | 目标上限 |
| `CELLHIVE_AUTOSCALE_INTERVAL` | `30s` | 评估间隔 |
| `CELLHIVE_AUTOSCALE_COOLDOWN` | `5m` | 动作**变化**的冷却窗口（防抖；0=关，ADR-151）。冷却期内返回 `hold/cooldown` + `cooldown_remaining_ms` |

## cell-agent：后台循环

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 关) | 到期计时派发 |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 到期上限 / fired 标记保留 |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 关) | queue 消费 |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0`（store 默认） | 单批消息数 / 可见性租约 |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | 失败批次的重投延迟（重试上限/DLQ 由 per-consumer 配置） |
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 关) | cron→timer 物化 |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 关) | fleet waker（TTL 自动取 2×） |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 上限 / fired 标记保留 |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | 连续错误时的退避封顶（base=interval，`interval·2^fails`） |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 关) | 本地 wake 索引修复扫描（ADR-177）；启动先跑一次 |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | 单 pass 检查的本地 cell 数（轮转窗口，几趟覆盖全部） |
| `CELLHIVE_DRAIN_TTL` | `30s` | drain token 有效期（关停等待 = 同值） |
| `CELLHIVE_PAGED_RESTORE` | bool，默认 `true` | cell-agent：冷恢复时对"已 compaction 且链 ≥ MIN_BYTES"的 cell 用 fault-in VFS 按页加载（ADR-160） |
| `CELLHIVE_PAGED_MIN_BYTES` | 字节，默认 `256MiB` | 小于该值的链整克隆；`0` = 一律分页 |
| `CELLHIVE_PAGED_HYDRATE_MBPS` | 整数，默认 `16` | paged cell 后台补齐速率（MiB/s，每节点同时一个）；`0` = 保持稀疏、每个冷页读桶 |
| `CELLHIVE_PAGED_WINDOW_PAGES` | 整数，默认 `64` | 窗口预取：一次 ranged read 最多连取多少相邻页（另有 256KiB 预算） |
| `CELLHIVE_PAGED_PREFETCH_WORKERS` | 整数，默认 `4` | child 并发预取 worker 数 |
| `CELLHIVE_LTX_COMPRESSION` | bool，默认 `true` | LTX page-map 用 LZ4（`WAL3`）；`false` 退回 `WAL2` 固定帧不压缩（省 CPU、桶字节更多；解码/GC 两者兼容，ADR-161） |
| `CELLHIVE_COMPACTION_INTERVAL` | `30s` (0 关) | L0→L1 压实 |
| `CELLHIVE_COMPACTION_MIN_SEGMENTS` | `64` | 触发段数 |
| `CELLHIVE_COMPACTION_MIN_BYTES` | `64MiB` | 触发字节 |
| `CELLHIVE_BUNDLE_GC_INTERVAL` | `0` 关 | bundle/assets GC |
| `CELLHIVE_BUNDLE_GC_GRACE` | `24h` | 未引用保留期 |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 永久) | 审计保留 |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` 永久 | 终态实例修剪 |
| `CELLHIVE_LOG_BUFFER` | `1000:200` | 日志尾缓冲 `<entries>:<workers>`（`cellhive tail --worker`） |
| `CELLHIVE_METRICS_NS_MAX` | `1000` | `/metrics` 上租户可归因指标的 ns 标签上限：超出记 `other`，空 ns 记 `platform`，0=不限（ADR-179） |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 关) | binding 声明缓存 |
| `CELLHIVE_NS_RATE` | 空（关） | 每 ns 写准入 `rps[/burst]`（缺省 burst=rps） |

## cell-agent：控制面 / 域名 / DO

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_BASE_DOMAIN` | 空 | 内置域 `<ns>-<worker>.<base>`；空=禁用 |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | deploy 首次自动建 app |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | deploy 急切重启 DO |
| `CELLHIVE_DISPATCH_URL` | 空 | 计时/queue/cron/workflow 派发目标 —— 必须是 **user-runtime 内部特权端点**（`http://user-runtime:8088`）。空则派发循环不启动（compose/k8s/Helm 默认已设） |

## workerd / 打包

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_WORKERD` | 自动查找 | workerd 路径（优先 pin `1.20260615.1`） |
| `CELLHIVE_ESBUILD` | 自动查找 | esbuild 路径 |

## user-runtime

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_CELL_URL` | `http://127.0.0.1:7001` | cell-agent REST |
| `CELLHIVE_ROOT_KEY` | 必填 | 派生 internal/scope/dispatch/log 凭据 |
| `CELLHIVE_USER_RUNTIME_PORT` | `8081` | 公开入口 |
| `CELLHIVE_USER_RUNTIME_INTERNAL_PORT` | `8088` | 内部派发 |
| `CELLHIVE_USER_RUNTIME_JS` | `workerd/user-runtime` | loader JS |
| `CELLHIVE_FACADES_JS` | `workerd/platform/facades.js` | facade 源 |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 运行目录（子目录 `user-runtime/`） |
| `CELLHIVE_SERVICE_NATIVE` | 空=启用 | `0` 关闭原生 service RPC |
| `CELLHIVE_DO_DIRECT` | 空=启用 | `0` 关闭 owner-hint 直达 |
| `CELLHIVE_TENANT_OUTBOUND` | 空→`public` | 出网类别 `public`/`private`/`local` |
| `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | 空 | BYO AI 端点与密钥；空=不注入 |

## do-runtime

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_DO_ADDR` | `*:8788` | workerd 监听 |
| `CELLHIVE_DO_NODE` | 主机名 | 节点 id |
| `CELLHIVE_DO_ADVERTISE` | `http://127.0.0.1:8788` | 广告地址（owner 转发目标） |
| `CELLHIVE_DO_PREVENT_EVICTION` | `true` | resident/evictable，须恰好 `true`/`false` |
| `CELLHIVE_DO_LEASE` | `30s` | DO owner 租约（Go duration，转为整秒给 host actor） |
| `CELLHIVE_DO_RUNTIME_JS` | `workerd/do-runtime` | host actor JS |
| `CELLHIVE_DO_GATE_URL` | 空 | 输出门基址；空=不过门 |
| `CELLHIVE_DO_OBJECT_INDEX` | `false` | 对象注册表持久化到桶（`true`/`1`） |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | DO SQLite 盘 = `<DATA_DIR>/do` |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 运行目录（子目录 `do-runtime/`） |
| `CELLHIVE_CELL_URL` / `CELLHIVE_ROOT_KEY` / `TENANT_OUTBOUND` / `AI_*` | 同上 | 与 user-runtime 相同 |

## CLI（`cellhive`）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_ADMIN_URL` | `http://127.0.0.1:8082` | admin 端点 |
| `CELLHIVE_CONTROL_URL` | `http://127.0.0.1:7001` | 内部端点 |
| `CELLHIVE_ROOT_KEY` | 必填 | 派生 admin（`CELLHIVE_ADMIN_TOKEN` 可覆盖）与 internal 令牌 |
| `CELLHIVE_ADMIN_JWT` | 空 | 有值则改用 `Bearer`（ns 级授权） |
| `CELLHIVE_BIN` | 自动 | dev `--strict-build` 用的打包器 |

## 测试 / CI（非运行时）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_S3_TEST_ENDPOINT` | 空 | 指向既有 S3；空则 `make s3-test` 尝试起 MinIO，docker 不可用则 skip |
| `CELLHIVE_S3_TEST_ACCESS` / `_SECRET` / `_BUCKET` | `minioadmin`/`minioadmin`/`cellhive` | S3 集成测试凭据 |
| `CELLHIVE_PERF_GATE` | 空 | `make perf-test` 设置，启用性能门断言 |
| `TEST_BYTES` | — | `internal/config` 单测用 |

## 相关文档

- 服务与端口、K8s、滚动升级：[`deployment.md`](./deployment.md)
- 运维 runbook：[`operations.md`](./operations.md)
- 安全边界与凭据分层：[`security.md`](./security.md)
- 桶角色与凭据：[`storage-and-s3.md`](./storage-and-s3.md)

_最后更新：2026-09-19_
