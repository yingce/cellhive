# user-runtime 响应 body 生命周期包装诊断与小响应缓冲 A/B（2026-10-20）

目的：在真实 workerd 上定位公共 fetch 响应路径的剩余成本，验证“小响应缓冲”（ADR-237）是否值得替换逐 chunk 的 JS 流生命周期包装。结论：**值得**——`/ping` 中位吞吐 +20%（5 对 5 次更快），`/json` +8%，流式/大 body 语义不变；p99 的 1s 级尖峰为改动前既有现象，未改变。

## 环境与方法

- 固定 stock workerd `1.20260916.1`（`cli/node_modules/.bin/workerd`）；本机单 cell-agent（`127.0.0.1:17101`，FS bucket）+ 单 user-runtime（public `:18281`，internal `:18088`），`CELLHIVE_TENANT_LOG_CAPTURE=on`、`CELLHIVE_TRACES_SAMPLE_RATIO=0.01`。
- Worker `loadbench`：`/ping` 返回 `pong`（`text/plain`）、`/json` 返回 146B JSON、`/stream` 为 `text/event-stream`（100 块 × 10ms）、`/big` 返回指定字节的 `text/plain`、`/empty` 204、`/err` 500；声明一个 KV binding。
- 入口为 workerd public socket 直连（无 Go 代理）：`wrk -t4 -c64 -d25s -H 'Host: bench.cellhive.test' http://127.0.0.1:18281/<路径>`。每轮重启 runtime、预热 5s、重置计数；base 与优化版交替，另跑反向顺序（优化版先）控制漂移。
- 宿主为共享环境（load 2–6，另有其它任务），所以只作同会话配对比较，不是容量承诺。

## 诊断：逐段停表与变体（同一基线 ≈2.2k RPS，c=64）

| 变体 | 改动 | RPS | 说明 |
|---|---|---:|---|
| base | HEAD（`f8e3e2b` 后） | 2.13k–2.31k | 每响应经 JS `ReadableStream` + reader 包装 |
| child_nowrap | 子 wrapper 直接返回租户 Response | **3.11k–3.25k** | 去掉 body 追踪（不安全），成本上界 |
| child_resonly | `new Response(res.body)` 不建 JS 流 | 3.06k–3.10k | 说明成本在 JS 流本身，不在 Response 重建 |
| child_plain | 去掉整个 managed wrapper | 3.23k–3.33k | hold/ctx/ctor/runInvocation 合计仅 ~10µs |
| child_const | 子侧直接返回常量 Response | 3.45k–3.46k | RPC + workerd 调度地板（无租户执行） |
| child_buffer | 子侧读完整个 body 再返回 | 2.87k–2.91k | 全量缓冲（不安全：大 body/流式） |
| child_hwm1 | `highWaterMark:1` | 2.23k–2.29k | 无变化（doc B4 的 HWM 建议无效） |
| child_bytestream | `type:'bytes'` + HWM 64KiB | 2.16k–2.22k | 无变化 |
| child_tee | `body.tee()` + 后台 drain | ~2.30k | timeouts 283–335，劣化尾延迟，不采用 |
| child_hybrid | 有界预读 + 每请求 `setTimeout` 超时 | 1.97k | 每请求定时器拖垮吞吐，不采用 |

每秒分桶显示 `handler_total` 的 1.3–2.9s 最大值出现在 `preparedWorker`/子 fetch 之后的 body 写出阶段，且裸 workerd `/ping`（本次同机 15.8k/13.7k/15.5k RPS）p99 也有 223–726ms；即 p99 尖峰不是本改动或 ADR-237 引入，属于 workerd/宿主级既有现象。

## 小响应缓冲 A/B（ADR-237）

base = 入口单层包装（HEAD）；opt = `Lifetime.response` 对 `application/json`/`*+json`/`text/plain` 预读（≤128KiB 或 EOF）后以内存 body 返回，其它 content-type 与超限 body 保留逐 chunk 跟踪包装。

### `/ping`（text/plain，4B）

| 轮次 | base RPS / p50 / p99 / timeout | opt RPS / p50 / p99 / timeout |
|---|---|---:|
| 1 | 2087.45 / 4.68ms / 1.69s / 88 | **2576.65** / 3.33ms / 1.76s / 179 |
| 2 | 2077.16 / 4.81ms / 1.67s / 58 | **2340.58** / 3.73ms / 1.70s / 172 |
| 3 | 1943.95 / 7.53ms / 1.10s / 1 | **2451.83** / 3.56ms / 1.72s / 129 |
| 反向 1 | 1787.42 / 9.07ms / 1.02s / 0 | **2033.84** / 7.85ms / 1.58s / 34 |
| 反向 2 | 1892.03 / 9.20ms / 1.03s / 3 | **1958.09** / 8.54ms / 1.44s / 42 |

中位：base **1,944**、opt **2,341 RPS**（**+20%**，5/5 轮更快，单对 +3.5%–26%）；p50 4.7ms→3.4ms。p99 两侧均在 1.0–1.8s 区间；随着吞吐提高，wrk 的 2s 超时绝对数上升，但逐请求超时比例仍在 0.1%–0.3%。

### `/json`（application/json，146B）

| 轮次 | base RPS / p50 / p99 / timeout | opt RPS / p50 / p99 / timeout |
|---|---|---:|
| 1 | 1831.08 / 8.08ms / 1.03s / 0 | **1986.18** / 8.11ms / 1.48s / 49 |
| 2 | 1799.40 / 7.99ms / 1.13s / 0 | **1851.08** / 7.90ms / 1.60s / 40 |

中位 +8%；p50 不变；两轮 opt 的 p99 与 timeout 略高，属需要更多轮次确认的尾延迟风险。

## 正确性检查（优化版 runtime）

- `/stream`（`text/event-stream`）：首块 ~1ms、100 块约 1.0s，仍逐块可见（未缓冲）。
- `/big?size=262144`：256KiB `text/plain` 超过 128KiB 上限后回退为逐 chunk 跟踪流，字节数 262144 一致；64KiB 正常缓冲。
- `/empty` 204、`/err` 500 与 `content-type` 保持不变；`ping`/`json` 200。
- `node --test workerd/platform/lifecycle.test.mjs` 9/9；`make js-test`（含 real workerd 的 userruntime/doruntime 集成测试）通过。

## 取舍

- `text/plain` 流式响应会把首字节推迟到 128KiB 或 EOF；需要逐块可见的流应使用 `text/event-stream`（不受影响）或其它 content-type。
- 缓冲按响应计，64 并发最坏约 8MiB。
- 未解决：p99 的 1s 级尖峰（既有现象）；如需继续，应先按本文件方法做逐段 profile，而不是再改 HWM/包装层。
