# user-runtime Tail 热路径分段压测（2026-10-20）

环境：同机 2 个 cell-agent（共享 FS bucket，KV A/B 分别在不同 agent）、**1 个 user-runtime**，直连其 workerd `18281` socket。Worker 的 `/ping` 返回 `pong`，不调用 KV，也不输出 `console.*`。固定 `workerd 2026-09-16`，相同 Worker 配置和 `wrk -t4 -c64 -d20s`，每轮启动新隔离目录并预检 `/ping`、`/read`、`/write`。每轮从生成的 Cap'n Proto 配置读回 `CH_LOG_CAPTURE`，避免误把关闭 Tail 的结果当作开启。

| 变体 | `CH_LOG_CAPTURE` | 单 runtime `/ping` RPS | wrk timeout | 原始输出 |
|---|---:|---:|---:|---|
| 原版：每个原生 Tail 事件转换并单独 POST | 1 | 973 | 330 | `/tmp/cellhive-local-rps.o9rhYJ/` |
| 原生 Tail 挂载，但 handler 立即返回 | 1 | 1,723 | 43 | `/tmp/cellhive-local-rps.07cqgQ/` |
| 完全不挂原生 Tail | 0 | 1,721 | 99 | `/tmp/cellhive-local-rps.DexqA1/` |
| 转换为结构化事件，但不 POST | 1 | 1,612 | 104 | `/tmp/cellhive-local-rps.nV5NAH/` |
| 当前有界合批，仍发送无 console 调用事件 | 1 | 1,441 | 230 | `/tmp/cellhive-local-rps.edeZ3s/` |
| 临时把合批投递交给 Tail 的 `ctx.waitUntil` | 1 | 1,468 | 160 | `/tmp/cellhive-local-rps.FW9Okm/` |
| 临时过滤无 console/异常的 Tail 事件 | 1 | 1,649 | 93 | `/tmp/cellhive-local-rps.IZArsT/` |
| 256 条或 3 秒合批，Tail `waitUntil` 异步投递 | 1 | 1,622 | 308 | `/tmp/cellhive-local-rps.JgPCOy/` |
| 本地 ring，热路径仍转换/序列化 | 1 | 1,653 | 284 | `/tmp/cellhive-local-rps.Jsjz2i/` |
| 本地 ring，查询时才转换事件 | 1 | 1,715 | 314 | `/tmp/cellhive-local-rps.nVYrhF/` |

这些是共享宿主上的顺序诊断，timeout 和其他任务会影响绝对 RPS，不能用于容量承诺。方向很明确：原生 Tail 挂载本身与完全关闭接近；对每次**无 console** 的请求做事件转换、内部 HTTP 投递、cell-agent 入站处理和 ring 写入才是主要额外成本。cell-agent 的 1000 条 ring 是保留上限，不是入站投递前的批量缓冲。有界合批减轻 HTTP 成本，但仍处理每个无日志调用，因此达不到跳过该链路的吞吐。让 Tail 异步等待投递也没有显著消除这份 CPU/HTTP 工作。

256 条或 3 秒版本已用固定 workerd 运行原生 Tail 定向测试，64 个并发调用的事件均保留，且没有跨请求 Promise 警告；单 runtime 压测读回 `CH_LOG_CAPTURE=1`，完成后 agent/runtime 就绪。实测 1,622 RPS、308 个 timeout，尚不能证明稳定提升。低流量日志会等待最多 3 秒；`waitUntil` 只让内部 HTTP 不挡住 Tail 返回，事件转换和序列化仍占用 workerd JS 线程。

随后将投递路径改为 workerd 本地有界 ring，cell-agent 在 Tail 活跃时才按游标拉取。第一版仍在每次调用转换并序列化，吞吐约 1,653 RPS；再把完整转换延后至查询时，约 1,715 RPS，接近原生 Tail 空 handler 的 1,723 RPS。两轮仍有 284/314 个 timeout，因此只是同机定位，不能用作无错误容量值。工作树内代码为查询时转换版，已用固定 workerd 验证普通 Worker、Service Binding 目标和 DO 的本地 Tail 查询。

最后在静默调用时加入保守的 URL/header 长度估算后，宿主负载已上升（`load average` 约 12.6），该版单轮为 970 RPS；紧接着同环境空 Tail 对照为 1,024 RPS。两轮均明显低于此前的 1.7k 区间，说明宿主争用显著，不能把这轮下降归因于长度估算，也不能宣称固定的性能提升比例。路径 `/tmp/cellhive-local-rps.u9JYFb/` 与 `/tmp/cellhive-local-rps.si6mlN/`。

更早的双 runtime 报告把残留的 `CELLHIVE_TENANT_LOG_CAPTURE=off` 当成 Tail 开启，因此“合批使合计 RPS 从 1.73k 增至 3.37k”的结论**无效**。本文件以上表的生成配置和单 runtime 结果为准。临时诊断脚本为 `/tmp/cellhive-tail-isolate-bench.sh`；仓库内可复用双 agent/双 runtime 完整压测脚本仍是 `scripts/bench-runtime-dual-local.sh`。

## 公共 fetch 响应流单层包装（2026-10-20）

对同一单 runtime、双 agent、直连 workerd `/ping`，临时脚本仅在复制的 loader 中恢复旧的入口侧 `Lifetime.response`，以当前工作树为对照；固定 workerd `2026-09-16`，`wrk -t4 -c64 -d20s`。旧版同时在动态 Worker 与入口包裹响应流。新版保留动态 Worker 的 EOF/cancel/error 追踪，入口在 fetch RPC 完成后释放引用，退休前仍查询动态 Worker 的 idle 状态。

| Tail | 双层包装 RPS / timeout | 单层包装 RPS / timeout | 原始输出 |
|---|---:|---:|---|
| 关闭 | 1638.64 / 2 | 2234.42 / 0 | `/tmp/cellhive-local-rps.y21yog/` / `/tmp/cellhive-local-rps.wYOQ8q/` |
| 当前本地 ring | 1483.97 / 65 | 1807.71 / 0 | `/tmp/cellhive-local-rps.SHnomG/` / `/tmp/cellhive-local-rps.kcqgH1/` |

这是共享宿主上的顺序诊断，不能用百分比作稳定容量承诺。真实 workerd 回归覆盖旧版本流发送期间切到新版本且旧流完整返回、公开 WebSocket 和原生 Tail；`make js-test`、`make test` 通过。取消路径另由 `workerd/platform/lifecycle.test.mjs` 验证 `Lifetime.response` 释放引用。

继续拆动态 Worker 的剩余开销：单层包装、Tail 关闭时，在隔离的复制文件里跳过请求头/trace 准备得到 1730.97 RPS，相邻正常对照 2183.34 RPS，因此无收益；跳过子 Worker 的 `ctx.waitUntil` 追踪得到 2375.68 RPS，但会失去后台任务的退休保护，未采用；跳过子 Worker 唯一剩余的响应包装得到 3218.58 RPS，但会让旧版本的未完成响应失去生命周期保护，未采用。正常对照另一轮为 2282.36 RPS。主机负载变化明显，不能把单轮差值当作精确组件成本。

安全的第二个小优化是：常态无待退休版本时不 `await sweepResidents()`，仍逐一检查 resident 的退休标志，有候选时照旧先清理。临时变体为 2320.40 RPS，相邻正常对照为 2282.36 RPS；正式改动后为 2349.79 RPS。增益接近共享宿主噪声，不能宣称稳定提升。固定 workerd 的流式版本切换、公开 WebSocket 和完整 `make js-test` 均通过。原始输出：`/tmp/cellhive-local-rps.YEOe0P/`、`RME6NX/`、`Odx7Oz/`、`HEebW1/`、`cggCZ4/`、`ZMMZfF/`、`LFePBt/`（共同前缀 `/tmp/cellhive-local-rps.`）。
