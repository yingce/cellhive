# workerd 监督 / Go 反代实践评估

## 结论

**机制可行，但暂不建议把全部生产请求切到 Go proxy。** 独立 Go 进程能监控、终止卡死 workerd 并重建；独立 workerd B 不随 A 的死循环停止。标准库 ReverseProxy 保留本次 echo/stream 语义，但短响应负载下有显著吞吐与 CPU 成本。推荐先实施进程外监控/watchdog，代理/热点专用槽先做灰度原型，再补生产动态路径与长连接验收。

本报告不批准生产实现，不变更“stock workerd、不 fork”契约。用户要求先报告后决定实施。

## 方法与复现

- 设计：`docs/superpowers/specs/2026-10-14-workerd-supervision-evaluation.md`。
- 非生产入口：`workerd/spikes/process-supervision/main.go`，Go stdlib，启动两个 pinned workerd 子进程；仅测试自己创建的进程和随机 loopback socket。
- 使用 Go 先绑定监听 socket，再通过 ExtraFiles / `--socket-fd public=3` 传给子进程；无寻找空闲端口再启动的竞态。
- workerd 输出 `2026-09-16`，原生静态租户模块，无 production loader/框架/DO/日志绑定；测量的是代理机制成本，不是全平台成本。
- Go 同时承载负载生成与 ReverseProxy；16 并发、每路径2秒、交替3轮。短轮次且没有独立driver，结果可能夸大共享CPU下代理损耗；不能直接外推SLA。
- 首轮宿主机无CPU quota；第二轮 Docker 2 CPU/2 GiB、禁swap，测试容器 root仅用于写证据目录，不是生产权限方案。
- 自动保存 version、RSS/CPU tick/FD、请求RPS和p50/p99、stream、kill/rebuild到 results.json，子进程日志分文件；无租户业务写入。

```bash
PATH=/usr/local/go/bin:$PATH go run ./workerd/spikes/process-supervision \
  -workerd /opt/cellhive/cli/node_modules/@cloudflare/workerd-linux-64/bin/workerd \
  -out /tmp/opencode
```

原始证据：
- 宿主机：`/tmp/opencode/supervision-1652008112/`。
- 2C2G：`/tmp/opencode/supervision-3059755025/`。

## A/B 请求性能

每轮均零失败，租户响应为 `pong`。

| 环境 / 路径 | RPS 三轮 | RPS 中位 | p50 ms 三轮 | p99 ms 三轮 |
|---|---|---:|---|---|
| 宿主 direct | 14,173 / 13,754 / 14,553 | 14,173 | .836 / .789 / .834 | 8.350 / 6.915 / 5.343 |
| 宿主 proxy | 10,216 / 10,387 / 9,500 | 10,216 | 1.346 / 1.280 / 1.326 | 3.751 / 3.269 / 3.424 |
| 2C2G direct | 17,675 / 16,956 / 17,459 | 17,459 | .658 / .614 / .643 | 3.043 / 2.628 / 2.652 |
| 2C2G proxy | 11,578 / 11,250 / 11,748 | 11,578 | 1.002 / .972 / 1.031 | 18.426 / 18.419 / 16.711 |

宿主proxy中位吞吐降低约28%；2C2G约34%。p50增量约0.35–0.5ms，受限CPU环境p99明显变差。宿主p99反而更低不能解读为代理优化，只是负载/调度不同。driver/proxy共进程使Go CPU无法准确拆分：2C2G direct Go每轮增长约135–146 ticks，proxy约242–266 ticks，可见额外CPU工作；不将ticks转换毫秒，未假设内核USER_HZ。

## 进程监控与内存

- 两个空闲workerd启动约40–42 MiB RSS，各11个FD；Go约14–17 MiB（包含driver/proxy）。
- 高频pong测试后A的RSS增长到约429–430 MiB，不是“每个workerd固定40MiB”；本实验没有heap/native拆分和长空闲测量，不能判定泄漏。
- `/proc/PID/stat` 能看到死循环500ms期间CPU增长约50 ticks，RSS保持约430MiB，符合CPU忙而非持续分配。
- FD统计正常可读；不使用RSS作硬isolate配额。
- 同Pod Go/workerd都共享内存资源，高水位轮询不能替代cgroup硬边界。生产需预留Go/sidecar与burst余量，防止OOM影响监督者。

## 流式与body保真

- direct/proxy均1 MiB echo字节精确一致。
- delayed stream先返回 `first`，300ms后返回 `last`；宿主first约0–6ms、总301–306ms，2C2G first约0ms、总301ms。
- 未出现等待完整body后才返回首块的整包buffering。
- 不证明上传反压、客户端取消、WS upgrade、RPC capability/大body长期内存正确性；这些为下一阶段必测项。

## 死循环 / watchdog机制验证

1. A执行 `while(true)`；500ms后采样CPU。
2. A HTTP健康请求150ms超时，B仍返回200。
3. 外部Go执行Kill并Wait，宿主约41ms、2C2G约28ms退出回收。
4. 重建A，HTTP就绪宿主约21ms、2C2G约15ms。

这是受控固定等待/探针失败后强制回收的**机制测试**，不是生产watchdog实现。没有实现周期检测、SIGTERM→deadline→SIGKILL、generation fencing、误报退避、请求drain/结果未知等状态机。重建时间是最小静态模块本地冷启动，不包含真实bundle下载/恢复。

## 尚未覆盖与阻塞

- 真实生产dynamic loader及大框架（Hono/SSR等）端到端 A/B、独立负载发生器、多并发档位和10分钟以上稳态。
- 同进程其他isolate卡死影响、热function专用槽、排队准入、generation竞态、滚动drain。
- WS/RPC长连接、取消传播、waitUntil任务、HTTP结果未知/幂等策略。
- Queue/Cron内部派发保持version、身份与任务ack边界。
- DO输出门、SQLite捕获、租约接管；禁止把此HTTP kill实验当作DO持久性验收。
- TKE实际资源配额、跨节点故障、云对象存储：本次没有使用云集群或凭据，不宣称完成。

## 建议实施顺序

1. **进程外监控/watchdog优先**：不增加请求代理；只恢复整个共享runtime，明确故障范围。实现必要的状态机、超时、监控指标和重启退避。
2. **代理可选灰度**：先普通HTTP function，Go固定版本派发，避免JS二次路由；独立driver重新测成本，不能凭本表直接决定生产CPU预算。
3. **热点专用槽试点**：只有热点/高风险版本占独立进程；池有总容量上限，槽满受控拒绝或有界排队。共享槽仍共享故障范围。
4. **WS/native RPC/DO逐项验收**：未通过前不迁移生产长连接和DO执行层。

当前证据支持“Go监督可落地”，不支持“Go proxy损耗可以忽略”或“所有执行入口已经适合进程池”。等待用户审阅报告后再实施。

## Review follow-up

The first review identified child `/drain` path exposure, irreversible high-water slot estimation, missing shared-slot expansion, last-shared-slot eviction, generation-directory accumulation, and readiness not reflecting usable capacity. The current worktree now includes fixes for those items: pool admission/generation cleanup, loopback-child cleanup, shared-capacity expansion, adaptive readiness, pool-mode child drain rejection, and bounded generation cleanup. Remaining before production: child-level quiescence for `waitUntil`/WS, DO/native-RPC/dispatch quota coverage, long-body proxy stress, and real TKE validation.

The proxy now records active `text/event-stream` and HTTP Upgrade/WebSocket tunnel lifetime on the slot, with tests for both EOF/tunnel close. This is transport-level accounting; it does not introspect child `waitUntil` state. The 30-second drain grace remains the bounded fallback before forced termination.

The `ReverseProxy` object is now cached per backend URL/generation (bounded to 256), while the Transport/BufferPool were already shared. A repeat of the short synthetic benchmark did not show a reliable improvement: direct median RPS was 13,696/14,548/13,447 and proxy was 9,868/9,320/8,819. This confirms that object construction was not the dominant cost in this benchmark; independent-driver and real dynamic-loader A/B are still required before further proxy tuning.
