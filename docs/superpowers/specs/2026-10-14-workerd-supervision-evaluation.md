# workerd 进程监督与代理评估设计

状态：实现阶段的核心模型已开始；当前仍不改变生产入口、调度、owner 或 workerd。用户要求先设计、实验、报告，再决定生产接入。

## 问题与目标

当前共享 standalone workerd 无有效 invocation CPU/heap enforcement，同步死循环会阻塞其执行线程。旧版本退休、缓存预算不解决死循环。评估 Go 进程外监控/watchdog 的恢复能力，以及 Go 反代带来的性能成本；不引入额外代理产品、不 fork workerd。

## 候选架构

1. 仅监督：保持 Traefik→workerd，Go 启停并监控子进程，无请求代理。
2. 调度入口：Traefik→Go→有限 workerd 槽；Go 解析控制面 Host/path 路由并固定不可变版本。真实实现应避免 JS 再解析一次 active version。
3. 热点专用槽 + 冷代码共享槽；已部署 function 数与驻留进程数解耦。进程数按 Pod 内存/CPU与排队延迟决定，不预设固定生产容量。

### 自动热点迁移

所有 function/version 初始进入共享槽，不要求运维预先列热点。Go supervisor 在内存中维护短窗口计数和 EWMA：请求率、inflight、pending、queue wait、冷加载次数、错误/超时；Prometheus只做观测和HPA输入，不进入每请求决策。

状态机：

```text
shared → promotion_pending → dedicated
dedicated → demotion_pending → shared
任意状态 → draining/unhealthy → stopped → starting
```

promotion 需要连续多个采样周期满足压力条件并且有预算；dedicated slot 固定 `namespace/function@version`，新请求切换前先健康检查。共享槽中的旧请求自然完成，不能强行迁移；迁移期间允许短暂双驻留。demotion 只在低流量冷却窗口后进行，且没有 inflight、WS、stream 或 waitUntil pin。

对外只保留 `memoryUtilization` 和 `maxSlots` 两个主要参数；promotion/demotion 窗口、冷却、RSS安全倍率、保留槽、最大排队等为控制器内置策略。专用槽最多占 `maxSlots` 的内部比例，避免热点提升耗尽共享容量。

热点槽不是 DO owner。DO 继续由现有 do-runtime 的 owner/epoch/output-gate 机制处理；Queue/Cron/Workflow 用内部请求携带的不可变 worker/version 进入同一调度器。

Go 维护 slot ID/generation/PID/backend/function-version/inflight/state，不持久化本地槽映射为权威。跨 Pod由 Service 提供入口，不承诺重建后原地续跑请求。Queue/Cron继续使用带身份的内部 dispatch，调度留在 cell-agent。DO继续独立 gated runtime，避免将普通 worker kill 策略套用到状态 actor。

## 监督策略与约束

- `/proc/PID/stat` CPU累计时间、`status` RSS、FD数和cgroup内存/CPU throttling；PID+generation 防止误杀复用 PID。进程 CPU不是并发 invocation CPU归因。
- watchdog必须在 workerd 之外运行；deadline 为 wall-clock，不是 cpu_ms。SIGTERM有超时，必要时SIGKILL并Wait收尸，旧generation失效后新槽才就绪。
- 重启前停止派发；正常请求drain，卡死时强制终止。不得自动重试已派发业务写请求；响应丢失标为失败/结果未知。WS断开，客户端重连。
- 同容器多个进程共享内存限额；RSS软准入不能保证挡住瞬时分配。独立子cgroup需要权限/委派验证，不能假定普通TKE Pod支持。
- liveness TCP可在应用卡死时误报健康，必须实验HTTP探针。CPU throttling不终止死循环。

## 代理契约

Go标准库 ReverseProxy，复用连接，流式转发，无整body缓存。验证取消传播、响应首字节、WS upgrade、在途记录直到body/连接结束；内部请求身份不得来自公网未校验header。可靠完成计数、waitUntil、native RPC capability不能只靠HTTP代理推断。

## 实验矩阵

- 同一二进制、租户handler及负载：直接与Go代理交替多轮，记录成功率、RPS、p50/p99与CPU/RSS。
- 原生静态worker作proxy开销下界，不能替代生产dynamic loader/framework端到端评估。
- 1 MiB echo及延迟stream，验证首块先于最终完成，避免buffering。
- 进程A死循环，HTTP健康超时；Go watchdog终止A，进程B仍服务；重建A验证就绪。无业务写入、不混入故障后的盲目重试。
- 监控能观察CPU增长及RSS；真实强制退出测量termination latency。
- 自动热点实验：共享槽中的一个 function 在压力窗口内升温，promotion 到专用槽；压力消失后冷却 demotion；检查 slot/generation、双驻留、忙请求完成和槽重建。
- 后续必须增加真实framework/dynamic loader、WS/native RPC/取消、Queue任务结果未知、DO gate/接管、多Pod云环境验证。

## 决策门槛

评估报告列出原始数据、工具版本、重复实验范围、缺口，不用一次PASS替代长期容量保证。只有机制正确且proxy成本/维护复杂度可接受，才写生产ADR和实施计划；若引入池，初期灰度普通HTTP function，限制专用槽数量，DO及native RPC逐项验收。

## 当前实现进度

`internal/workerdpool` 已有不依赖 workerd 的控制模型：系数目标槽数、原子 slot admission、热点滞回、dedicated/shared selection、slot generation、stale exit 防护、抽象 Launcher/Supervisor、cgroup v2 memory/cpu 读取、外部 watchdog、socket-fd workerd launcher、流式 HTTP ReverseProxy 和单测。它尚未接入 `cmd/user-runtime`、控制面 Host/path resolver、内部 Queue/Cron/Workflow/Service dispatch、生产 Kubernetes；这些必须通过后续真实 workerd E2E 后才能进入生产入口。
