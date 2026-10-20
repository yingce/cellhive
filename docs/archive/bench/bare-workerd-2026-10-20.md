# Stock workerd 直连基准（2026-10-20）

目的：把 stock workerd 自身处理简单 Worker 的吞吐与完整 CellHive 入口路径分开测。使用 `/tmp/cellhive-local-workerd`（`workerd 2026-09-16`），只启动原生 workerd 进程；入口为 workerd HTTP socket。压测器是 C 版 `wrk debian/4.1.0-4build2`，没有 Go 代理、Go user-runtime 或 cell-agent。脚本为 `scripts/bench-workerd-bare.sh`，原始输出保留在 `/tmp/cellhive-bare-workerd.JoP6sJ/`。

Worker 的 `/ping` 只返回 `pong`；`/fast` 每请求分配并填充 64 KiB 后返回短响应，与双 runtime 测试路径相同。每项 30 秒，总连接数 64；单实例为 4 个 wrk 线程 × 64 连接，双实例为两组各 2 个 wrk 线程 × 32 连接，分别直连两个 workerd socket，同时运行后相加。预检四个端点均返回 200，wrk 输出中没有 socket error 或 non-2xx 计数。

| 路径 | 单 workerd RPS | 双 workerd A/B RPS | 双实例合计 RPS | 单实例 p50 / p99 |
|---|---:|---:|---:|---:|
| `/ping` | 13993.77 | 13147.69 / 13492.54 | 26640.23 | 1.21 / 339.80 ms |
| `/fast` | 11392.98 | 8925.00 / 8992.72 | 17917.72 | 1.48 / 435.04 ms |

单实例期间 workerd 约 101%–104% 单核 CPU；双实例时各约 101%–104%。双实例 `/fast` 没有接近单实例的两倍，且尾延迟有数百毫秒尖峰，需独立于当前共享宿主再次测 CPU/调度。此前完整 CellHive 双 agent / 双 runtime 路径用 Go 压测器测得 `/ping` 1417.3、`/fast` 1301.3 RPS；与本次 `wrk` 测试的客户端和入口结构不同，不能把两组数值的比率当成精确组件开销。可以确定的是裸 workerd 的处理能力远高于完整路径的约 1k RPS，瓶颈在 CellHive 入口/loader/路由/绑定等额外路径或其与 workerd 的交互，需要再用相同压测器和分段 profile 定位。
