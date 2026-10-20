# KV 资源栅栏热点复测（2026-10-20）

## 背景与原因

2026-09-16 的 `kvbench` 近万 RPS 是直连 cell-agent/本地文件桶的结果；当时还没有 2026-10-15 加入的 KV/Queue 删除栅栏准入。旧版 `scopeAuth` 对一个 KV 请求依次调用 `ResourceState`、`ResourceGeneration`、`EnsureResourceGeneration` 的权威回调，以及 `BeginResourceRequest` 的两次权威回调。五次 `resource-fence.json` 点查中，后三次在同一 cell 的世代锁内运行；若从非 owner 转发到 owner，目标再执行准入。真实 COS 往返会按并发请求串行放大。

ADR-235 将常态请求收敛为两次权威点查：准备时读取完整 fence 快照；本地 marker 已相同则不重查，若需改变本地代际则在锁内重验；取得请求 gate 后再检查一次，删除或世代变化则释放 gate 并拒绝。常态对象存储 I/O 不持世代锁，仍不使用 TTL 缓存。捕获写入的 RPO=0 持久证明和副本写入栅栏不变。

## 同机 A/B（USW 独立 E2E COS）

- 同一主机、同一个独立 `cellhive-e2e-usw-1376795072` 桶；每轮前清空桶，轮后清空并确认 0 对象。旧版为镜像 `70c945f`，新版为本次未发布的源码；workerd 均为镜像内的 `2026-09-16`。两个 user-runtime、一个 cell-agent；Go HTTP 代理轮询两个 runtime。相同 Worker：`/fast` 分配/填充 64 KiB，`/read` 轮换两个 KV binding 读 `hot`，`/write` 轮换两个 KV binding 写 64 个循环 key。各 runtime 路径预热到 200 后计时，所有统计响应均为 200。

| 路径 | 并发/时长 | 旧版 RPS | 新版 RPS | 新版成功/失败 |
|---|---:|---:|---:|---:|
| `/fast` | 64 / 20s | 1377.6 | 1400.1 | 29403 / 0 |
| `/read` | 64 / 30s | 3.7 | 76.7 | 2378 / 0 |
| `/write` | 16 / 30s | 3.2 | 5.6 | 177 / 0 |

新版另做 2 cell-agent + 2 user-runtime、同一桶的 25s 测试：`/read` 64 并发 74.2 RPS（2040/0），`/write` 16 并发 13.0 RPS（343/0）。第二个 agent 的 peer spool 有 259033 字节，桶中出现一个 `node-logs/` 对象，证明 fleet 路径参与。两轮均在本机跨地域访问 USW COS；这些绝对值不是 USW TKE 同地域容量，也未测生产桶。

读吞吐的同机 A/B 提升约 20.7 倍，支持“锁内重复 COS 点查”为主要原因。写入还受 COS 往返、RPO=0 持久证明及有无合格 peer 影响；两 agent 相比一 agent 的写吞吐约 2.3 倍。区域内新镜像尚未发布/复测，不宣称达到历史直连 `kvbench` 的万级 RPS。

## 纯本机文件桶复测

修复后的本机二进制、stock workerd `2026-09-16`、独立临时文件桶、1 个 cell-agent、2 个 user-runtime、轮询 Go HTTP 代理；Worker 与上表相同。预热确认代理及两个 runtime 的 `/fast`、`/read`、`/write` 均返回 200。结果：

| 路径 | 并发/时长 | 本轮 RPS | 成功/失败 | p50 / p99 |
|---|---:|---:|---:|---:|
| `/fast` | 64 / 60s | 983.9 | 60023 / 0 | 43.04 / 386.81 ms |
| `/read` | 64 / 30s | 892.9 | 27679 / 0 | 55.97 / 247.75 ms |
| `/write` | 16 / 30s | 761.1 | 23596 / 0 | 17.79 / 101.15 ms |

结束时 agent、两个 runtime 的健康检查均通过，进程随后正常清理；日志中 runtime 的 `signal: killed` 是脚本退出清理产生的。此前同拓扑文件桶记录依次为 1366.4、1169.8、965.0 RPS，本轮反而低约 21%–28%，不能据此声称本地吞吐提升。本机复测后宿主 load average 为 12.73/9.47/7.20，swap 约 1.9 GiB 已满，测试未隔离 CPU/内存争用；需在受控负载环境做配对 A/B 才能归因。该修复针对远端对象存储的重复往返与锁内串行，文件桶下收益预期较小。

## 双 agent 本机标准拓扑

双 agent 不能只检查进程数。首次测试中 A、B 两个 KV 数据库都落在 agent1，agent2 仅接收转发，读 894.5 RPS、写 401.2 RPS，不能作为双节点分摊结果。修正后分别经 agent1、agent2 首次写入 A、B，确认 `agent1/.../A.db`、`agent2/.../B.db`，两个 runtime 分别连接各自 agent，代理轮询 runtime。相同 Worker 与本地文件桶，三项均 0 失败：

| 路径 | 并发/时长 | RPS | 成功/失败 | p50 / p99 |
|---|---:|---:|---:|---:|
| `/fast` | 64 / 30s | 876.9 | 27184 / 0 | 37.91 / 339.22 ms |
| `/read` | 64 / 30s | 752.7 | 23334 / 0 | 80.37 / 272.09 ms |
| `/write` | 16 / 30s | 537.5 | 16663 / 0 | 23.84 / 127.16 ms |

两个 agent 和两个 runtime 在压测后均健康；两侧 peer spool、各自 KV 数据库均存在。主机同时有其他 Node/Rust 编译任务，测试结束后 load average 为 18.47/15.43/10.30，swap 约 1.9 GiB 已满；结果只代表这台共享主机当时的端到端吞吐。`/fast` 不访问 KV，仍低于历史专用测试结果，说明当前端到端上限不能直接归咎于 KV 或 cell-agent。分布式写还有 RPO=0 的副本与对象存储工作；这组数据不足以量化每段开销。后续使用 `scripts/bench-runtime-dual-local.sh` 作为双 agent、双 runtime、分别持有 A/B cell 的本机标准，配对 A/B 须在相同宿主负载下重跑。

标准脚本首轮验收（同拓扑，临时 bundle 的 `/fast` 响应未显式设置 `content-type`；现脚本已恢复原 Worker 的该头）：workerd `2026-09-16`，A/B 分属 agent1/agent2，`/fast` 1308.8 RPS（39361/0）、`/read` 933.9 RPS（28143/0）、`/write` 720.7 RPS（21633/0）；压测后四个服务健康，agent1/agent2 peer spool 分别为 3399104/3501569 字节。开跑前主机 load average 为 7.68/12.84/9.87；与上一轮宿主负载不同，吞吐明显波动，因此这些轮次不能作为代码改动的可靠 A/B。

恢复 `/fast` 响应头后的另一轮完整路径（诊断脚本先运行相同的 30 秒三项，再运行额外直连探针）为 `/fast` 1201.4、`/read` 553.7、`/write` 315.1 RPS，均 0 失败。这一轮读写明显下滑，进一步说明共享宿主上的单次 RPS 不稳定；额外直连探针曾因测试键未预置而失败，已从可复用标准脚本移除，不计入三项端到端结果。

同拓扑诊断时，用临时编译的 `kvbench` 把 scope 名称从默认 `KV` 改为已部署的 `A`，预先写入 `k0`，以 `-keys 1` 直接请求 agent1 的 A cell。64 并发读 10 秒为 **11062 RPS**（110732/0，p50 4.33 ms），16 并发写 10 秒为 **2112 RPS**（21140/0，p50 6.73 ms）。这一诊断跳过了代理、user-runtime/workerd 与第二个 KV cell，只能定位数量级：cell-agent 的本地 KV 接口远高于当前完整 Worker 路径。相邻的 1 秒完整路径探测为 `/fast` 805.8、`/read` 477.7、`/write` 380.1 RPS，宿主负载波动很大，不可用其比值推算单个组件开销。完整路径还包含租户路由、workerd 调度、binding 请求与潜在跨 agent 转发；需要分段 profile 才能精确定位其上限。

纯无 KV 的双 agent / 双 runtime 新测：脚本选择 `CELLHIVE_BENCH_SPECS=$'ping 64 30\nfast 64 30'`，仍声明 A/B bindings 但两个路径都不调用绑定。`/ping` 只返回 `pong`，**1417.3 RPS**（42576/0，p50 29.45 ms、p99 254.18 ms）；`/fast` 每请求分配并填充 64 KiB，**1301.3 RPS**（39141/0，p50 31.67 ms、p99 262.00 ms）。开跑前 load average 2.93/4.75/7.46；压测后四个服务健康。`/ping` 压测期间 `pidstat` 三个 2 秒样本平均显示两个 workerd 进程各约 94.7%/95.5% 单核 CPU，代理 65.7%，压测器 29.2%，两个 cell-agent 各约 32.7%/31.3%，Go user-runtime supervisor 近 0%。这证明在该拓扑下纯 Worker 请求已使两个 workerd 接近各自单核饱和；不能据此判断 CPU 花在路由、加载、trace 还是 Worker 调度的哪段代码。

预热判别：同一进程与版本、预检已让两个 runtime 的 `/ping`、`/fast`、`/read`、`/write` 各返回 200 后，连续跑两轮 30 秒 `/ping`（64 并发），依次为 **1290.5 RPS**（38801/0）与 **1392.5 RPS**（41892/0），第二轮高 7.9%。这一差别小于裸 workerd 与完整路径的数量级差距，也可能受共享主机负载影响；它不支持“未预热造成持续约 1k RPS”的解释。源码中 host/worker env/prepared plan/bundle 及 resident Worker 均有缓存；首轮预检后已经命中过 Worker。周期性 host TTL 约 10 秒并采用 stale-while-revalidate，不会每请求同步重新加载 bundle。

Go user-runtime supervisor 判别：同一双 agent、双 workerd，使用 C 版 `wrk` 直接访问两个 workerd public socket（各 2 线程、32 连接、20 秒，同时执行），没有前置 Go 轮询代理。Go supervisor 正常运行时，两个实例 `/ping` 分别为 **903.58 / 897.71 RPS**（合计 1801.29）；对两个 Go supervisor 发送 SIGSTOP、使其完全停止执行后，workerd 继续服务，分别为 **899.28 / 895.79 RPS**（合计 1795.07）。暂停期间 `wrk` 报 3 个 timeout，其他连接错误为 0；四个服务恢复后健康，进程已清理。原始输出在 `/tmp/cellhive-local-rps.4jsY4b/`。这验证默认单 workerd 模式下 Go supervisor 不处理公网页面每个请求；Go 只在启动时渲染 config/注入角色凭据、启动并监护 workerd、退出时请求 drain。与纯裸 workerd 的 26.6k RPS 对比仍相差数量级，但两边的 Worker 结构不同；差距在 CellHive workerd loader/managed fetch/控制交互等路径，需分段 profile 定位。可选 adaptive pool 模式则确实由 Go HTTP proxy 接管 public/internal 端口，不能套用此结论。

## MinIO 边界

本地固定镜像 `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` 通过条件写，但对旧 ETag 的 `DeleteObject If-Match` 返回成功；两个 cell-agent 均在 `bucket.Diagnose` 处拒绝启动。此前的 MinIO 基准早于完整条件删除契约，或只运行了不含此断言的 `s3probe`/复制测试；`docs/testing.md` 已记录固定 MinIO 的完整 S3 集成失败。不可跳过诊断把该版本 MinIO 当成双 agent 权威桶。
