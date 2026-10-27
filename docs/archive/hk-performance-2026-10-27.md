# 本机计算 + HK COS 性能验收（2026-10-27）

最新本地工作区（HEAD 1e2cb4a，包含未提交的 Wrangler staged versions、Queue/Cron 与 scope 鉴权重构）；make build 成功。单个 cell-agent、单个 user-runtime + pinned fork workerd，专用桶 vwork-e2e-hk-1376795072，durability=bucket，不等待 follower，也没有 owner 转发。8 vCPU / 约 30.5 GiB 的共享开发机，未限制为 2c，不可外推为 2c 或生产容量。

## 方法与结果

真实 HTTP keep-alive，1 KiB 值，32 个 key、单 KV cell，闭环并发 1/8/32，各两轮。直连 agent 和经 Worker binding 分段顺序运行，并非同时 A/B。每次 GET 校验完整内容；PUT 校验 HTTP 成功。长样本 GET 共 100,000 次，PUT 热请求 2,640 次，另有 34 次首访/预热，总计 102,674 次，失败 0。固定值覆盖用于延迟测试，不是故障注入/RPO=0 恢复验证。

下表为两轮区间，分位数没有跨轮平均。串行写每轮只有 60 次，p99 等于样本最大值；GET 每轮 5,000/10,000/10,000 次。并发写每轮 60/200/400 次。

| 路径 | 操作 | 并发 | p50 ms | p99 ms | 成功 RPS |
|---|---|---:|---:|---:|---:|
| direct | PUT | 1 | 43.25–44.27 | 88.77–509.69 | 18.40–20.66 |
| direct | GET | 1 | 0.31–0.36 | 0.77–0.93 | 2531.94–2957.95 |
| worker | PUT | 1 | 45.30–46.01 | 97.09–281.23 | 18.09–19.45 |
| worker | GET | 1 | 1.24–1.26 | 2.54–3.27 | 717.58–723.84 |
| direct | PUT | 8 | 44.30–44.73 | 89.38–138.20 | 159.12–171.31 |
| direct | GET | 8 | 0.82–0.92 | 2.89–3.15 | 7398.88–8267.34 |
| worker | PUT | 8 | 83.12–86.50 | 191.65–243.29 | 79.64–82.00 |
| worker | GET | 8 | 7.62–7.63 | 26.76–29.80 | 849.02–891.49 |
| direct | PUT | 32 | 86.42–91.86 | 132.75–585.32 | 190.61–341.53 |
| direct | GET | 32 | 3.12–4.21 | 6.93–14.51 | 6562.49–9700.63 |
| worker | PUT | 32 | 87.50–89.42 | 106.81–262.71 | 291.87–350.57 |
| worker | GET | 32 | 23.89–29.53 | 80.69–109.67 | 982.13–1248.93 |

首次新 cell PUT：pilot 901.5ms，长样本 1491.6ms；它们是独立新 namespace 的第一次测量，不是从已有业务快照恢复的基准。

## 分析

- 串行热 GET：直连 0.31–0.36ms，经 Worker 1.24–1.26ms。Worker 调用链存在额外开销。并发 32 下 Worker GET p99 为 80.7–109.7ms、982–1249 RPS；直连为 6.9–14.5ms、6562–9701 RPS。后续应独立剖析 runtime/binding/IPC 与排队，当前测试不确定具体函数瓶颈。
- 串行热 PUT：直连 p50 43–44ms，经 Worker 45–46ms。本测试强制 bucket 确认；不代表双 agent peer-fsync 性能。并发提高吞吐但不保证尾延迟。
- pilot 出现 1900.9ms 的 PUT；同一测试 cell 的提交日志 total_ms=1894.914、bucket_ms=1894.906、owner_ms=0.005。确认该次慢提交几乎全在 bucket durability 等待，不能进一步仅凭此日志区分 COS 服务端、网络和本地上传调度。长样本另有 528.7ms bucket 等待。证据见 slow-commits.txt。
- 共享开发机存在其他负载，两轮吞吐有波动；这不是隔离机器上的最大容量测试。

## Wrangler + HK 控制面

真实 Wrangler 4.133.0，三次 upload/deploy：upload 1121–1274ms，deploy 1045–1351ms。包含 Node/CLI 启动、打包和网络请求，不能当作单次服务端 HTTP 延迟。每次 upload 后核验 active 未变，每次 deploy 后核验 active 等于上传版本；全部通过。pilot 另有三组也通过。

## 日志与清理

长样本 agent/runtime 日志无 WARN/ERROR；测试应用 DELETE 返回 200，进程停止。应用删除是既有软删除/后台清理语义，不声称桶对象已全部物理清除。前两次启动尝试分别因测试 /ready 路径冲突、固定域名归属冲突退出，均在压测前失败且执行应用清理；脚本已改用独立路径和每轮唯一域名。测试没有部署或重启生产服务。

原始数据见同名目录的 JSONL、run.txt、slow-commits.txt。完整临时日志：/opt/.hermes/cache/scratch/cellhive-e2e.WB90o6；pilot：/opt/.hermes/cache/scratch/cellhive-e2e.xF2cpi。

复现：make build；设置 WRANGLER_CLI_PATH 为本机 Wrangler 4.133.0 cli.js，然后运行 python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance --port-base 28200。不要与其他 HK E2E 并行，共享控制 cell。
