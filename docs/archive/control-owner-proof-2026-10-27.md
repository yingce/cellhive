# Control 本地读证明与退出续租修复（2026-10-27）

## 变更

对应 ADR-303，未提交/部署。

- `owner.LocalReadGuard` 仅接受权威查询产生的同 node/session、node-lease 本地 route；读前验证本地租约健康与缓存证明截止时间，读后检查原截止时间、同一 owner/revision 与 lease，失效则丢弃 SQLite 结果。
- `ReadFence` 检测读期间的本地所有权变更，本身不构成授权。Release 开始/结束均失效 route/revision，并以计数标记释放中；即使期间重新读到旧 owner，也不能取得本地控制读许可。CAS 失败同样清理证明。
- `localBinding` 在安全证明存在时读取控制 SQLite，不再每次刷新授权都 `Owner.Resolve`；冷缓存/legacy/不满足证明仍走权威点查，epoch/session/撤销/fence/授权TTL不放宽。
- Renew/Release 同时核对 Session，防止同名旧进程以相同 epoch 续期或删除新进程 owner。
- 合成 control binding 请求注入 trace context，修复前次跨节点无法严格关联该请求的问题。
- SIGTERM 不再取消 node lease 发布；独立续期上下文贯穿 HTTP 关闭、capture drain、上传 flush、session seal 与 owner release，在 main 退出末尾取消。watchdog 始终保留，真实 lease 失效仍终止进程。

## 本地验证

owner/server/cell-agent 全量测试通过；相关 race 三次通过，vet、diff check通过。新增测试覆盖：10次授权刷新只做1次初始owner GET；显式失效后恢复查询；读取与Release并发；释放中再次读到旧record不得授权；CAS失败不能保留证明；lease过期/session变化使旧读结果失效；旧session不能Renew/Release；模拟drain超过两个完整lease周期仍正常续期且watchdog不误杀。原有过期watchdog与control handoff测试继续通过。

## HK 测试准备中的恢复

上轮旧二进制曾被watchdog抢断handoff，留下session1793037187265115524需访问旧follower 127.0.0.1:28904。此次初始启动恢复失败，未计入正式压测。使用保留目录cellhive-e2e.KGVMNG/peer和新二进制启动旧follower，control/apps恢复HTTP200；随后SIGTERM旧follower，确认handoff complete和stopped。没有删除/跳过持久化数据。后续检查发现 HTTP200 不代表整份 session 已封存，peer 后续仍出现 tail after 768 连接失败。因此重新启动保留原盘/instance 的 follower，并在 COS 最新 CHL2 记录中校验 CRC 与状态：session 1793037187265115524 已 sealed，recovered_seq/covered_seq 均为 856。随后 SIGTERM，02:18:34 确认 handoff complete（released=18）及 stopped。没有删除或跳过恢复数据。

完整测试日志 `/opt/.hermes/cache/scratch/hk-review-1793038045-664a9d-5u11lbtq/0/result.log`；数据 `/opt/.hermes/cache/scratch/cellhive-e2e.zcavaY`；trace `/tmp/cellhive-owner-proof-spans.jsonl`。性能与最终退出结果完成后追加。

## 首轮环境阻塞与补充修复

首轮未进入正式压测：90 次探测未取得 fleet ack。节点发现串行读取历史 node 耗尽 15 秒预算后返回部分结果，排在末尾的新节点被长期遗漏。Sample 改为最多 8 路并发，超时拒绝部分列表。80 节点延时模拟及取消测试通过，lease 全包 race 通过；lease/owner/server/cell-agent 全包测试与 vet 通过。

重跑 artifacts：`/opt/.hermes/cache/scratch/hk-review-1793038713-e551d1-qbeemfik`。

## 最终真实双节点验收

本地计算 + HK COS，1KiB/32 keys，并发 1/8/32，两轮，owner/direct、follower/forwarded、Worker 各 PUT/GET。153960 次正式请求全部成功；正式批次合计 fleet 834、bucket-batch 0（组提交次数，不是请求数）。准备阶段冷写/发现仍可能回退桶，不计入正式性能。结果 `HK-REVIEW: PASS`；app 删除 HTTP200；perf/peer 分别 released=18/1，均 handoff complete + stopped，无 watchdog fencing fatal。Wrangler 性能子项因未设 WRANGLER_CLI_PATH 跳过，不能算作本次覆盖。

| 串行路径 | PUT p50 / p99 ms（两轮范围） | GET p50 / p99 ms（两轮范围） |
|---|---|---|
| owner | 6.22–6.52 / 8.39–9.54 | 0.33–0.39 / 0.77–0.91 |
| follower转发 | 6.54–6.59 / 81.97–142.39 | 0.59–0.61 / 1.35–1.72 |
| Worker | 8.31–8.55 / 10.17–11.29 | 1.73–2.02 / 2.94–6.98 |

并发32：owner PUT 2026–2383 RPS、GET 3807–6799 RPS；Worker PUT 562–855、GET 756–855 RPS。全量 trace 开启，本机共享 CPU；第一轮部分时间与短 race 回归重叠，不能当作独占机器容量数据。完整分批指标见 `control-owner-proof-2026-10-27/batches.json`。

版本授权刷新290次：p50=0.186ms、p99=2.146ms、max=2.828ms；此前样本439ms。对应本次优化生效，不表示所有端到端长尾都消失。普通 binding 刷新仍有 max=369ms，最慢热请求 forwarded PUT=510.386ms：等待同一刷新 singleflight，leader trace `c43baa6bc2d2f8dd5a2dff83d2fa2a5c` 的 owner_bucket_read=343.67ms，等待者 trace `891fce9a0d52afdc6b78c63e94a095b0` binding_refresh=368.77ms、fence read=27.48ms、owner HTTP=18.46ms，durability=16.73ms。这是远端 route 证明到期重查的剩余边界，并非此次本地权威证明重复查询，也不是同步COS持久化回退。高并发Worker GET最大168.493ms仍待单独分析，不声称消除。

相关包全量测试、vet、相关 race 三次、git diff --check 均通过。代码与记录未提交、未部署。
