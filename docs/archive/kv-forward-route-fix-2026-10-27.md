# KV 转发路由长尾闭环（2026-10-27）

基线 `4c160ff`，隔离分支 `perf/data-path-optimization`。本报告补充上一轮仅解决入口 fence 的报告；不表示整个数据路径计划完成。未推送/部署。

## 根因及修复边界

1. 远端路由仍绑定首次查询时的节点租约截止时间。即使后台取得新租约，原路由过期仍重新读取 COS owner。新增 `ResolveForwardRoute`：只有已有远端 node-lease 提示与内存中有效的同 node/session 租约匹配时复用目的地。不能作为本机读、capture、接管证明，不续写权威缓存截止时间。过期/缺失/换 session/显式失效/自身 node 走原解析；最终接收者保持完整授权、资源和 owner 检查及现有一次重路由规则。
2. 第一版修复的真实 trace 又暴露：`NodeLease` 点查发现的节点没有纳入 known refresh，后台全量发现完成前仍重复 COS 查询。成功点查通过 observation 时间戳保护的合并注册已知节点，最多4096条；后台按原机制点查续刷。没有新增热路径 List，没有本地虚构续期。
3. Worker 采样路径在返回响应前等待 span 上报。成功与错误路径均改为 `ctx.waitUntil`，持久化 ACK 不变；阻塞遥测回归验证响应及 resident release 不等待上报。

## 回归与安全边界

旧实现 overlay 回归分别复现 unexpected bucket GET、point lookup not registered；旧 loader 的阻塞 export 测试失败，修复后10/10通过。新增测试覆盖过期、session改变、缺失、显式失效、自身节点，提示不能复活本机读权限；点查注册有界且旧响应不能覆盖新 session。最终 lease/owner/server 三轮 race 通过。

真实 HK COS routing-safety：双 agent HTTP GET/PUT/get-batch、错误 token/namespace 拒绝、删除拒绝、同名重建旧值不存在、新值跨节点可读，PASS，应用清理 HTTP200。

真实 HK COS batch-recovery：owner SIGKILL 后100/100已 ACK 值恢复；清除本次本地数据后 COS 冷恢复100/100，首 GET 1.584786s，PASS，应用清理 HTTP200。未放宽持久化确认条件；单轮强杀不等同整个计划中的10轮故障矩阵或断电验证。

## 全量 trace

本地计算、双 agent、HK COS、真实 HTTP，153960 正式请求，零失败；fleet822批，bucket-batch0。串行转发 PUT 第二轮 p99/max9.920ms；并发8/32的转发PUT p99分别为第一轮12.580/26.418ms、第二轮9.700/19.969ms。

首次访问 follower PUT 200.854ms，其中两次 owner read 38.391ms、149.066ms；全量 peer trace 按父子关系检查，共71次owner read，仅这两次属于前台HTTP，其余无HTTP祖先。后续热转发没有再次进入owner COS读取。入口HTTP资源准入子span为0；后台COS读取仍存在，不宣称零COS访问。

完整 trace 暂存 `/tmp/cellhive-route-final-trace.jsonl`（约544MiB），只归档摘要与首次慢请求关联span。Worker并发trace关联仍有既存缺口，不声称全部Worker链路已完整关联。

## 无 trace 对照

三轮、并发1/8/32，owner/转发/Worker分别测PUT与GET；1KiB值、32个key、HTTP keep-alive、完整响应体校验。`PERF_PRIME_ENTRIES=1` 单独记录首次进入follower及预热，不把该冷初始化算成稳定期。正式结果见归档统计。

首次对照未进入采样：90秒等待无fleet ACK；日志显示退出前控制cell已开始fleet提交。共享桶历史节点导致首次全量发现延迟，测试等待改为180秒，仍必须看到真实fleet ACK才开始。保留失败记录，不把COS回退数据计入双节点性能。继续下钻发现ProviderBucket.List在外层8并发之前串行解析所有authority记录。补充最多8并发解析，保留最新window、删除过滤、GC竞争回查、排序与失败不返回部分列表。对应有界并发/删除过滤/失败/取消测试。最终复测记录如下。

## 尚未覆盖

首次冷元数据仍需COS；网络故障、未发现/失去follower时仍按RPO=0回退。完整P0–P3、跨机网络收益、生产灰度、分支GitHub CI及全故障矩阵未在本轮完成，不以本轮性能结果替代这些验收。

## 最终版本检查

`make test`（pinned stock workerd）、`make vet`、`make build` 均退出0，loader10/10。lease/owner/server三轮race及bucket三轮race通过。HK使用平台fork，真实COS；不将原生云存储可选测试的本地skip视作通过，另跑COS契约。

串行authority解析版的三轮对照230940请求零失败，但启动fleet就绪约175.48秒；补充8并发解析后相同共享桶4次探测、约3.30秒到首个fleet ACK。两场非严格控制网络抖动的性能实验，启动数字是实测，不保证任意历史数据规模均为3秒。

最终三轮230940请求零失败；逐批commit计数见JSONL（GET批次可能含后台提交，不等于每请求一批）。

| 路径 | 并发 | 操作 | 三轮 p99 ms | 三轮 RPS |
|---|---:|---|---|---|
| direct | 1 | put | 9.028 / 8.26 / 66.01 | 143.82 / 164.05 / 112.67 |
| direct | 1 | get | 1.208 / 0.579 / 1.05 | 2161.02 / 3237.79 / 2624.93 |
| direct | 8 | put | 18.599 / 8.9 / 51.306 | 836.05 / 1162.11 / 826.26 |
| direct | 8 | get | 7.298 / 2.258 / 6.518 | 4157.93 / 8170.27 / 5109.9 |
| direct | 32 | put | 17.512 / 37.965 / 17.895 | 3069.41 / 1673.46 / 2816.82 |
| direct | 32 | get | 8.35 / 14.119 / 18.594 | 7857.55 / 6058.26 / 6405.26 |
| forwarded | 1 | put | 9.895 / 8.441 / 15.58 | 138.2 / 165.13 / 129.1 |
| forwarded | 1 | get | 1.566 / 1.224 / 3.143 | 1605.58 / 1831.54 / 1294.13 |
| forwarded | 8 | put | 30.927 / 12.032 / 18.088 | 422.75 / 1004.76 / 875.64 |
| forwarded | 8 | get | 8.832 / 3.364 / 3.726 | 3433.5 / 6315.18 / 5719.95 |
| forwarded | 32 | put | 19.3 / 33.276 / 24.208 | 2635.19 / 2081.63 / 2590.59 |
| forwarded | 32 | get | 10.582 / 18.885 / 11.725 | 6524.29 / 5081.97 / 6383.28 |
| worker | 1 | put | 13.382 / 12.545 / 19.713 | 115.35 / 123.43 / 86.7 |
| worker | 1 | get | 4.595 / 2.779 / 9.938 | 556.21 / 655.54 / 365.39 |
| worker | 8 | put | 249.16 / 17.307 / 55.466 | 235.04 / 714.86 / 560.52 |
| worker | 8 | get | 32.622 / 27.873 / 18.796 | 880.61 / 905.72 / 1143.1 |
| worker | 32 | put | 49.291 / 48.256 / 131.557 | 1224.71 / 1010.25 / 791.75 |
| worker | 32 | get | 95.774 / 92.605 / 67.739 | 1242.41 / 975.64 / 1231.01 |

最终性能场仍有抖动：owner串行PUT第三轮p99/max66.010ms，Worker并发8第一轮PUT p99 249.160ms。该场不是严格独占机器（可见其他cell-agent、CLI web、k3s负载），因此不宣称所有分位数改善或全部长尾已消失。慢commit日志显示159.788ms例子的replicate=136.901ms、enqueue=22.831ms、bucket=0；另52.477ms例子的enqueue=46.980ms、bucket=0。这里需要等待副本/本地spool，不能通过删除RPO=0屏障优化；可继续按既有P2计划做spool批量持久化及独占资源对照。此前567ms同步owner查询阻塞与这些复制/磁盘尾延迟是不同来源。

最终补充扫描修复后的强杀恢复100/100、冷COS恢复100/100（首GET1.695670s）、routing-safety均PASS，应用清理HTTP200。COS-CONTRACT 7/7无skip通过，含新增TestProviderListCloud、原生append竞争、条件删除、分页恢复与复制链。全量检查/回归/race与摘要位于[证据目录](kv-forward-route-fix-2026-10-27/)。本轮不推送、不部署；仍不能将Worker所有长尾或整个优化计划标为已完成。
