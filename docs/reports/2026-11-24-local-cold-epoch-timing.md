# 本地冷恢复与 epoch 切换耗时（2026-11-24）

状态：本地隔离测试完成；未修改生产代码、未提交或部署。当前 main 为 c423db58，Go 源码与此前构建的 7df788d3 一致。二进制为本机 bin/cell-agent。

## 测量环境与口径

本机独立临时 Redis 7.2、MinIO（pgsty/minio:latest），独立 bucket/namespace、loopback TCP、真实 cell-agent 进程与 HTTP。没有访问云桶、生产 Redis、生产 cell 或修改常驻 Compose。一次测量一个当前 owner，旧节点退出后另一个节点接管；使用 bucket 模式 ACK，不能冒充线上双节点 fleet。每次 SIGKILL 前先等待本测试节点 upload-spool 连续三次为空，保证被测历史数据已上传；该准备时间不计入故障接管耗时。

主要参数：lease TTL 10s、upload interval 100ms、GC interval 1h；默认分页阈值 32MiB；3MiB 随机 KV 数据三轮、40MiB 一轮。关闭无关 Cron/Timer/waker/rebalance；epoch fold 正常运行。3MiB、40MiB 场景每条 preload value 为 64KiB。

请求耗时从 HTTP 发出到响应读完，排除 JWT 生成/进程启动。脚本原始 `ttfs_ms` 对单次请求还包括一次 JWT 生成，故表中使用 request trace 对齐后的 `request_ms`；故障场景另列从杀进程到首成功的总耗时。启动到 readiness 探测约 230ms，探测周期 200ms，属粗粒度结果，未混入请求耗时。

## 3MiB，三轮结果

| 场景 | 首次成功 HTTP 请求中位数 | 三轮范围 |
| --- | ---: | ---: |
| 同盘重启后首请求 | 24.95ms | 24.14–26.60ms |
| 清空本地缓存后首请求 | 60.42ms | 59.05–66.72ms |
| 主动交接后首请求 | 63.98ms | 59.12–65.39ms |
| 故障接管成功的请求 | 83.17ms | 79.91–87.61ms |

三个 cold 请求对应 cell.open_uncached 分别 37.345 / 39.311 / 46.575ms；其中完整恢复 30.884 / 32.208 / 39.433ms。选中的是 1 个 L9 加 4 个 L0，恢复阶段 5 次完整 GET，另有 1 次 header Range GET；不能套用线上 149 个 L0 的成本。后续首次写入准备另有一次当前 epoch L0 点查。

主动交接的目标 epoch 从 5 到 6，释放旧 owner API 约 1.1–1.2ms，之后首请求耗时见上表。这里没有租约到期等待。

故障接管的目标 epoch 从 3 到 4。TTL 配置为 10s，三轮从杀旧节点到首成功分别 7819.4 / 7941.1 / 7858.3ms；成功请求本身约 80–88ms。总时间含剩余租约、控制 cell owner 恢复、重试和启动，不能称为 Redis CAS 本身耗时。接管前出现旧 control owner 连接拒绝的 500，保留记录。

## 40MiB，单轮

清缓存后首请求 45.618ms，cell.open_uncached 20.371ms；分页恢复探测 12.905ms，7 次 Range GET、涉及 5 个不同对象，没有完整 GET。该样本走按需分页，不代表 40MiB 已全量下载，因此不能与 3MiB 完整恢复作直接吞吐比较。后续逐键验证覆盖全部已确认数据，所有场景 missing=0、wrong=0。

## 150 个 L0 的同数据对照

为控制链形态，另建隔离 bucket：149 条随机 4KiB value，总 610,304 字节（596KiB），upload interval 1ms，不跑持续写入阶段。桶中产生 150 个 L0、无 L1/L9。保持默认 compaction 门槛，不修改生产实现。

1. 清空本地缓存并以新 session 重启：目标 epoch 1→2，首请求 101.083ms；后台打开 76.515ms，恢复实际完整 GET 150 个不同 L0，另有 1 次 header Range GET。全部数据读回一致。
2. 等实际 epoch fold 循环发布 e2/L9，再次清空缓存并重启：epoch 2→3，首请求 41.452ms；后台打开 20.649ms，恢复仅 1 次完整 GET（L9）和 1 次 header Range GET。首次写入准备另有当前 epoch L0 点查。

第二次恢复前桶内仍有 152 个 L0 和 1 个 L9，但选链只下载了 L9；不能按目录文件总数推算恢复 GET 数。两次之间新增一个探针键，业务主体数据相同；这是单机单组对照，不是统计意义上的生产性能保证。独立的另一轮 150-L0 样本首请求 118.509ms，后台打开 93.299ms。

## fleet 补充验证

通过 Go overlay 给既有真实双进程 session 恢复测试加计时，不改仓库测试源码。真实本地 Redis + FSBucket，80 条已确认数据，TTL 为测试既有 3s；暂停旧 owner 后主动等待 3.3s，再发接管读。三轮 epoch 2→3，读及 owner 确认耗时 8.423 / 8.065 / 8.086ms，总耗时 3309.746 / 3308.954 / 3310.206ms。旧 owner SIGCONT 后被 fencing 退出，重启后数据可读，三轮全部通过。这是同机 FS 数据后端、小数据集，不能替代 MinIO 或跨机器 fleet 延迟。

## 失败样本及限制

最初两个无上传屏障的单节点样本，在杀旧节点后分别有 2 / 8 个已 ACK 值缺失：ACK 可能只落原节点 spool，而接管节点无法读取原磁盘。第二轮试加 bucket-proof header，但 KV handler 不使用 commit endpoint 的该 header，未解决前提。这两个样本不计入性能通过结论；后续相同 key 被覆盖不能反证此前没有缺失。最终改为验证上传队列排空后才故障注入，所有有效样本每阶段 missing=0、wrong=0。

另一次 agent 端口 37012 被占导致启动失败，保留日志，改用低于系统临时端口范围的 27610–27613 后完成测量。fleet overlay 首轮计时起点误放在更早 sleep 后，结果自相矛盾，已废弃并修正计时点重跑三轮；未修改生产逻辑。

同机 MinIO 缺少生产 COS 网络 RTT，不能据此认定线上 5–6 秒请求已经解决。本次表明：租约等待与实际打开成本必须分开；高层覆盖能减少下载，但没有覆盖时仍需读取所有必要 L0。

## 证据与复现

`/var/tmp/cellhive-local-cold-20261124/` 内有 run.sh、cold-minio.sh、run-fragmented.sh、fragmented-minio.sh、minio-helper.py、analyze.py、summary.json、各轮日志与 fleet overlay。各轮原始 agent span、逐请求 trace ID、ledger SHA-256 与桶目录快照保留在日志所指向的 `/tmp/cellhive-cold-e2e.*`。临时 MinIO 凭据文件为私有测试凭据，不提交。未推送代码；临时服务完成后清理。
