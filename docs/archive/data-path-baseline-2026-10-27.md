# 数据路径观测与初步基线（2026-10-27）

状态：P0 部分实施；P1a 首步已提交 55da8e6。未推送、未部署。

## 环境与限制

本机双 agent、同机 HTTP keepalive 客户端、1KiB 值、一个 KV cell、32 个 key；远端专用 HK COS，生产 cos:// provider，durability=auto。先验证 owner 和 fleet ACK，再采样 direct/forwarded/Worker binding，1/8/32 并发，两轮。使用既有 performance-dual，不是计划中的 30s 预热/120s/三轮完整矩阵，未固定 CPU 配额，不能作为性能收益验收。Wrangler 子测试因未配置 CLI 跳过，KV HTTP 测试未跳过。

命令：`python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance-dual --port-base 29500`。

## 初步结果

55da8e6 二进制：153960 次热请求、0 失败；热采样 commit 增量 fleet=781、bucket-batch=0（group commit 次数，不是请求数）。测试结束应用 DELETE=200。启动预热期间尚无 follower 时出现 bucket fallback，未混入热采样。

|轮次|入口|操作|p50 ms|p99 ms|RPS|
|---|---|---|---:|---:|---:|
|1|direct|put|8.243|19.06|106.94|
|1|direct|get|0.344|0.589|2653.48|
|1|forwarded|put|6.902|110.76|110.87|
|1|forwarded|get|0.529|1.11|1730.33|
|1|worker|put|8.28|14.756|112.42|
|1|worker|get|1.383|3.504|649.66|
|2|direct|put|6.086|9.341|159.18|
|2|direct|get|0.306|0.786|2926.37|
|2|forwarded|put|6.4|686.32|55.27|
|2|forwarded|get|0.527|1.152|1731.95|
|2|worker|put|7.866|12.035|121.95|
|2|worker|get|1.218|2.683|716.24|

串行 PUT 每组仅60个样本，p99 就是该组最大值。follower 入口出现686.32ms尾延迟，尚未定位；同阶段没有 bucket 回退，但不能据此排除 owner 解析或其他控制路径 COS 访问。GET 转发中位数增加约0.2ms，仅适用于同机测试，不能外推生产跨机器 RTT。

## 观测补充

新增 spool Append 总时长、锁等待、编码、打开/写文件、目录同步；Enqueue 总次数/时长及满队列同步回退次数/时长。Stats 到 HTTP metrics 全链路导出，无租户/对象标签，不改 ACK 顺序或磁盘格式。旧 spool_sync_seconds_total 是成功文件 Sync+Close 加 Rename，遗漏目录同步；保留旧口径并新增独立指标。新计时包含失败尝试，目录同步含后台 Remove，因此不能把阶段总量相加当作单请求耗时。

测试覆盖容量拒绝不虚增磁盘工作、文件打开失败仍计时、队列满回退次数和 HTTP 指标导出。真实 HK 第二次运行用于检查新指标可用性；与 race 测试有同机并行干扰，不能作为5%观测开销对照。

## 尚未验收

完整矩阵、逐请求样本、CPU/RSS固定资源对照、观测开销、compaction/capture完整统计、长尾trace、P1b调度、P2批量spool、P3空闲退避均未完成。不能将此报告作为整个优化计划完成或发布许可。

## 新指标真实验证

第二次 HK 双agent热采样同样153960次、0失败，应用DELETE=200。Owner导出820次enqueue，累计1.588973s；spool Append累计1.584396s，锁等待0.115254s，编码0.003305s，打开/写入0.051376s，目录同步1.200363s（含后台Remove），sync fallback=0。这是整个测试进程累计，包含准备和预热，不是单个热采样窗口。只能确认指标可用以及spool有可测成本，不能定位单次686ms长尾，也不能作为优化收益证明。

原始输出和两节点metrics保存在同名目录。第一轮与第二轮均非无干扰固定资源对照；第二轮与race测试并行。完整计划门槛保持未通过状态。

验证：`make build`、`make vet`、upload/server三轮`go test -race -count=3`全部退出0，新增HTTP指标测试单独运行通过，`git diff --check`通过。此次未重跑全仓make test；前一55da8e6提交的全仓检查通过不替代本次全仓验证。未推送、未运行分支CI、未部署。
