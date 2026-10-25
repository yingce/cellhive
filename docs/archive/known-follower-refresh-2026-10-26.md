# 已知 follower 独立租约刷新（2026-10-25—26）

## 原因与实现

此前 SG 热写的 555.8ms 样本中，SQLite仅0.111ms，桶持久化等待553.1ms。全量nodes扫描滞后使follower缓存过期约2.57s，`no_eligible_follower`触发RPO=0桶回退，后续请求被前序提交拖至604ms。原始证据见 [前测](sg-kv-performance-2026-10-25.md)。

ADR-295 / `a3c5c7e`：cell-agent生命周期中启动独立已知节点刷新任务，默认每2秒点查已知非本机节点；每轮最多8个并发，每次超时不超过1秒和刷新周期。只追踪存活或过期不超过一个LeaseTTL的节点，历史死节点不持续轮询。请求只读内存，不增加热路径List或同步点查。

点查按桶中原始session/instance/address/expiry更新缓存，错误不延长租期、404移除。快照copy-on-write，扫描开始后取得的新点查（包括404）优先，慢扫描不得回滚身份/地址或复活已删除节点。关闭时取消并等待后台点查退出。owner接管、epoch/instance fence及真正失去follower后的RPO=0桶回退不变。

`29d8d55`补全部分扫描边界：Sample会跳过失败GET，遗漏节点并不代表被删除，因此保留尚有效或在一个TTL重试窗口内的已知节点，留给独立点查确认；不延长expiry，古老过期记录仍可清理。

冷启动尚未发现任何peer时仍可回退桶；本改动不保证COS或peer真正不可用时不回退，也不消除复制、磁盘、调度等其他尾延迟。固定小集群以默认周期验证；大集群的点查成本随活节点数增长，8个worker限流不等于任意规模都能在2秒内完成一轮。

## 本地验证

- 先添加回归：旧版本缺少独立维护，blocked discovery / late replacement / late deletion均失败；实现后lease race重复10次通过。
- 全量List阻塞时，已知peer仍更新session、地址和expiry；保留旧快照的读者不被修改。
- 慢扫描读取旧身份后暂停，点查已获得新instance/session或404；扫描恢复后不能覆盖或复活节点。
- 点查超时/身份不匹配不能续期；历史死节点不轮询；取消中断GET；并发最多8个。
- 部分扫描遗漏已知有效peer：修复前缓存被清空，回归失败；补全后race重复10次通过。
- 双节点真实HTTP owner commit→follower append/fsync，阻塞全量发现；越过原缓存到期时间后仍取得fleet证明并逐段核验follower spool；权威lease改为过期后返回bucket-batch。初版race连续5轮通过（70/82/86/94/100条fleet提交）；最终版相关server/lease/owner/watchdog race通过。
- 最终源码 `make test`、`make vet`、Docker build/push均退出0。
- 最终源码显式执行 `scripts/e2e/session-recovery.sh`（不是依赖普通测试中的skip），11.81s通过：两节点地址变化、恢复中断重启、空替换磁盘拒绝、原follower磁盘恢复、80个确认值恢复、旧控制注册恢复、两个旧session封口；SIGSTOP owner超过TTL后peer恢复并推进epoch，SIGCONT旧进程退出1，新session能读确认值。

## SG 第一轮（revision65）

初版 `a3c5c7e`，digest `46f41e108b6d93333a703969d7bd113ed222b8e8802cb6722faf1c9b9a5cd950`，正式Helm脚本preflight/diff/upgrade通过。6个业务Pod Ready、restartCount=0、imageID一致。KV/DO发布前后持久值、DO binary/header/cookies/deleteAll/redirect通过。

独立namespace `ops-sg-known-refresh-20261025`，owner agent-0；等待follower-ready后计量。同区真实HTTP、1KiB、单cell、100键；每组warm100不计量，probe使用与前测相同的逐请求traceparent和日志。

|入口|并发|时间|成功|失败|p50 ms|p99 ms|max ms|RPS|
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|owner PUT|1|20s|1897|0|9.97|18.70|39.13|95|
|owner PUT|8|20s|12553|0|11.51|25.34|84.66|627|
|转发 PUT|1|20s|1663|0|11.27|22.02|68.48|83|
|转发 PUT|8|20s|10144|0|14.83|30.48|66.05|507|
|owner PUT 延长|8|60s|36495|0|11.79|29.59|146.24|608|

62,752个计量请求无失败，两个入口逐字节核对100键通过。测量窗口agent0 fleet计数4→12851、bucket-batch12→12；agent1 fleet11→33、bucket-batch13→13。计数是节点全局统计，包含其他业务；桶计数零增量证明该窗口没有新增桶提交回退。冷启动/预热之前已有的12/13次桶提交不能算作热请求回退。

最大尖峰附近scope txid54773–54780的commit为fleet/peer_ack，总137.18ms，其中replicate133.97ms、enqueue3.19ms、bucket0ms。剩余复制尾延迟不属于本次缓存过期回退问题。前后测试不是同步严格A/B，也不能用有限样本保证未来最大延迟。Vector tap结束时kubectl连接reset，未声称trace全量采集；HTTP计量和前后metrics均完整成功。

原始证据：`/tmp/sg-known-refresh/`（red/green、测试、race、session E2E、构建/推送、发布、请求、metrics及日志）；最终验收在其`final/`子目录。

## SG 最终验收（revision66）

最终代码 `29d8d55`（包含 `a3c5c7e`），镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:9e9811870fe03e673b5e1ef4e0fb1cb54ed1984af8b34c1a7e524667b939fd89`。2026-10-26 00:09（Asia/Shanghai）经正式Helm脚本发布revision66，preflight/diff/upgrade退出0；6个业务Pod Ready、restartCount=0、imageID一致。USW/本地本次未发布。两次滚动更新后旧KV/DO持久值及DO行为校验通过；测试scope的100键也在再次更新后经两个入口逐字节核验通过，owner为agent-0/epoch2。

最终两组各60秒，8并发、1KiB、100键，与前测同一probe、相同traceparent/逐请求日志；每组warm100不计量：

|入口|成功数|失败|p50 ms|p99 ms|max ms|RPS|
|---|---:|---:|---:|---:|---:|---:|
|owner PUT|37789|0|11.46|26.78|155.65|630|
|转发 PUT|31510|0|14.04|30.85|138.70|525|

最终69,299个计量请求无失败，两轮共132,051个计量请求无失败。最终窗口agent0 fleet2→11247、bucket-batch6→6；agent1 fleet11→33、bucket-batch22→22，其他commit mode无新增，live_followers前后采样均为1。**最终窗口没有新增桶提交回退**，不把启动/预热的既有桶提交计入热回退。

最终目标scope的>=50ms慢commit记录均为fleet/peer_ack、bucket_ms=0。其中txid122893–122900总132.56ms，replicate130.14ms、enqueue2.41ms，保留复制尾延迟的限制；未宣称所有请求都低于50ms，也未给出稳定容量保证。

测试结束再从两个入口核验100键，全部正确；正式API删除binding，cleanup `ds_59a2122f402919f0a3b89ad557f10f05`达到complete、attempts1；app删除ok=true，Pod临时probe移除。清理后业务Pod仍全部Ready、零重启。

最终证据 `/tmp/sg-known-refresh/final/`：`summary.txt`、前后metrics、`requests.log`、agent日志、Pod imageID、发布前后校验、cleanup/finish；持久化摘要同时保存在 `/opt/cellhive-helm/reports/sg-known-refresh-2026-10-26/`。
