# P0：后台基线位于增量批次中间（2026-10-26）

## 现场与根因

SG `vwork-77/__invocations__/main` 在 epoch23 每30秒compaction失败：expected3772/got3770。只读导出该scope桶内41个对象（2,154,231bytes），证据保留在受限本机目录 `/tmp/p0-chain-20261026/`，没有删除或改写线上对象。

原始链：e22 snapshot3766 + delta3767、3768；e23 marker3769（截断前驱到3768），delta3769，snapshot3771，delta3770..3772，后续连续delta直到3798。ADR-294后台backup保存固定事务3771，但独立WAL捕获随后把3770..3772折成同一page-map delta，造成新基线无法独立构成严格连续链。不是日志缺少3772；跨水位段仍包含3772，不能直接丢弃。

RestoreChain此前仅凭最新epoch包含snapshot就裁掉前驱；compaction仅读当前epoch；epoch GC仅凭snapshot-done就允许删除更老epoch。三个路径没有验证新基线是否真的独立可恢复。此次e22仍被保留，所以能恢复；没有证据支持所有历史scope均无数据问题。

## 修复（ADR-298）

- RequestBaseline与poll协调：本地复制与登记水位期间阻止poll推进，上传前释放协调锁；后续WAL批次在每个登记的快照水位切分。上传仍不持有写锁/捕获协调锁、不提前推进durability barrier。
- RestoreChain保留最新基线快路径；连续性验证失败时按需读前驱baseline及中间epochs，仍要求最终全链严格连续、完整、checksum有效。
- compaction当前链失败时尝试验证后的跨epoch链，重建为当前epoch的L1；仍执行发布前owner校验。不会直接删除有更新尾部的重叠delta。
- epoch GC在删除前验证当前epoch自身的链，snapshot-done不再单独构成删除授权。

## 验证进度

修复前确定性测试失败：后台复制两次未捕获写入之间产生delta2..3跨snapshot2；跨epoch恢复失败；keepPrevious=0时GC删除必要前驱；当前epoch compaction失败。

修复后sqlcapture/replica/restore/compaction全包通过；四包race重复3次通过。缺失前驱delta必须拒绝恢复、禁止GC的负例重复10次通过。已有后台上传阻塞测试继续验证新写入不等待上传。

实际线上副本：旧RestoreChain只返回30段并失败；完整33段离线恢复成功，SQLite integrity_check=ok，invocations1887行。修复后的RestoreChain返回33段，恢复数据库与完整链恢复的SHA-256一致：`7eb3a9a11f0a3a862085a31635e189f8d84c5f74da52ffb93e82ce4a88840cbb`。副本执行compaction+GC，生成覆盖3798的L1，清理该副本30个L0，再从单个L1恢复，数据库SHA-256仍完全一致。线上原始对象未受该实验影响。

进程session恢复E2E通过（12.189s）：地址变化、中断恢复、空替换盘拒绝、保留盘恢复80个确认值及控制注册、旧session封存、SIGSTOP超过TTL后接管、旧owner恢复后退出、新session读到确认值。

第一次全量测试包含正在修改的临时诊断cmd，导致临时工具编译失败；诊断源码移出仓库后，make test及make vet重跑均通过。新增100次确认写入与10次后台快照并发，每个快照独立加尾部增量恢复全部100个值，race重复5次通过。生产部署及原故障scope冷恢复验证已完成，见下文。


## 发布与原故障数据验收

应用修复 `3932b56` 已推送。镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:780b7228589ad1d26c65ca8a85ee649816dd0f931fabc262229c17a3a7c9d630`，SG revision68、USW revision53、本地三个业务服务均更新。两地各6个业务Pod全部Ready且restartCount0，imageID逐个核对；本地三个服务healthy且restartCount0。使用官方Helm preflight/diff/atomic upgrade，diff仅业务镜像及对应generation。Compose保留原配置、环境与数据卷，回滚镜像 `cellhive:pre-298-local`。

三处发布前后KV与DO持久值正确；两地分别经两个agent验证KV，DO binary/headers/双Cookie/deleteAll/redirect均通过。没有把滚动后的单次PUT样本作为热性能结论。

SG原故障scope在新epoch25从3818继续：delta3819..3820、snapshot3820、delta3821及后续，快照准确落在批次边界。第一份发布后桶数据副本独立恢复到3823，SQLite integrity_check=ok，1899条invocation；原1887条逐行全部一致，新增12条。

线上日志随后确认 `compacted scope=vwork-77/__invocations__/main inputs=8 parts=1 min_txid=3819 max_txid=3825 deleted_l0=8`。旧epoch GC已执行。再次从空诊断目录重新下载桶内当前对象（没有复用第一次下载的旧文件）：共9个对象、931306bytes，当前L1快照3825与manifest。只用权威链选择出的1个L1即可恢复，SQLite integrity_check=ok，共1900条invocation，原1887条逐行全部一致。该证据验证实际compaction/GC之后的冷恢复，不依赖在线SQLite或已保全的旧epoch。

SG最后3分钟日志无compaction/non-contiguous错误；USW本次新Pod日志无同类错误。该观察窗口有限，不等于全桶历史scope审计。原先DO recovery pointer CAS错误/桶回退另案待查，本P0未修改该路径。

本机受限证据目录 `/tmp/p0-chain-20261026/` 保存原始对象副本、发布后两次对象副本、完整性及逐行比对输入、red/green/race/全量测试/session E2E日志、build/push/Helm日志和Pod回读。临时诊断源码移出仓库，Pod内诊断工具及临时副本已清理；本机原始证据保留。
