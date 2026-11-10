# DO idle retirement owner/checkpoint 修复

2026-11-10，本地迁移恢复实例切至 8081/8082 后，用户访问 Workbench 会话 events 返回 invoke_failed / do_runtime_capacity。

## 已修复并验证

- RetirePagingLoader 的最终捕获遗漏 admission 中保存的 shard owner identity，退回 Supervisor.Epoch，与已有 fileState.dataEpoch 冲突。
- retirement 关闭新准入后，native SQLite 在 detach 前执行的 checkpoint 被普通 capture gate 拒绝，真实 fork 退出 74。
- retirement 使用 admission 保存的 claim，经 OwnerLeases.Status 验证后建立捕获 context；不信任 retire 请求覆写的 owner 字段。
- checkpoint 仅允许仍附着于同一 scope/claim 的 native handle（或现存 ordinary session）完成关闭捕获；新 open、普通 queued capture、detach 后捕获继续拒绝。
- 未放宽 epoch fencing、没有关闭分页/输出 gate、没有更改 LTX 或桶文件格式。

验证：新 owner/过期 owner/请求篡改/关闭中 checkpoint capture 单元测试，supervisor 全包普通测试通过（8.394s），相关测试 race 通过（1.810s），vet 通过。
真实 workerd 的 scripts/e2e/do-idle-retirement.sh：先 seed，停止原 runtime，fresh runtime 接管，连续 5 次写入—释放—重开读回 1..5，全部通过。仅临时副本的 idle/sweep 时间缩短为 1s/100ms；生产 JS 常量不改。
完整包 race 中的 1GiB snapshot fixture 运行较慢而被主动终止，不计作通过；相关 race 和全包普通测试有独立完成证据。

恢复实例仅 do-runtime 服务换用 cellhive:retire-owner-fix-20261110；沿用已有 workerd 构建、数据卷，agent/user-runtime 不更换。公开域名真实登录后，原失败会话页面、会话信息、events 均 200，events 返回 3 条记录。凭据不记录。
证据目录：/tmp/cellhive-retire-owner-fix；repository-e2e.log、final-unit.log、final-targeted-race.log、live-after.json。

## 多 class 恢复修复与验收

继续定位发现：不同 class 共用 storage_id/shard host，但其 owner epoch 独立；objectForRel 又使用最后一次 Bind 的 Class，导致 SchedulerDO/global 被捕获到 SessionDO scope。现将 host identity 统一为 storage_id/storage_class/shard，invoke/connect/restore/abort/delete/restart 共用派生函数；facet class 从自身名称取得。对象 LTX scope 保持不变，新 host 首次 open 按已验证 claim 从对象 scope 恢复，不沿用旧共享 host cache。

本地恢复实例停机后，仅把一份错放的 SchedulerDO LTX 复制到原先无 LTX 的正确 scope；不复制 owner/control 记录、不覆盖现存数据。两端恢复的 SQLite 均 integrity=ok，32768 字节与 SHA256 完全一致。33 条 sandbox_lease 均为 released，故 /status 的有效租约数为 0 符合源数据。旧错误 scope、旧 cache、原始 volumes 和备份均保留。

验证结果：
- supervisor 全包测试通过（8.352s），相关 race 通过（1.801s），supervisor/doruntime/入口 vet 通过；JS 测试 24/24。
- do-multiclass.sh：两个 class 同在 shard5，分别 epoch 2/1，空缓存恢复后 3 轮独立写入、idle 释放、重开全部通过；2 个独立 host manifest。
- do-idle-retirement.sh 再跑通过：owner 切换后 5 轮释放重开，读回 1..5。
- do-transfer.sh 再跑通过：真实原生 SQLite 导出/导入、导入期间并发激活拒绝、非 owner 转发、活跃 shard 拒绝导入、冷重启再导出 counter=8。
- 本地恢复实例 do-runtime 更新为 cellhive:multiclass-fix-20261110，三个服务 healthy。真实域名登录、用户指定会话页面、会话信息、events 均 200；SchedulerDO /health 与 /status 均 200；do-bench.local/get 空缓存恢复值 130694。
- 当前 runtime 日志未再出现原先的 loader admission/epoch mismatch 或 scheduler touch 错误。

证据：/tmp/cellhive-multiclass-fix/{e2e,transfer,idle,race,vet}.log；迁移校验 /var/backups/cellhive/local-migration-20261110-193242/multiclass-repair/checks.json。仅验证上述 DO 范围，不把这些结果等同于整站所有外部依赖验收。未编译 workerd，未提交代码。

## 后续事件：并发冷打开导致持续 capacity（21:31 后修复）

用户会话 `ses_1edd5a1e445847f7` 的 events 请求复现外层 500、内层 DO 503/do_runtime_capacity。进程健康，无须增加容量。代码级回归证明同一 facet 的并发请求会同时通过 admission Map 的空检查，各自异步 admit 后覆写身份对象；native 构造函数在异步加载 bindings 后比较身份引用失败，拒绝的构造被 native facet 缓存，后续请求持续失败。此前单请求 cold/idle 测试遗漏此场景；quiescence 409 是退休握手的可重试状态，不等于进程容量用满。

修复：每个 facet 的并发首次打开共享 in-flight admission promise，保持身份对象唯一；失败清理 promise 允许后续再申请。显式退休等待已有 admit 完成再取得 activation，不提前宣布退休完成。保持 owner/epoch/activation 校验，没有去掉 fencing。

验证：新增 16 路并发单元回归，修复前稳定报 do_runtime_capacity，修复后通过；新增失败 admission 后重试用例。全部 do-runtime JS 26/26。真实原生 workerd 空缓存接管后 16 路冷读取，以及 5 轮 idle 后 16 路重开均通过；多 class 同 shard 独立 epoch 和 3 轮冷恢复/idle 再跑通过。

本地恢复实例仅 do-runtime 更新为 `cellhive:admission-fix-20261110`，沿用原数据卷，未编译 workerd。用户原 URL 登录后 3 轮各 16 个并发请求：48/48 HTTP 200，响应均 6630 字节。最初并发探针缺 User-Agent 被边缘返回 403/1010，使用与登录一致的 User-Agent 后才完成上述业务复验，不把边缘拒绝当作 DO 测试结果。

证据：`/tmp/cellhive-admission-fix/`（js.log、e2e-after.log、multiclass.log、live-results.json）；失败回归 `/tmp/do-concurrent-before.log`。COS 并发退休回归结果见后续补记。

COS 补记：指定 cellhive-e2e-hk-1376795072 桶，owner 切换后首次 16 路冷读，以及后续 5 轮 idle 后 16 路重开均通过，读值依次 0..5；测试 app 清理成功，harness 最终 PASS。日志 /tmp/cellhive-admission-fix/cos.log。

## 后续事件：重接管的 restore_500（历史方案已撤回）

当前局部恢复实现与验收见 [DO 局部恢复报告](2026-11-10-do-owner-recovery.md)，尚未部署。

以下为已撤回的整进程重启方案及历史验证记录。用户要求不得依靠重启 workerd 恢复此冲突；本次移除自动终止回调，保留拒绝覆盖活跃视图。shard 范围恢复尚待实现和验证，运行镜像是否更新必须另行核实。

会话 ses_9fedec5e01fb45b7 对应错误为 `paging: cannot replace a registered database`：同进程重新 claim 后，旧 native 分页视图仍在注册表，恢复拒绝覆盖。手工重启先恢复可用性；代码为该冲突增加 ErrPagingRegistered 哨兵和 supervisor 一次性 fail-stop 回调，终止旧 workerd，再由部署重启清空运行态。仍保留旧视图，禁止强行覆盖或解除活跃句柄；租约管理仅在 native 子进程退出后关闭。代价是同进程其他 DO 短暂受影响。

Go supervisor/入口测试通过；真实 native 故障注入强制重复 restore-host，验证 supervisor 退出、旧租约到期、相同数据目录重启后16路读取值0、5轮写入/idle/16路重开值1..5。修复镜像 cellhive:restore-conflict-fix-20261110 已用于本地恢复实例，补齐 unless-stopped 策略（原测试 compose 为 no）。两个用户会话 events 分别 HTTP200/676727字节和HTTP200/6630字节。未编译workerd，数据卷保留。证据 /tmp/cellhive-restore-conflict-fix/e2e.log 与 /tmp/cellhive-precommit-review/restore-fix-tests.log。
