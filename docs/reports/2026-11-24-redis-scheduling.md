# Redis/Kvrocks 调度补齐

最新状态：已部署本机常驻 Compose，未部署生产；下文“未部署”为此前测试时状态。见 [本地部署记录](2026-11-24-local-redis-compose.md)。

2026-11-24，`feat/redis-coordination` 本地未提交改动，未推送、未部署。承接 [协调权威报告](2026-11-24-redis-authority.md)；本报告取代其中“调度持久 outbox 尚未实现”的历史状态。

## 实现

- Queue messages、timer/timer_retry 变更与 SQLite outbox revision 同事务提交；迁移已有库时建立 pending。relay 经 owner/资源 generation/租约检查和 capture 耐久确认后同步发布，再条件 ACK 精确 revision。发布失败保留 pending，旧 ACK 不清除并发更新。
- shared 索引使用 v2 命名空间，以 kind/scope 合并提示。Lua claim/ACK 校验 token 和 revision；领取期间新 deadline 单独保留，旧 ACK 不把未来事件错误变成立即到期。陈旧提示只触发业务权威检查，取消、改期、重试仍由 cell 状态决定。
- Cron 下一 deadline、purge 与 resource-cleanup deadline 投影共享索引；接收 nudge 唤醒原循环，保留 Cron revision/slot 去重、任务租约与 GC 安全门。
- relay 启动发现本机 scope，每秒有界处理候选，每分钟可重投干净 deadline。待发布 outbox 暂缓 cell 驱逐，发布成功后允许驱逐；单个被阻止的 cell 不再阻挡其他干净 cell。初次驻留探针在资源 generation admission 前释放 pin，避免自等待。

## 验证范围

测试采用 loopback 独立 Redis/Kvrocks 容器、临时 FS bucket、真实 cell-agent/user-runtime/do-runtime 和兼容 workerd fork。未连接生产 COS/OSS。复现：

```sh
CELLHIVE_WORKERD=/tmp/workerd-usw-accepted/workerd E2E_PORT_BASE=29500 bash scripts/e2e/redis-scheduling.sh redis
CELLHIVE_WORKERD=/tmp/workerd-usw-accepted/workerd E2E_PORT_BASE=29600 bash scripts/e2e/redis-scheduling.sh kvrocks
```

workerd SHA256：`afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`。

单元与回归覆盖：事务 rollback、关闭重开后 pending 保留、旧 ACK、迁移幂等、Queue visibility/retry、Timer 删除/取消、发布失败不确认、耐久证明失败不发布、非 owner 拒绝、generation pin 释放、真实 Sweep 在发布失败后保留及成功后驱逐、idle/capacity 模式跳过被保护候选。共享索引测试连接真实 Redis，覆盖未来 deadline 在旧 claim ACK 后不提前领取。

最终命令结果另见下方验收记录。

## 保留边界

- Redis 协调权威仍需显式配置和离线迁移，默认 object 模式不变；切换 shared v2 不支持旧新调度器混跑，停止旧写者后启动新版本，依靠持久状态重投新索引。
- SQLite/对象存储继续保存业务数据，Redis 到期索引是可重建投影。保留分钟级对账、孤儿唤醒及恢复发现；不是消除全部桶访问，也未测得生产费用降幅。
- Redis 故障时 pending 可能延长 cell 驻留时间。Redis 权威丢库仍停写，不自动回落 COS 或重建授权。
- 初轮真实 Worker 演练覆盖单 agent 上的消费/重试、自然分钟 Cron、Alarm 设置/改期/取消/失败重投；后续已补双节点 Queue 接管及真实后端停止/恢复演练，见文末。跨节点 Cron/原生 DO Alarm owner 接管及 Redis 数据丢失仍未验收。
- 不保证外部副作用 exactly-once，不包含自动均衡或 Redis 灾难恢复。purge/资源清理验证为单元/回归层，没有执行真实桶删除演练。

## 验收记录

- 最终 `make build`、`make vet` 通过，日志 `/tmp/cellhive-scheduling-build-accepted.log`、`/tmp/cellhive-scheduling-vet-accepted.log`。
- 最终二进制两套真实业务演练均通过：Redis `/tmp/cellhive-scheduling-redis-accepted.log`（产物 `/tmp/cellhive-e2e.nxYv0O`），Kvrocks `/tmp/cellhive-scheduling-kvrocks-accepted.log`（产物 `/tmp/cellhive-e2e.Jx329i`）。包含 Queue 明确重试、自然分钟 scheduled()、DO Alarm 设置/改期/取消/失败重试；一次失败重试记录 attempts=2、completed=1。
- coordination/scheduleindex/cellstore/server 全包 `-race -count=1` 通过，真实 Redis URL 已设置；日志 `/tmp/cellhive-scheduling-final-core-race.log`。Queue/timer/Cron/control 的 outbox/投影专项 race 通过，日志 `/tmp/cellhive-scheduling-race-final.log`；第一份专项日志中 coordination 无匹配测试，已由后续全包 race 补齐。
- 复核发现的未来 deadline 提前领取、generation admission 自等待、驱逐阻塞其他 cell 均已修复并有回归覆盖；最后 scoped review 无新增阻断项。
- 首轮 `make test` 全仓通过（`/tmp/cellhive-scheduling-all-final.log`）；最后驱逐修复另经 cellstore/server 普通测试和全包 race 验证。最终源码 `make test` 全仓复验亦通过，日志为 `/tmp/cellhive-scheduling-test-accepted.log`。
- `git diff --check` 通过；本批 Redis/Kvrocks 测试容器已清理，保留临时日志/产物用于追溯。

## 完整 E2E 补充验收

用户要求继续完整 E2E 后，以当前源码 `make -j1 e2e` 重新编译并执行默认对象权威六套测试；然后用同一产物分别执行 Redis、Kvrocks 权威 + shared 调度六套测试，合计 **18/18 通过**：Bindings、Workflow、Queue、Queue-multinode、Queue-concurrency、Service-headers。多节点用例实际杀掉 n1 后验证存活节点继续消费；并发用例验证 100 条消息完成、四批并发。均为同一主机上的独立进程与临时 FS bucket，环境通过 `env -i` 隔离，测试目录和端口范围独立。

新增通用测试参数 `E2E_COORDINATION_URL`，每套脚本在启动 agent 前初始化独立协调 region 和桶；未修改生产实现。默认完整入口不含 MySQL/ProxySQL、真实 COS 合同/性能、26 个 examples 等独立专项，本轮没有把这些算作已验证。

两种后端的 `TestSessionRecoveryProcesses` 均通过：恢复 80 个已确认值、地址变更、中断恢复后重启、空替换盘拒绝、原 follower 盘恢复、旧 session seal；SIGSTOP owner 超 TTL 后 peer 接管，SIGCONT 旧 owner 退出 1，新 session 读回已确认值。

日志目录：`/tmp/cellhive-full-e2e-20261124/`。
- `standard.log`：默认对象权威六套。
- `redis-standard.log`、`kvrocks-standard.log`：两种协调后端的六套矩阵。
- `redis-session.log`、`kvrocks-session.log`：真实双进程恢复/fencing。

额外故障脚本使用 `E2E_SCHEDULE_OUTAGE=1`：确认延迟 Queue/Alarm 后停止后端、SIGKILL agent，等待任务到期，再启动保留原存储的后端及原目录 agent。首轮脚本因 Docker 重启重新分配随机映射端口，连接旧地址失败；已修复重读地址与 PING 就绪检查，失败原始日志保留在 `redis-fault.log` / `kvrocks-fault.log`，最终结果另记。该测试不是空 Redis 重建，也不是无人介入的自动进程重启。

### 故障测试发现的启动恢复缺陷

刷新 Docker 端口后，Redis/Kvrocks 两种后端均复现已确认任务恢复失败。诊断留样 `/tmp/cellhive-e2e.4NI9ob/agent-after-crash` 中 Queue 原库 TXID=17，上传 spool 仍有尾部；重启后 Queue 库恢复到 TXID=16（另一次只到初始 schema TXID=2）。后台 spool 随后上传，把持久水位推进至 17，capture 正确拒绝落后库。Alarm 也可能恢复为过旧状态。同步重放缺失是启动顺序问题，不能通过提高本地 TXID 或跳过耐久检查处理。

修复：在 session recovery、owner takeover 与本地 hydration 之前同步重放已有 upload-spool；上传/读取/解码失败拒绝启动并保留文件。启动 strict loader 还拒绝已有 `.corrupt`，包括再次启动且队列 count=0 的情况；正常后台 quarantine 行为不变。单元覆盖失败保留、成功重放、损坏及已 quarantine 文件跨两次启动仍拒绝。上传模块全包 race 通过，scoped 复核确认无阻断项。

初步修复后 Kvrocks 故障演练通过；Redis Queue 已接上完整持久链，但 Alarm 曾超过旧 90 秒验收窗口。故障阶段预算改为 240 次轮询（HTTP 调用耗时另计），覆盖分钟级孤儿发现及重试，普通 Alarm 测试预算不变。不能以本用例承诺秒级恢复或 90 秒 SLO。


### 最终结果

最终二进制 Object/Redis/Kvrocks 六套矩阵 **18/18 PASS，所有进程退出码 0**；日志 `object-standard-accepted.log`、`redis-standard-accepted.log`、`kvrocks-standard-accepted.log`。

Redis、Kvrocks 故障恢复均 PASS：日志 `redis-fault-accepted.log`、`kvrocks-fault-accepted.log`，产物 `/tmp/cellhive-e2e.hY7HUd`、`/tmp/cellhive-e2e.fiihKo`。两套均验证延迟 Queue 完成、Alarm attempts=1/completed=1。该轮 Alarm timer 接管约在 agent 重启后两分钟发生，验证预算扩大有实际分钟级发现依据，不应宣称秒级故障恢复。该结果替代早前故障阶段失败状态；保留失败日志用于追溯。

修复后的双进程 session/fencing 测试两种后端均通过（`redis-session-fixed.log`、`kvrocks-session-fixed.log`）。最终 `make build/test/vet` 通过（`build-final.log`、`unit-final.log`、`vet-final.log`）；上传模块全包 race 及跨启动损坏拒绝回归通过（`upload-race-final.log`）。脚本语法与 diff whitespace 检查通过。最后调整仅将脚本总 PASS 标记移到可选故障阶段之后，避免失败用例提前打印总通过。

本批测试容器清理后仅保留日志和临时证据；未提交、未推送、未部署。默认生产配置仍不变。本轮没有执行真实 COS/OSS、异机故障域、MySQL/ProxySQL、全部 examples 专项，也没有证明 Redis 丢库恢复或跨节点 Cron/Alarm 接管。

## shared 即时接管与恢复延迟修复

用户追问约两分钟 Alarm 恢复后，补齐共享提示到接管的路径：缺失/已死亡 owner 经既有资源门禁、过载检查、恢复、ClaimWithReset/CAS 接管；成功后重新读取并验证 owner，再唤醒原 runner。live owner 的 RPC 失败和 lease 查询错误不触发抢占；retired owner 不复活。失败提示按 token 条件释放，一秒后重试，避免已知失败仍占用 30 秒领取窗口；消费者进程消失仍保留原 claim 超时。分钟级扫描不加速，继续作兜底。

最终真实 E2E 把 waker 和 wake repair 周期设为 **10 分钟**，确认后端停止、agent SIGKILL、保留后端磁盘恢复后，Queue/Alarm 走共享提示快速恢复：

- Redis：从后端 PING 就绪、启动 agent 前开始，到延迟 Queue/Alarm 都确认完成，约 **3 秒**。
- Kvrocks：相同测量口径，约 **2 秒**。

日志 `/tmp/cellhive-fast-recovery-20261124/redis.log`、`kvrocks.log`；脚本以 `E2E_SCHEDULE_OUTAGE=1 E2E_FAST_RECOVERY=1` 运行并断言总恢复不超过 20 秒。此计时不包含故意注入的后端停机 12 秒窗口；为同机 FS bucket、小规模夹具单次实测，不是生产最坏延迟或跨机器 SLO。没有取消租约到期等待、spool 重放和数据恢复时间。

定向 race 覆盖 absent/旧 session 接管、活 owner 转发及 RPC 失败不得抢占、lease 错误不触发接管、回调失败/虚假成功不 nudge、retired 不复活、失败 claim 提前重试、旧 token 不释放新 claim。scoped review 无阻断项。测试目录保留编译、全量测试、vet、定向 race 和后续双节点回归日志。未提交、未部署。

即时接管最终验证：`make build`、`make test`、`make vet` 全部 exit 0；Redis 专项 race 与 Kvrocks scheduleindex 全包 race 通过；Redis/Kvrocks 双节点 Queue 正常消费、DLQ、杀 n1 后存活节点消费从 5 增至 8，均 PASS/exit 0。日志位于上述 fast-recovery 目录的 `build.log`、`test.log`、`vet.log`、`focused-race.log`、`owner-safety-race.log`、`kvrocks-index-race.log`、`redis-multinode.log`、`kvrocks-multinode.log`。本批独立测试容器已清理，未提交、未部署。
