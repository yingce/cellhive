# Redis/Kvrocks 协调权威本地验证

最新状态：已部署本机常驻 Compose，未部署生产；下文“未部署”为此前测试时状态。见 [本地部署记录](2026-11-24-local-redis-compose.md)。

日期：2026-11-24。分支：`feat/redis-coordination`。未提交、未推送、未部署；不代表 SG/USW/GZ 已切换或生产账单降幅。

## 实现

- 可选 Redis/Kvrocks 元数据权威，owner、owner-gen、node lease、fleet drain/waker、wake 点查/条件操作走 Lua。选定元数据的 Redis 缺失/失败不回落桶。
- 普通 owner claim 在一次 Lua 中校验版本、更新永久 epoch 高水位、发布 owner 和 node/session 持有索引。Lua 对 uint64 做十进制字符串运算，避免 2^53 精度损失；拒绝溢出和损坏高水位。
- 维护/清理通过一次 Lua 预留 epoch，然后 CAS 发布，保留原未知结果回收语义；Redis 模式不再执行 Go 层 owner-gen 读/CAS 循环。epoch 高水位仍保留，不是随 owner 释放就删除的缓存。
- 业务数据与 node-log 保留原桶，分页、流式、GC、原生条件删除能力经 adapter 转发；provider preflight 仍执行。
- 离线初始化复制既有协调数据，保留 epoch/退役信息。桶绑定与 Redis generation 防止普通启动把空 Redis 当新集群；加载中或已激活状态不能回到 object 权威。
- shared 调度修复领取期间重复提示被旧 ACK 清除的竞态；保留业务持久状态校验与低频对账。
- node 上报进程 CPU、RSS、cgroup v2 内存余量及 owner/resident/dormant 口径，valid 字段标识采样与统计完整性。休眠 owner 由 Redis 集合计数，不依赖仅含本机维护对象的 OwnedScopes。

## 环境与验证

独立容器仅绑定 loopback：Redis 7.2.17（16389），Kvrocks（16679，镜像 `sha256:a1e99b3653fb900d5a17b80a9487f74079edc6d5e9b70e3103c01cbfc0d5146a`）。每项测试独立 region 和临时 FS 数据桶，无生产凭据。测试后删除本次容器。

- Redis/Kvrocks 集成：并发 CAS 单胜者、ABA、旧版本释放拒绝、活 owner 阻止重启实例抢占、过期接管、休眠计数、零桶元数据回落、wake 操作、迁移保留大整数 epoch 与退役标记、迁移中断重试、generation 丢失拒绝启动、原始数据流式/分页转发、普通/维护分配共用高水位、重复通知与旧 ACK。通过。
- 两种后端的真实双 cell-agent 进程演练：各恢复 80 条已确认写入；地址变更、中断恢复重启、空替换盘拒绝、恢复原 follower 盘、旧 session seal；SIGSTOP owner 超过 TTL 后 peer 接管并递增 epoch，SIGCONT 后旧进程退出 1，新 session 能读回已确认值，默认保留前任 epoch 数据。最终二进制均通过。
- 实际 CLI：首次离线初始化 exit 0；重复初始化拒绝覆盖 exit 1；迁移后 object 启动拒绝 exit 1。通过。
- `make build`、`make vet`：通过。
- `make test`：首轮与最终重跑均通过。
- Redis `-race`（coordination/owner/lease/scheduleindex）：首轮与最终重跑均通过。Kvrocks coordination/scheduleindex `-race` 通过。

复现时使用 Makefile 的 SQLite tags 和 `CGO_CFLAGS='-O2 -g -DSQLITE_ENABLE_FILESTAT'`。关键环境变量：

```sh
CELLHIVE_TEST_COORDINATION_URL=redis://127.0.0.1:16389
CELLHIVE_TEST_SCHEDULE_URL=redis://127.0.0.1:16389
CELLHIVE_SESSION_E2E_BIN=/opt/cellhive/bin/cell-agent
CELLHIVE_SESSION_E2E_REDIS_URL=redis://127.0.0.1:16389
```

双进程测试为 `./cmd/cell-agent -run '^TestSessionRecoveryProcesses$' -count=1 -v`，Kvrocks 改端口 16679。未设置 session E2E 环境时，普通 make test 会跳过这项真实进程演练。

本次原始日志在 `/tmp/cellhive-redis-authority-tests-final.log`、`/tmp/cellhive-redis-authority-build-final.log`、`/tmp/cellhive-redis-authority-vet-final.log`、`/tmp/cellhive-redis-authority-race-final.log`、`/tmp/cellhive-redis-authority-e2e-final.log`、`/tmp/cellhive-kvrocks-authority-race.log`、`/tmp/cellhive-kvrocks-authority-e2e-final.log`。

## 边界

不保证普通异步主从丢失已确认授权后的透明提升；需要持久化、noeviction 和禁止此类提升的部署契约。Redis 丢库自动重建、在线切换、混跑旧二进制、自动均衡尚未实现。调度持久 outbox 已在后续批次补齐，见 [调度报告](2026-11-24-redis-scheduling.md)。保留低频对账及真实数据恢复/清理的桶访问，不能宣称消除全部 COS 请求。Cron/Queue/清理业务状态继续使用 cell；本批将其 owner/lease 权威和 wake 发现移至 Redis，不重写全部业务调度器。未在真实 COS/OSS 服务执行迁移，也未证明生产成本下降。

## 单入口配置修订与验收

2026-11-24 后续按用户要求移除独立调度和 region/URL 环境项。现在只需 `CELLHIVE_COORDINATION_BACKEND=redis://host:6379/0` 或 `rediss://...`，同时启用协调权威和 shared 调度；不设置、`object`、`cos` 使用既有对象存储与 cluster 调度。Redis namespace 从 bucket 配置派生，同桶节点必须使用一致配置。旧拆分环境项（包括空值）明确拒绝；非法 URL 不泄露凭据，不静默切换后端。已有旧实验 namespace 的 binding 不自动迁移，不删除绑定绕过校验。此前报告中的独立环境变量命令属于历史验证记录，当前配置以 docs/configuration.md 为准。

本轮仅单个生产配置变量的 Redis/Kvrocks 真实测试通过：离线初始化、Queue 消费/重试、Cron 自然分钟、Alarm 设置/改期/取消/重试、后端停止/启动 + agent 崩溃恢复；在兜底扫描 10 分钟条件下，两种后端均约 3 秒完成恢复。双进程 session 恢复/fencing 两种后端通过。默认 object Bindings E2E 通过，配置全包 race 通过。日志 `/tmp/cellhive-unified-config-20261124/`，包含 `redis-scheduling.log`、`kvrocks-scheduling.log`、`redis-session.log`、`kvrocks-session.log`、`object-bindings.log`、`config-race.log`、`build.log`、`test.log`、`vet.log`。未提交、未部署。

单入口配置最终 `make build`、`make test`、`make vet` 均 exit 0，脚本语法和 `git diff --check` 通过；本批测试容器已清理。
