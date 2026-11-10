# 全量 E2E 与 examples 复验（2026-11-10）

状态：本轮常规 E2E/examples 矩阵已跑完；当时 COS assets 并发条件创建失败，后续已修复并另行验收，见 [修复报告](2026-11-10-cos-assets-atomic-create.md)。本报告保留当时原始结果。未提交、未部署。仅修改 `/opt/cellhive-worktrees/celld-ltx-migration`，未修改主工作目录或 workerd fork。

证据目录：`/tmp/cellhive-e2e-examples-20261110`。使用现成 `/tmp/cellhive-workerd-lifecycle-20261110`（workerd 2026-09-16），没有重编译 C++。Go 使用项目完整 SQLite tags，GOMAXPROCS=4、包编译并发 1。Docker 测试镜像由本轮 Go 二进制、当前 workerd JS 和现成 fork 组成，非旧镜像验收。

## 本轮发现并修正

- Workflow 注册校验查实际资源名/ID，不能查 env binding 别名；增加正确/错误注册回归。
- 示例修正：D1 使用逻辑 database_id；React 静态模板日期不超过平台上限；具名 Service 入口补 fetch；WebSocket 返回 pair 的客户端，不能返回已 accept 的服务端。
- R2 head/put 返回值也在调用方提供 writeHttpMetadata；原实现只处理 get，HEAD 示例因此 500。
- 冷路由查询与 routes revision 更新竞态时，有界重查控制面，丢弃旧 revision，不重放租户执行。
- DO connect 像 invoke 一样将已验证 shard/owner node/epoch 传入 Host，并覆盖来客同名字段。
- 默认 supervisor 的 facet scope 使用实际应用 namespace；原捕获默认 workerd 与 paging admission 的应用 namespace 不一致，导致 Compose/Kubernetes 重启恢复返回 do_runtime_capacity。沿用既有 host binding 元数据，未增加桶文件类型；旧错误 namespace 布局依既定 export/import 离线迁移。
- 补充分页生命周期错误诊断，避免仅看到笼统 capacity 错误。

## 已完成验收

| 项目 | 本轮结果 | 证据 |
|---|---|---|
| 本地常规 E2E 15 组 | 全通过：bindings、D1 ID、workflow、workflow recovery、queue、queue multinode、queue concurrency、service headers、review fixes、cell transfer、DO transfer、DO paging fleet、KV routing safety、KV batch recovery、session recovery | local-results.jsonl |
| examples 26 项 | 全通过；实际 Queue 消费、Cron 触发，WebSocket 持久计数，Astro 安装/构建/SSR | examples-async-final.log |
| CLI Bun | 18 pass、0 fail | cli.log |
| owner lease / takeover / deploy rollback / crash | 通过 | extra-*.log |
| DNS/连接中断后的 DO 网络恢复 | 通过；第一次仅挂载目录访问权限失败，使用容器 root 重跑当前代码 | extra-do-network-fixed.log |
| RPO=0 SIGKILL + 删除本地库 | 1200 条已确认写入全部恢复，missing=0、wrong=0，integrity=ok | rpo-zero.log |
| 冷写入、驱逐、同节点重启、跨节点接管、本地擦除 | 通过 | cold-write-local.log |
| MySQL + ProxySQL | 真实 SQL、自动配置、代理故障 readiness 和恢复通过 | mysql.log |
| OTLP / OpenObserve | 日志、trace、Queue 多消息 links 通过 | otlp.log、openobserve.log |
| 原生 Wrangler 跨节点 Tail | fetch/Queue/Cron/Workflow 事件通过 | cf-tail-fixed.log |
| Compose runtime baseline | 全通过，包括 secret 轮换/回滚、队列故障、DO 重启/WebSocket、隔离检查 | runtime-baseline-namespace.log |
| Kubernetes fleet A→B→C | 通过；B 写入只到 follower、尚未进对象桶时退出，C 恢复已确认写入 | k3d-fleet-final.log |
| Kubernetes 默认 bucket 模式及 fleet 故障/滚动 | 两种模式均通过 owner A→B→A、最终计数一致断言；滚动期间存在重试后恢复的短暂 HTTP 500，不是零错误可用性 | k3d-bucket-final.log、k3d-fleet-fault-final.log |
| DO 压测 | 本地单组 runtime，4 个 wrk 线程、64 HTTP 连接；32 WebSocket、14872 消息，0 错误 | do-rps.log |
| 真实 MinIO Docker | 对象存储条件写/逻辑删、复制恢复通过 | minio.log |
| 真实 OSS | review-fixes、queue、queue-multinode 和原生追加/诊断通过 | oss-review.log、oss-native.log |
| 真实 HK COS 10 万条 | 3 次强制 owner 切换，最终冷恢复 100000/100000 正确 | cos-epoch-stress.log |
| 真实 HK COS 10GiB SQLite | 全文件 SHA、10240 行 BLOB 哈希、integrity_check 与跨 epoch 增量均通过；总计 560.89 秒 | cos-large-10gib.log |
| COS assets 并发条件创建 | **失败**：8 写入者 2 个成功，唯一赢家契约不成立；仍为发布阻塞 | cos-assets-detail.log |
| 真实 HK COS 存储契约 | 16/16，零跳过 | cos-contracts.log |
| COS 原生 DO 分页冷恢复 | 通过；首次冷读 1.108 秒、hydrated 286720 字节，独立全恢复后 256 条 BLOB SHA256 一致；17 个 canonical LTX、无辅助 marker | cos-do-cold-paged.log |
| 修复后 DO transfer / paging fleet | 本地及真实 COS 最终二进制重跑通过，owner epoch 1→2→3，强杀 A→B→A 后内容一致 | local-do-transfer-final.log、local-do-paging-fleet-final.log、cos-do-transfer-final.log、cos-do-paging-fleet-final.log |
| Go / vet / race | 最终 namespace 修改后再次全量 Go、vet 通过；supervisor/wranglercompat race 通过，JS 全量通过 | go-all-latest.log、vet-latest.log、race-final.log、js-all-final.log |

Examples 边界：AI 为本地 OpenAI 协议 fixture，不是模型质量验收；Hyperdrive 示例只读连接配置，真实 SQL 由 MySQL E2E 覆盖；react-static 是配置模板，使用临时 HTML fixture，不是已有 React 应用构建。examples.sh 部署真实仓库源码，测试结束清理自己的进程。

COS 使用专用 `cellhive-e2e-hk-1376795072`，应用测试持独占测试锁并使用独立 namespace。性能数据是在全量验收期间收集，存在同机其他测试负载，不作为与历史隔离基准的性能回归结论。

## 最终结果与剩余限制

- 本地矩阵 15/15、COS 矩阵 15/15、examples 26/26 通过，额外 Docker/Kubernetes/OSS/MinIO 等结果见表。
- COS 完整 DO 冷恢复也通过：256 条 BLOB 完整 SHA256 一致，9 个 canonical LTX、无辅助 marker；见 cos-do-cold-full.log。
- 独立 COS assets 唯一创建探针失败（退出码 1）；8 个不同内容并发创建同 key，有 2 个返回成功。该问题未修复，不作为 LTX 恢复失败，也不能被其余通过结果抵消。
- 最终 Go/vet/JS、相关 race 和 git diff --check 通过。本轮启动的测试进程/集群已结束；失败 Compose 实例已清理，证据保留。

性能比较脚本、idle trace/report 是组合/分析工具，不等于新增功能 E2E。未执行历史二进制 A/B 比较；不能从本轮数据推断历史性能提升。以上是本轮实际覆盖，不代表每个可选开关组合都已运行。

测试资源：本轮 k3d 集群已清理，`cellhive:dev` 已恢复原镜像 sha256:88623f95abd861377ce449e99f790415e65697ffcd03834f2f8f5624520ae229。

10GiB 为逻辑 payload，编码后快照 787262031 字节；编码 59.443 秒、上传 31.795 秒。完整恢复验证先执行全量 watermark 校验 75.255 秒，再建立页索引 0.733 秒、物化 85.232 秒、SQLite 完整性/全行哈希 108.179 秒。这是全量校验路径，不是按需读取首条记录延迟，也不是 10GiB 不可压缩上传吞吐。

覆盖边界：本轮 10GiB 为普通 cell SQLite 的流式恢复测试；原生 DO 冷恢复使用 16MiB fixture，没有重跑 `do-cold-cos.py --large` 的 10GiB DO 可选矩阵，也没有运行 100 epoch 专项。
