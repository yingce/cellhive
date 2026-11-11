# 当前问题、限制与验收状态

_更新：2026-11-10；仅描述当前 migration worktree 的验收，不代表已部署或当前线上状态。_

历史问题、逐轮修复与旧阶段清单已移到 [历史记录](archive/known-issues-history-2026-11-10.md)。其中“待实现/验证中”只描述当时状态，不应直接作为当前任务；归档也不等于把未核实事项自动关闭。设计契约见 [decisions](decisions.md)，产品边界见 [compatibility matrix](compatibility-matrix.md)。

## 已验证的本轮修复

- 本地 E2E 15/15、COS 矩阵 15/15、examples 26/26；Compose/Kubernetes fleet、OSS/MinIO、SQLite 导入导出及冷恢复通过。滚动期间仍有短暂 HTTP 500，经重试恢复；不能宣称零错误切换。详见 [全量报告及未测范围](reports/2026-11-10-full-e2e-examples.md)。
- 原生 DO 不再固定数据 epoch 1；使用已验证 shard owner epoch，迟到旧写者不会覆盖接管者的数据。普通 LTX 上传不依赖 COS forbid-overwrite，也不新增发布协调文件。详见 [对象发布修复](reports/2026-11-10-object-publication-fixes.md)。
- COS assets 原子创建已修复：同 key native append(position=0) 加 16 字节长度头，非空与空文件竞争、64MiB 及公开静态资源 E2E 通过。原先失败证据保留在 [assets 修复报告](reports/2026-11-10-cos-assets-atomic-create.md)。
- 默认 supervisor namespace、Workflow binding alias、R2 HEAD、冷路由竞态及 WebSocket 示例问题已修复并复验；不再作为当前未解决项。

- DO idle retirement 的 owner context 与关闭中 checkpoint 捕获已修复；本地恢复实例的原失败会话 events 复验 200，详见 [修复及剩余边界](reports/2026-11-10-do-retirement-owner-fix.md)。

## 当前限制与上线边界

1. **未部署**：以上修改尚未作为生产迁移/切换验收。历史 release 中的集群版本不是本次实时查询结果。
2. **离线迁移**：旧 LTX/固定 e1/旧 R2 sidecar 与新布局不混跑；数据库按 SQLite export/import 迁移。COS raw assets 需要空目标重传或停服清理后重传，仅重发同内容版本仍会命中旧 token。
3. **COS assets 读取**：不再直接预签名物理对象，走 authenticated agent 点读并返回逻辑 Content-Length，保留 runtime 小文件缓存。增加了首次读取对 agent 的依赖，未声称性能提升。
4. **R2 旧正文回收**：不可变版本保留以保护进行中的读取及签名 URL；安全在线版本 GC 尚未实现，物理占用可能大于逻辑 usage。见 [对象发布报告](reports/2026-11-10-object-publication-fixes.md)。
5. **provider 边界**：MinIO/S3 控制记录以条件 PUT tombstone 适配，不能据此声称原始条件 DELETE 可用；不支持的条件物理删除不可降级为无条件删除。COS/OSS 使用原生追加权威记录，其他桶仍需逐桶探针。见 [OSS/MinIO 报告](reports/2026-11-10-oss-minio-acceptance.md)。
6. **规模与环境边界**：本地计算访问真实云桶不等于生产同区/跨主机多 AZ 验收；滚动可用性、长尾需分开评价。100 epoch 深链专项暂缓；最新全量 E2E 未重跑全部可选大库组合，历史 10GiB 原生 DO 证据仍保留于 [大库验收](reports/2026-11-09-final-acceptance.md)。
7. **产品待办**：Hyperdrive、Workflow/Queue 兼容范围与后续性能评估见 [TODO](todo.md)、[bindings](bindings.md)、[compatibility matrix](compatibility-matrix.md)。不因存储验收通过而扩大产品能力声明。

8. **多 class DO 恢复映射已修复并在本地恢复实例验证**：host 按 storage_class 隔离，facet scope 不再依赖最后一次 Bind；错放的 SchedulerDO 数据已停机校正并逐字节验证。真实会话 events、SchedulerDO health/status、多 class 独立 epoch 冷恢复、idle 和导入导出均通过。见 [修复与验收边界](reports/2026-11-10-do-retirement-owner-fix.md)。

## 证据入口

- [全量 E2E/examples](reports/2026-11-10-full-e2e-examples.md)
- [COS assets 修复与专项复验](reports/2026-11-10-cos-assets-atomic-create.md)
- [存储与请求性能](reports/2026-11-10-storage-performance.md)
- [原生 DO 导入隔离](reports/2026-11-10-native-do-import-exclusion.md)
- [历史问题记录](archive/known-issues-history-2026-11-10.md)

- 2026-11-10 后续 DO capacity：并发冷打开重复 admit 覆写身份，导致 native 构造拒绝缓存。已加 per-facet in-flight admission 合并，26 项 JS 和真实并发冷读/idle 回归通过；指定会话 48/48 请求恢复 200。本地恢复实例已更新，详见 [报告](reports/2026-11-10-do-retirement-owner-fix.md)。

- 2026-11-10 驻留配置：普通 cell 默认软上限已由32调至128，空闲5m不变；Go 配置测试、Compose 校验、Helm 渲染通过。本地恢复实例已显式设128，metrics确认 max_open_cells=128、idle_ttl_seconds=300。未修改外部生产集群。

- DO 重接管 restore_500：整进程重启方案已撤回，源码改为目标 Host/shard 的 native detach 后清理与新 epoch 恢复。本地及真实 HK COS 同进程重新接管、跨节点写入后接回通过；尚未部署。见 [局部恢复验收](reports/2026-11-10-do-owner-recovery.md)。

- DO 版本变化且旧 WebSocket 常驻时返回 do_runtime_capacity：源码已改为单 facet 切换、关闭旧连接并等待 native detach；真实 fork 测试 16 并发切换、状态保留和无关连接存活通过，尚未部署。另修复 WebSocket-only facet 首次退出前映射未刷新导致的捕获 scope 错误。

- 2026-11-11 Hello 并发502：已在本地复现调用日志连接积累/子进程1024句柄耗尽，以及无日志时事件循环延迟处理健康连接。响应释放、I/O公平轮询及watchdog有界排空修复验收中，尚未部署。

2026-11-11 Runtime 502 本地验收：完整修复版 Hello 1/8/32 并发短测零失败，32 并发持续 120 秒 150150/150150 成功；KV 33553/33553 成功并校验内容。Hello 子进程句柄峰值79，未发生健康检查失败或重启。代码尚未部署；详见 [验收记录](reports/2026-11-11-runtime-502-fix.md)。

2026-11-11 遥测慢服务隔离（实施中）：调用日志与 spans 共用每 isolate 最多16个并发导出，无等待队列；超额遥测丢弃，不影响业务数据。导出覆盖响应头及响应体的3秒超时，响应体最多64KiB，正常读取释放连接、超限取消。业务响应和 resident release 不等待导出，保留 ctx.waitUntil 生命周期管理。需验证慢响应、挂起响应体、不可用及饱和时业务请求成功。

2026-11-11 冷加载 admission 修订：同一不可变 Worker 版本的 preparedInflight 等待者复用加载，不重复占用 coldLoads；保留16个不同冷加载的资源上限。避免32并发首次访问同一 Worker 时误报 cold-load capacity。

2026-11-11 遥测隔离/冷加载补充验收：真实 workerd 32 并发，首次冷访问及日志服务挂起响应头/响应体、断连、503、恢复六阶段，共 131118 次请求零失败；无 unhealthy/进程退出。相关单测30/30、loader 全套49/49通过。此处为本地验证，尚未部署。

2026-11-11 本地文件日志/Vector：user-runtime、DO共用有界异步spool，DO不再等待span/统计上报。真实Vector停机、重启、轮转补采集和native DO慢span测试通过；采集停机32并发27174次请求零失败。日志仍是best-effort，Pod删除/长时间积压可丢遥测。代码/Helm尚未部署，见 [验收记录](reports/2026-11-11-runtime-file-logs.md)。

2026-11-11 Go遥测转发修订（未部署）：撤回未上线的Vector sidecar，改为Go读取本地spool、批量OTLP直连CellHive Vector；Trace也走可信本地pipe。真实Vector验收发现native OTLP日志解码保留resourceLogs整批envelope，与现有逐条remap不匹配，已改logs解码为Vector事件格式；traces保持原生OTLP供后端转发。日志/Trace验收和故障边界见 [Go转发报告](reports/2026-11-11-runtime-go-export.md)。VWork资源未改。

2026-11-11 DO入口合并（验收中，未部署）：生产默认 `do-runtime` 一次启动即包含render和持久化supervisor，移除独立 `do-supervisor` 产物/启动脚本。原输出门、续租与退出顺序保持；隔离测试使用显式 `-gate=false`。README中英和部署入口同步，Service/PVC命名保持。

2026-11-11 DO统一入口本地验收：相关Go/native测试及导入导出、多class、idle重开、双agent双runtime分页接管E2E通过；两次crash后epoch依次1→2→3，数据保持。使用本地FS bucket和真实fork，不等于本次COS验收或生产上线。见 [统一入口报告](reports/2026-11-11-unified-do-runtime.md)。

- 2026-11-11 节点续租：已完成无截止时间的续租 I/O、丢失响应确认及节点换段清理阻塞的代码修复；本地 race、owner/server 回归、双进程 session-recovery E2E 通过。真实 COS 故障注入尚未进行。原租约过期仍停止服务。未部署；线上具体阻塞请求及恢复 fragments.log 缺失仍未定位。

- 存储 debug 日志需在 cell-agent 启动时设置 `CELLHIVE_LOG_LEVEL=debug`；本轮未在线开启。逐 HTTP 请求的 SDK 重试标识仅在 SDK 提供时记录；本地序号不是跨节点 trace ID。

2026-11-11 SG rollout exposed stale workerd HTTP-client addresses after headless agent Pod IP changes. Live user-runtime sockets continued SYN_SENT to retired 10.0.16.82:7001 while DNS returned current endpoints. Stable general/per-ordinal ClusterIP routing is being implemented; this alone does not prove every historical Capability deadline exceeded had this cause. USW also observed one lease_replaced restart after ambiguous renewal; that separate issue is not resolved by routing changes.

2026-11-11 部署更新：稳定 agent ClusterIP、统一 runtime、Go 日志出口已部署 SG 131 / USW 118；新 Pod 均 Ready、采样重启为零，SG 公开 KV canary 12/12 成功。旧 Pod IP 连接残留在新 runtime 网络采样中未再出现。上述独立的 USW lease_replaced 问题保持开放；本次不宣称全部历史超时已消除。

2026-11-11 USW lease_replaced 已定位并修复源码：超时后读到旧值过早清除 pending，下一次续租把自身延迟提交判为竞争者。现在保留并精确重试原条件操作；本地复现、race、真实 HK COS 4 项故障注入（含换段）及双进程恢复 E2E 通过。尚未部署，原始网络超时原因仍未确定。见 [验收记录](reports/2026-11-11-lease-delayed-commit.md)。

2026-11-11 发布更新：上述 pending 续租修复 `3e08b76` 已部署 USW 119 / SG 132，四个 agent Ready、零重启，采样未见续租/fencing 错误；SG KV 12/12 成功。仅更新 agent，原始网络超时原因仍未确定。
