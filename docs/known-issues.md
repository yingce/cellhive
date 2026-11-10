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
