# 上线前 review 修复与回归（2026-11-10）

本轮只修复当前 worktree 的 review 问题，没有提交、部署或操作生产迁移。代码路径为 `/opt/cellhive-worktrees/celld-ltx-migration`，分支 `perf/celld-ltx-storage`。

## 修复

1. DO SQLite import/export 使用的 agent owner 在成功响应前释放；错误路径也尝试有界释放，不受 HTTP 取消阻断。同 scope 传输串行化到释放结束，避免另一传输提前释放仍在发布的 owner。ClaimAs 对 node-lease 前任使用节点/session 活性检查，保留 live owner 拒绝。
2. S3 `r2meta/` 纳入原 key CHC1 CAS 帧，带 metadata 的对象可重复覆盖，去除 metadata 后不会返回旧 sidecar。LTX 原始字节不变。
3. retirement 扫描及迟到 LTX 发布的清理，重新确认持久退休下界后删除旧 epoch 对象。新 incarnation 使用更高 epoch，不会被旧清理任务删除。没有绕过 mutable control 的条件写。
4. 缺少条件 DELETE 的 S3，wake due hint 以每次随机 revision 的独立 key ConditionalCreate 发布；完成或过期提示物理删除。旧提示删除不会影响同分钟的新提示；ptr 仍保持 CAS tombstone。其他 provider 保留原有 due key。

## 验证

证据目录：`/tmp/cellhive-prerelease-review/`。

| 验证 | 结果 / 证据 |
| --- | --- |
| 全量 Go 测试 | 77 个包通过，`final-full-go.log`；含环境条件跳过，不代表所有可选外部服务测试 |
| 全量 vet | 通过，`final-vet.log` |
| 相关 race | bucket/r2/wake/owner/server/replica 通过，`race.log`；最后 DO import/export 修订再次通过 `final-transfer-race.log` |
| DO 迁移回归 | import → export → runtime ClaimAs → 新 epoch PageFetcher 恢复，SQLite 值 42；无效导入不占用 owner；live/expired/restarted session 判定通过。此项为 Go 集成测试，不是原生 workerd 迁移 E2E |
| 真实 MinIO 适配 | 控制 CAS/ABA、R2 metadata 多次覆盖/清除、40 个历史分钟 wake 提示物理回收通过，`minio-regression.log` |
| 真实 MinIO retirement | 旧 epoch 清理、successor 保留、迟到旧上传拒绝并删除通过，`minio-retirement.log` |
| MinIO / OSS SQLite E2E | 100 条值、重复导入拒绝、导入后写入、清空本地缓存冷恢复、再次导出、普通 SQLite BLOB/index 保留通过，`minio-transfer.log` / `oss-transfer.log` |
| MinIO / OSS 路由与删除重建 E2E | GET/PUT/batch 转发、授权拒绝、删除重建隔离通过，`minio-routing.log` / `oss-routing.log` |
| MinIO Workflow E2E | 常规 timer/event/retry、16 个并发 wait 独立消费、旧回调 409、持久消费后响应前强杀并冷恢复输出 42 通过，`minio-workflow.log` |

真实 MinIO 使用本机 Docker 的 pgsty/minio，端口 29920，桶 `cellhive-e2e-fixed`；OSS 使用 `llmfs-bj` 的隔离测试 namespace。E2E 使用从当前 worktree 构建的 Go 二进制；复用 `/tmp/cellhive-workerd-lifecycle-20261110`，未重新编译 workerd。SQLite transfer E2E 在最后 DO export 释放补充之前执行，该补充由最后 Go 全量与 DO transfer/race 回归验证；这些 E2E 覆盖 KV/D1 SQLite，不覆盖原生 DO workerd 迁移。

独立只读 review 未发现四项修复的新阻塞问题。S3 格式变更面向空/离线迁移目标：不自动转换此前 raw JSON 的 r2meta，也不在线删除旧布局 wake tombstone。不得将这些测试描述为生产迁移或部署完成。

后续验收：真实 workerd DO 迁移已补测并修复额外问题，MinIO/OSS 同 shard 导入、HTTP 读写及新 runtime 目录重启通过。详见 [原生 DO 迁移报告](2026-11-10-native-do-transfer.md)，替代本文此前“尚未单独 DO E2E”的当前状态。
