# 真实 workerd DO SQLite 迁移验收（2026-11-10）

代码位于 `/opt/cellhive-worktrees/celld-ltx-migration`。本轮未提交、未部署，没有操作生产数据。使用现成 `/tmp/cellhive-workerd-lifecycle-20261110`（workerd 2026-09-16），没有重新编译 workerd。

## 测试场景

永久脚本 `scripts/e2e/do-transfer.sh`，HK runner 增加 `--suite do-transfer`。本机 cell-agent + 实际 do-supervisor/workerd，经 HTTP 调用真实 DO：

1. 创建 counter=7，以及 8 行、每行 64 KiB 的随机 BLOB；保存逐行 SHA-256。
2. 停止源 runtime，按实际 DO 数据 scope 导出 SQLite，校验 integrity_check、业务表和数据。
3. 导入同 namespace 的新对象。目标 ID 特意选为与源对象相同的 shard，以覆盖恢复后未激活兄弟对象的输出确认路径。
4. 启动全新本地目录的 runtime，通过 HTTP 读取 counter=7，逐行哈希比对；写入 counter=8。
5. 再停止 runtime，另起全新本地目录；通过 HTTP 验证 counter=8 和全部 BLOB 哈希，再导出验证 SQLite。

这里重启并清空的是 runtime 本地目录，agent 持续运行；不将它表述为整个集群同时清空缓存。此前 agent 缓存清除覆盖见 SQLite transfer E2E。

## 发现与修复

- DO 用户类数据 scope 与 `__do__` shard owner 不同。通用 timer 注册曾给用户类打开 cell-agent 空库，export 优先选择该 resident，导致导出无业务表。现在不对 DO scope 打开通用 timer cell，DO export 使用原生 LTX。
- 新导入对象尚无 manifest/facet 文件映射，首次 native 打开曾直接初始化空库。现在 native 分配文件名后，在打开 SQLite 前按稳定对象 scope 恢复原生 LTX；只有明确无历史才可创建新库，其他错误拒绝打开。
- OSS 同 shard 场景中，RestoreHost 注册的未激活兄弟对象阻塞当前请求输出 gate。现在只有 scope 匹配、从未打开、未修改、未失败的 pristine 恢复 descriptor 可视为已持久化，已打开或脏对象仍必须正常捕获。
- 独立 review 发现首次恢复失败可能留下 guard 而丢失清理归属。已在 remote 操作前固定 path/disposable 状态，失败关闭 admission；故障注入证明退休能清除完整 tuple 和容量计数。

未改变 LTX 文件格式或添加对象存储 marker。

## 证据

证据根目录：`/tmp/cellhive-prerelease-review/`。

| 验证 | 结果 |
| --- | --- |
| 真实 MinIO Docker，桶 cellhive-e2e-fixed | 完整迁移通过：`minio-do-final.log` |
| 真实 OSS，桶 llmfs-bj，隔离 namespace | 完整迁移通过：`oss-do-final.log` |
| 全量 Go 测试 | 77 个包通过，`native-do-final-go.log`（环境条件测试仍可能 skip） |
| 相关 race | 通过，`native-do-final-race.log` |
| 全量 vet | 通过，`native-do-final-vet.log` |
| 独立复核 | 首次失败清理问题已修复；复核未发现新的阻塞项，三个相关定向回归通过 |

原始失败记录保留：`minio-do-transfer.log`（导出空库）、`minio-do-transfer-2.log`（新导入对象空库）、`oss-do-transfer.log`（同 shard gate）。初次 MinIO 成功记录 `minio-do-transfer-3.log` 未强制同 shard，最终结果以上表 final 日志为准。


双节点分页恢复回归也通过（`minio-paging-regression.log`）：16.9 MB DO、fleet ACK、两次强杀接管，owner epoch 1→2→3，counter 10→11→12；两次冷恢复标量读取的本地 allocated bytes 均为 135168，随后全部 256 行 BLOB 哈希一致。接管读取分别 31/32 秒，包含旧 owner 租约失效等待，不作为纯恢复耗时。测试 namespace 清理成功。
