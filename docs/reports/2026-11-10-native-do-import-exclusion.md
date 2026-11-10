# Native DO import 并发隔离修复与验收

日期：2026-11-10。工作目录：`/opt/cellhive-worktrees/celld-ltx-migration`。未提交、未部署。

## 修复

原导入仅持有数据 scope 的 agent CellStore gate，不能排除远端原生 DO 首次打开空数据库。现在先通过 control owner 当前投影验证 storage ID，使用持久化 storage class 和对象 ID 定位既有 shard，再独占该 shard owner；之后才读取上传 body、验证 SQLite 并发布快照。

- 活跃 shard 返回409，包含向同 shard 的其他未使用对象导入；不抢占运行中的 DO。
- 独占领取持有本地 claim gate，直到发布完成并释放，避免同节点 Claim 重入及后台 idle/rebalance 提前释放。
- runtime ClaimAs/RenewAs/ReleaseAs 不可借同名 node 绕过 node-lease owner。临时 shard owner 不携带 runtime 转发地址。
- 发布前后核验 shard 和数据 scope 的 owner/session/epoch；成功响应前释放 shard。取消和失败使用独立、有界 context 清理；临时释放失败允许按原 epoch 重试，不删除 successor。
- `__do__` 是 host/owner scope，不接受业务 SQLite 导入。普通 KV 等 reserved 数据 scope 保持原流程。
- 修复 native capture lineage 与 agent owner epoch 混用：标准 supervisor 的 native 数据仍在 epoch 1，导入初始完整快照归一化为 TXID 1..1；agent owner epoch 独立用于授权。这样已递增的 agent owner 不会导致导入成功但 native 恢复为空。
- 没有新增对象存储文件类型或 marker，保持原生 LTX。

## 验证

使用既有 `/tmp/cellhive-workerd-lifecycle-20261110`（workerd 2026-09-16，含 paging lifecycle），没有重新编译 workerd。cell-agent 从当前 worktree 构建。

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| 全量 `go test`，规定 SQLite tags | 77 包通过 | `/tmp/cellhive-prerelease-review/import-guard-final-go.log` |
| owner/server 相关 race | 通过 | `/tmp/cellhive-prerelease-review/import-guard-final-race.log` |
| 全量 `go vet` | 通过 | `/tmp/cellhive-prerelease-review/import-guard-final-vet.log` |
| 发布故障两种顺序 | 通过 | `/tmp/cellhive-prerelease-review/import-guard-publication-orders.log` |
| 真实 MinIO Docker + native workerd transfer | 通过 | `/tmp/cellhive-prerelease-review/import-guard-minio-final.log` |
| 真实 OSS llmfs-bj + native workerd transfer | 通过 | `/tmp/cellhive-prerelease-review/import-guard-oss-final.log` |
| 双 agent/native runtime fleet 分页恢复 | 通过 | `/tmp/cellhive-prerelease-review/import-guard-fleet.log` |

MinIO/OSS E2E：真实 workerd 创建 counter=7、8×64KiB BLOB；停止源 runtime、导出 SQLite；先领取并释放目标数据 owner，使下一次 owner epoch 大于1。导入通过 HTTP Expect/100-continue 确定已取得 shard guard，再并发发起原生 DO 读取，确认为409。上传完成后读取7，BLOB SHA256一致，写入8；全新 runtime 目录冷恢复读到8及相同 BLOB，重新导出的 SQLite 也为8。另验证活跃 shard 内未使用对象的导入返回409。两个 provider 都完成独立测试 namespace 清理。

故障测试使用可控 bucket 注入：在不可变快照发布前/后替换过期 shard owner。若 successor 先发布，延迟导入不能覆盖，冷恢复保持 successor 的9；若导入先发布，successor 的冲突初始快照不能取得成功 ACK。旧导入在失去 owner 后不返回成功，清理不破坏 successor。另覆盖上传取消、无效 SQLite、错误/未知 storage ID、同名 runtime、后台释放阻塞及释放瞬时失败重试。这些是确定性 Go 故障测试，不冒充真实云端 lease-loss 注入。

Fleet：16,920,576 字节 DO 数据，follower 提交增量24；两次强杀完成 A→B→A，owner epoch 1→2→3，counter 10→11→12，256行 BLOB 哈希一致。接管读取32/33秒含原 runtime 租约过期等待；冷恢复 SQLite 已分配135,168字节，证明分页恢复仍生效。

首次广泛回归发现一处旧故障测试使用 runtime ReleaseAs 删除 agent node-lease owner；改为对应的 agent Release 后重新通过。首次未带必需 SQLite tags 的命令未执行测试，后续均带完整 tags。

## 操作边界

导出仍按离线迁移流程先停止源 runtime。导入排他范围为整个 shard，而非单个对象。普通迁移使用标准 native capture epoch 1，自定义 supervisor `-epoch` 不属于本轮 transfer 验收范围。

失败导入不承诺回滚已到达对象存储的不可变快照，必须核对目标后再处理，重试不会覆盖已有数据。持续对象存储故障可能使 owner 清理失败并保留 guard；请求返回失败，需恢复存储并使用现有 owner 维护释放。node-lease owner 不会只因记录内 Expiry 到期自动释放。

本轮没有部署生产，没有导出或迁移生产数据库，也没有重新运行 COS、大库10GiB或100个epoch测试。

## 跨节点转发与自动清理补充验收

同日后续修复已关闭上文的手工清理依赖与最新 COS 未验收项；前述第一轮结果保留为历史证据。

- 仅 POST `/v1/control/cell/import` 的管理员转发将 client 总超时由30秒调整为30分钟。浅拷贝 client，共享原 Transport，持续流式转发；请求 context 保留取消语义，无导入重试。普通请求超时不变。
- 独占 shard 前台释放失败后，以原 scope/epoch 去重登记进程内队列。常驻 cell-agent 每5秒触发有界 pass，不依赖 rebalance 或磁盘压力。每 pass 最多32项、15秒，每项最多2秒；存储恢复后继续条件释放。已无 owner 或 epoch 改变即移除旧任务，不重新 claim，不删除接管者，不保证每项5秒内完成。
- 持续存储故障仍失败关闭。队列不产生远端记录；进程退出后由现有节点租约和接管机制处理。

当前代码检查：77包 Go 全量通过，相关 race、全量 vet 与独立 review 通过。定向测试覆盖超过普通转发超时的2MiB流、客户端取消、共享 client 参数不变、前台和后台持续释放失败、存储恢复后自动清理、旧任务不删除 successor。第一版取消测试未读取请求 body 就等待断开，测试 HTTP handler 无法观测取消而挂起；修正 fixture 后通过，初始栈证据保留在 forward-cleanup-unit.log。

真实 COS：`cellhive-e2e-hk-1376795072`，香港 endpoint，本机两个 cell-agent + 既有 native workerd。control 在 A，从 B 的 admin 入口导入；上传中暂停31秒，超过旧30秒上限后仍成功。期间原生 DO 激活409；owner epoch>1 与 capture epoch1 映射正常。counter7、8×64KiB BLOB 全部哈希一致，修改到8，全新 runtime 目录冷恢复及再次导出仍为8。活跃 shard 的新目标导入409。测试 namespace 清理HTTP200。没有重新编译 workerd、没有生产迁移或部署。

证据目录 `/tmp/cellhive-prerelease-review/`：

- `forward-cleanup-go.log`：77包 Go 全量通过。
- `forward-cleanup-unit-final.log`：定向回归通过。
- `forward-cleanup-race.log`：owner/server 相关 race 通过。
- `forward-cleanup-vet.log`：全量 vet 通过。
- `forward-cleanup-cos-transfer.log`：真实 COS E2E PASS，upload_pause_seconds=31。
- `hk-review-1794289257-f16af6-_c7hewfo/`：隔离真实 COS 测试产物。

本轮未重跑10GiB与100个epoch测试；暂停上传测试用于验证跨节点超时，不作为大库吞吐基准。

## 独占 owner 领取丢响应补充

继续审查发现：独占领取的 ConditionalCreate/CAS 已提交，但存储响应丢失时，原代码返回错误，没有 release 回调或后台清理任务；node-lease guard 会残留。修复前定向测试在首次创建和过期 owner 接管两条路径稳定失败。

修复仅在独占领取的条件写失败分支登记本次已分配候选 epoch，复用现有后台释放队列；不重新领取、不返回可用 guard、不增加远端记录。Release 仍验证 node/session/epoch，并使用条件删除。successor 已接管时保留新 owner，旧任务移除。普通 Claim 行为不变。

证据位于 `/tmp/cellhive-prerelease-review/`：`claim-lost-reply-red.log` 为修复前复现；`claim-lost-reply-green.log` 为新增故障场景和现有独占清理 race 通过；`claim-lost-reply-go.log` 为77包全量Go通过；`claim-lost-reply-vet.log` 为全量vet通过。独立审查未发现阻塞问题。这些丢响应场景是确定性 bucket 故障注入，不冒充云端实际丢包。

最新二进制真实COS fleet回归通过：桶 `cellhive-e2e-hk-1376795072`，两个cell-agent/native runtime，16,920,576字节DO；fleet_commit_delta=13。两次强杀完成A→B→A，owner epoch1→2→3，counter10→11→12及256行BLOB哈希一致。接管读取32/33秒含租约等待，分页SQLite已分配135,168字节；测试namespace清理HTTP200。证据：`claim-lost-reply-cos-fleet.log`，产物`hk-review-1794291226-14116a-7i31kpzl/`。既有workerd未重新构建，未部署生产。
