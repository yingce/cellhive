# 对象发布一致性修复与剩余阻塞

状态：R2 两项已修复并验证；LTX 已按用户决定改为普通上传；原生 DO 固定 e1 的迟到导入竞态已修复并完成本轮验收。未提交、未部署。所有改动在 `/opt/cellhive-worktrees/celld-ltx-migration`。

## R2

- 原子发布记录一次提交独立正文版本和 metadata；失败写入不覆盖当前正文，丢响应不误删可能已提交的正文。
- COS/OSS onlyIf 使用既有 native append authority；S3/MinIO 使用已有 CAS 控制记录，不再对 raw body 的条件 PUT 作安全假设。
- GET/HEAD/Range 和 List 的每个对象固定一份发布记录，避免相同正文、不同 metadata 的并发混用。Presign 固定到正文版本。
- 删除条件绑定最初读到的版本，冲突时保留后来发布的版本。Multipart complete 走同一原子发布路径。
- 保留 `.versions`、`.mpu` 内部 namespace，防止跨 namespace 覆盖内部正文；List include 保留逐字段过滤。
- 老版本正文暂保留，增加物理占用；当前逻辑 usage 不计该部分。安全在线版本 GC 未实现，不能声称旧正文自动回收。
- 旧 R2 raw body/sidecar 布局不在线兼容，使用空目标或离线迁移。LTX 字节、目录未变化。

## 证据

目录 `/tmp/cellhive-final-acceptance-20261110`，二进制 `bin-publication/`，workerd `/tmp/cellhive-workerd-lifecycle-20261110`，未重编译 C++。

- `go-publication.log`：77 包通过，退出 0。随后最后的 include/保留 namespace 修改由专门测试与 race 复验。
- `vet-publication.log`：退出 0。
- `r2-publication-final-race.log`：R2 与 server 相关回归、race 通过。
- `cos-r2-publication.log`、`oss-r2-publication.log`、`minio-r2-publication.log`：真实对象存储 8 写入者 onlyIf 竞争单赢家、正文/metadata 配对、删除重建、空对象、旧版本读取均通过。
- `cos-review-publication-fixed.log`、`oss-review-publication-fixed.log`、`minio-review-publication-fixed.log`：当前 Go 构建与原生 workerd 的 review-fixes（R2/D1）、queue、queue-multinode 均通过。
- 首轮 `*-review-publication.log` 因测试目录遗漏 cellhive CLI 未启动业务测试；补齐构建后上述 fixed 日志通过，不隐藏原失败。
- 独立 review 提出的 namespace 碰撞和 include 过滤问题已修复并补回归。MinIO 测试结束恢复 stopped 状态。

## COS LTX 阻塞

普通 PUT 的 forbid-overwrite 已多次复现并发多赢家；multipart Complete 专项 5 次中 1 次出现两个赢家。不能用 multipart 规避。

单次 Append(position=0) 的不同正文竞争 10 次均单赢家，且成功后的 timestamp metadata 正确。但暂停/中断 8MiB 上传时，HEAD 可见零长度 appendable 对象，失败后仍保留零长度对象。`cos-append-visibility-size.log` 保留响应长度；这不代表已观察到非零部分正文，也不能据此声称大对象原子发布已经成立。未把这个候选接入生产 LTX 路径。独立探针只操作唯一 itest/ 前缀。

最新决定替代此前协调记录候选：用户选择对齐 celld 普通 PUT/multipart，不增加发布协调文件。条件创建探针不是普通 LTX 上传的验收门槛；assets 的独立条件创建要求仍未关闭。

## 普通 LTX 上传验证及曾发现的阻塞

当前工作区接入 Put/PutReader，保留32MiB自适应分片和有界并发，去掉普通上传的远端冲突下载比较。owner/R2/assets 的条件接口未放宽。续链种子保留角色校验；恢复入口保留连续同范围日志的矛盾内容校验。资源退休检查与读取的 ETag 校验仍在。

真实 COS `TestCOSNativeStreamingSnapshotMultipart` 通过：8,475,185字节、单次/重复multipart、时间戳metadata、L1/L2/L3/L9、跨epoch恢复、恢复SQLite完整SHA256。新Go二进制的COS cell-transfer E2E也通过。证据：`/tmp/cellhive-final-acceptance-20261110/cos-ltx-ordinary-upload.log`、`cos-ltx-put-transfer.log`。这些正常流程通过不证明失主上传安全。

故障注入发现不能整体移除旧保护的前提缺口：`NativeDOCaptureEpoch=1`，DO shard owner epoch变化不会改变LTX数据路径。`TestNativeImportDelayedPublicationCannotOverwriteSuccessor` 证明迟到导入把接管者的值9覆盖为7；`TestNativeImportNeverAcknowledgesLostShardAtPublication` 的同路径冲突保护也不再成立。这两项不是过期测试，不能删改成通过。需要先把原生DO的数据epoch隔离、恢复与导入链路对齐，或者保留原保护直到具备该前提。当前普通上传改动是未验收草稿，不可用于上线；没有采用新协调文件，也未部署。

本轮最终检查：`go-ltx-put.log` 全量Go仅上述2项server失主导入测试失败；其他包通过。`race-ltx-put-final.log` replica/recovery/compaction通过，bucket race单独通过（首次合跑发现续链种子本进程并发检查/上传间隙，已加64槽本进程串行并复测；不构成跨进程锁）。相关包vet与git diff --check通过。尚未宣称整体验收完成。


## 原生 DO epoch 隔离修复

原生 DO 的 LTX 数据 epoch 现取已验证的 DO shard owner epoch。独占导入使用领取的 shard epoch；接管者写自己的更高epoch，所以旧请求即使在上传后才发现失主，也只能更改旧目录。capture owner epoch继续单独验证cell-agent权限，不能替代data epoch。原生DO缺少owner身份或携带不匹配data epoch的HTTP捕获拒绝发送；服务端重复核验。文件捕获状态绑定首次epoch，旧pending内容不能改绑到新activation。

冷恢复按claim epoch为上界，使用既有原生LTX续链；只读管理导出查看最新原生链。导入先检查该shard epoch内及前任的已有数据；不再以独立data-owner epoch作为恢复上界。无新标记或协调文件，旧稳定e1部署不得与新版本混跑，沿用用户要求的离线export/import迁移。

两项故障注入保留原安全断言：失主import返回失败；迟到上传后接管者值仍为9。测试中的接管写入改为真实领取的successor epoch，不再硬编码e1；并增加捕获不写e1、跨epoch pending改绑拒绝、冷恢复前任数据、data epoch/shard proof不匹配拒绝的回归。

实际COS（local compute，bucket `cellhive-e2e-hk-1376795072`，workerd `/tmp/cellhive-workerd-lifecycle-20261110`，本轮Go二进制 `bin-do-epoch/`）：
- `cos-do-epoch-paging.log`：fleet确认、owner 1→2→3、两次接管及分页冷恢复、内容证明通过；16,920,576逻辑字节仅135,168本地分配字节。接管读32/33秒包含owner失效等待，不能当成纯冷恢复时间；后续内容proof7.331/5.854秒。
- `cos-do-epoch-transfer-final.log`：SQLite导出完整性与内容、流式转发导入、并发激活409、已激活shard拒绝导入、冷重启读取及再导出通过。
- `race-do-epoch-final.log`：supervisor/capture/server相关race通过。

最初transfer脚本仍断言epoch=1，已按新契约改为切换后epoch>1并重跑；不删除后续实际读写、重启与内容比对。首次全量测试的checkpoint fixture固定传epoch1已同步为实际claim epoch，最终完整结果见下。

最终验收：`go-do-epoch-final.log` 全量77个测试包通过（含既有opt-in集成测试跳过，不代表所有外部provider重跑）；`vet-do-epoch.log` 全量vet通过；相关race、两项真实COS E2E与git diff --check通过。本次未编译workerd，未提交、未部署。静态assets的COS条件创建独立问题、R2旧正文保留的既有限制不因本修复关闭。
