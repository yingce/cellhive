# 换代后异步 Fold 验证

范围：本地 main 工作区，未提交。早期各节记录当时的验证状态；2026-11-23 已按用户要求将当前全部 Go 改动部署本地并验收，生产未部署。原有 cell-protocol/server/sqlcapture 改动包含在本地镜像中；最新结果见文末。

## 设计与实现

见 [设计](../superpowers/specs/2026-11-22-epoch-fold-design.md)。生产删除 idle/pre-release/rebalance/disk-shed 的自动 Fold；每秒仅检查内存 owned 集合，对未完成换代维护安排一个后台任务。等待本代接链可见，完整快照满足预算时发布 L9，否则复用有界 Fold 回退；self-contained 后停止。旧 L0→L1 计数、输入和 GC 门控不变。

一次尝试的输入切点固定；中断后的新尝试可重新规划。完整快照输入上限默认 256 个，逻辑大小和输入总字节预算默认 1 GiB，因此小库也可能因输入数或历史输入字节超预算而走 L2。当前 epoch 长期运行的 L1 对象数仍可增长。

## 验证

- 新增 TestFoldEpoch / TestEpochFold 系列（含 race）通过：未发布本代接链前不抢先生成；固定切点与并发尾部；重启后幂等停止；多个中间 epoch 部分 Fold 后续做；失主停止；预置本代对象后的接管调度（不能证明真实纯读接管，见下方修订）；旧 reader 与新 reader 分别读取各自切点；无 L9 大库使用完整 L2 基线。
- `make build` 通过。
- 相关包普通回归在 Makefile 完整 SQLite 配置下通过：compaction、cell-agent、replica。
- 相关包 `go vet` 通过：cmd/cell-agent、internal/compaction、internal/replica、internal/config。
- `make vet` 被现有测试导入环阻止：internal/wal/wal_test.go（package wal）引用 cellstore，而 cellstore/continuation.go 引用 wal；这些文件本次无修改。
- `make test` 已结束，因上述导入环返回失败；75 个包通过，其余无测试文件的包不计入；没有其他失败项。
- 完整 race：replica 和 cell-agent 通过；compaction 在既有 TestCompactWholeObjectsBelowAndAbove1GiB/above-1GiB 上触及默认 10 分钟超时，未报告数据竞争，不能声称全套 race 通过。该大尺寸用例的普通测试已通过。排除 TestCompactWholeObjectsBelowAndAbove1GiB 和 TestCompactOneGiBSnapshot 两个 GiB 压力用例后，compaction 其余用例 race 全部通过（使用 -skip 选择）。
- 首次直接 go test 没有 SQLite tags，编译被 require_tags 拒绝；补 tags 但未设置 SQLITE_ENABLE_FILESTAT 的普通运行导致分页用例失败，随后统一使用 Makefile 配置，compaction 全包通过。不能将前两次运行视为通过证据。

复现相关检查：

```bash
CGO_CFLAGS='-O2 -g -DSQLITE_ENABLE_FILESTAT' go test -race \
  -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' \
  ./internal/compaction ./internal/replica ./cmd/cell-agent
```

未验证：真实 S3/COS 故障注入、进程 kill/多节点接管、生产负载和部署后的延迟。没有改变 GC 删除权限，也没有声称新基线发布后旧 reader 已切换。

## 2026-11-23 纯读接管修复

原调度测试手工写入本代对象，绕过了 GET/List 不初始化 capture 的问题，不能作为纯读接管收敛的证据。现改为真实 HTTP GET 完成 claim、空目录恢复和读取；确认 GET 后仍仅有前代对象，再启动后台维护，不执行后续业务写入。关闭 prepare 回调时该用例稳定报 `GET-only takeover never established continuation and folded`；接入后通过，并校验本地 TXID 只增加一次 continuation、恢复链全部落在本代，以及从该链冷恢复后的值。

后台仅对已驻留 cell 调用 `EnsureContinuation`，复用 capture 初始化串行化、continuation 发布和重试恢复逻辑；不冷开库，不回退全量 snapshot。要求预期 epoch，并在发布 seed 前再次检查 owner。control schema 在捕获 WAL 游标前完成初始化。Fold 本身仍拒绝未接链的 epoch。capture 关闭时不启动此维护。

新增安全测试覆盖：非驻留、epoch 不匹配、本地领先/落后于 durable、发布前失主；均不发布 seed 或 snapshot，也不推进本地 TXID。cell-agent、cellcapture 与 compaction（排除上述两个 GiB 压力用例）race 通过。真实 COS、多节点故障与生产部署仍未验证。

补充验证：新增冷恢复值校验后的真实 GET 用例在 race 下连续 3 次通过；server、sqlcapture、cellcapture、cell-agent 普通全包回归通过；cell-agent、cellcapture、compaction 的定向 vet、`make build` 和 `git diff --check` 通过。此前全量 WAL 测试导入环和 GiB race 超时限制仍保留，本次未重跑全量矩阵。

## 2026-11-23 空链 Fold 休眠优化

新增 `SkipEmptyRestoreChain` 仅标识成功规划出的空链，与非空链缺少基线区分。调度器对此休眠，使用 replica 现有 L0 上传水位作为内存唤醒提示，不新增通知队列。任务开始前保存水位，结果返回后不覆盖它，保证扫描期间的上传不会丢失唤醒。首次发现仍查桶；L1 清除 pending 对象后保留的上传水位仍可唤醒。失主清理、换代和重启走原发现流程。读取错误、等待接链保持原退避；不改变超时、对象发现规则或 GC 权限。

先复现后修复：`TestEpochFoldEmptySleepsUntilDurableUpload` 在原实现中失败，首次 2 次 List 后，3.5 秒空闲窗口累计为 4 次；修复后仍为 2 次。直接发布真实 L0 后恢复发现，即使 `CoverL0` 已覆盖 pending，也不会漏掉唤醒；自包含后再次停止扫描，换代后重新发现。另以 FS bucket 包装器注入“读取空列表后、返回调度器前上传”和首次 List 失败，验证并发上传与错误重试。

上述用例、真实 HTTP GET 纯读接管、已有 Fold 与 L0Pending 定向 race 均通过，相关三个包 vet 通过。仅本地文件桶计数，不代表真实 COS 延迟或多 cell 负载结果。代码未提交、未部署。

补充全包回归：`cmd/cell-agent`、`internal/replica`、`internal/compaction` race 通过（排除之前已记录的两个 GiB 压力用例）；`git diff --check` 通过。该次未重跑整个仓库测试和真实桶测试。

## 2026-11-23 同 owner 重开后恢复维护登记

根因：空闲驱逐回调从 Owner.OwnedScopes 移除 scope，但 forwardOrClaimMode 对仍属于本 node/session 的 live owner 直接返回，GET/List/PUT 可重新打开数据库却不恢复后台扫描集合；GC 与异步 Fold 因此都可能漏扫。线上只读核查发现 TEST 重新打开时数据库句柄存在、分页 cell 数为零，但 e14 仍保留；本地独立 HTTP 驱逐/重开用例确认缺失登记。旧“未驻留”解释不足以覆盖重开后的情况。

新增 Owner.TrackLocal，仅将已验证 live owner 的同 node/session、非退休、非零 epoch 信息恢复到本地 advisory 集合，不覆盖已登记的更高 epoch。不访问桶、不改 owner 记录或推进 epoch。接入数据请求同 owner 快路径、claim 竞争回退和控制面 claimForRead。后台仍重新验证 owner；恢复登记不构成删除许可。

修复前真实 GET 重开回归失败：GET 成功且 cell 驻留，但 OwnedScopes 缺少 scope。修复后 GET/List/PUT 三个路径 race 均通过，重开保持 epoch=3，按生产后台使用的 OwnedScopes 遍历＋Safe RetireEpochs API 可删除 epoch 1、保留 epoch 2/3。此测试仅对临时新建对象取消年龄宽限；生产一小时宽限不变。另验证控制面读登记、其他 node/session、退休/零 epoch 拒绝和较旧 epoch 不回退。

本次只修复本地代码并运行测试，未提交、未部署，未执行 USW 删除；真实 USW e14 的物理回收须在发布后的后台 GC 验证。

验证完成：`internal/server`、`internal/owner`、`internal/cellcapture`、`cmd/cell-agent` 四包全量 race 通过；相关 server/owner/cell-agent 包 vet、`make build`、`git diff --check` 通过。没有重跑整个仓库测试，之前记录的 WAL 测试导入环仍未处理。


## 2026-11-23 本地镜像与真实 MinIO 验收

已部署既有本地 `cellhive-migration-test` 的 cell-agent、do-runtime、user-runtime，保留原卷与 MinIO。镜像 `cellhive:local-20261123-epoch-fold`，ID `sha256:f6e8beb301eb2c33a29570563a8c0df8db380533a9c78c65661d3671e86ab4a8`；构建基于 main `af5c7534` 加全部未提交 Go 改动，保留此前本地镜像的 workerd/host.js。验收结束检查 Go 源文件与构建 manifest 无漂移，三个服务均 healthy。未提交、未推送、未部署 USW/SG。

本地新建 `epoch-local-20261123/__kv__/EPOCHCHECK`，PUT/GET 一致；同 namespace 部署 `epochcheck` Worker，通过真实原生 DO CounterA 执行 SQL seed=71、五次递增、读取=76。

另建内部 Docker 网络、独立真实 MinIO 桶 `epoch-accept` 和四个 agent（a/b/c/d），仅操作本次新建 namespace/cell。测试加速配置：lease=5s、idle=15s、GC=2s、L1 min objects=4/min bytes=8KiB；生产安全条件保持。c 的快照预算降为 64KiB，专门触发有界 L2；d 经 HTTP 代理注入选定对象 PUT 阻塞。它们不是生产尺寸/时延基准。

| 预期 | 实际验证 |
|---|---|
| 热 cell 同代只 L0→L1 | HOT2 六批持续写入，e1 有 32 个 L0、18 个 L1，无 L9/L2 |
| 低频 cell 空闲不 Fold | 实际驱逐后无 L9/L2，GET 重开保持 epoch=1、恢复 owned 登记 |
| GET/List/PUT 重开均恢复维护集合 | 三个独立 cell 驱逐后依次请求，owned 0→1→2→3，epoch 均保持 1 |
| 纯读接管能收敛 | B 仅 GET 接管 e2，后台发布 continuation 与单个 L9；21 个键一致，没有手工发布本代对象 |
| 同代 L9 不刷新 | e2 继续写入 30 个键，L9 文件保持不变，无 L2 |
| 固定切点不丢并发尾部 | FIXEDCUT 写入尚未结束时发布 L9，cut=10；尾部随后至 TXID=154，L9 不变；新节点冷恢复验证全部 150 个写入键 |
| 新代基线完成后安全 GC | HOT2 e3 后后台退休标记前进，正常一小时宽限仍保留年轻 e1；显式 Safe GC 使用测试专属 grace=1s 删除 e1 的 50 个对象，保留 e2/e3，51 个键一致 |
| GC 后空缓存恢复 | 新节点 c 没有本地库，恢复 HOT2 全部 51 个键；e4 超预算保留 L9 并生成 L2 |
| 大库没有历史 L9 | LARGE 写入 2,621,440 字节业务值，c 接管后生成独立 L2 基线、不生成 L9，40 个键一致；此为降低预算的真实桶测试 |
| 杀进程后已确认值恢复 | LARGE 最后一次写入 ACK 后 SIGKILL c，B 接管并恢复全部 41 个键，包含最后写入；重启 c 后请求正确转发 |
| 中间 epoch 未完成 Fold | 代理阻塞 e2 L9 PUT，e3 仍可沿实际前代链恢复并完成 L9；解锁后读值一致 |
| 旧 owner fencing | 提交有效原生 LTX、携带旧 epoch，HTTP 409 epoch_mismatch |
| 空链 Fold 休眠和上传唤醒 | 隔离独立 GC 后，真实桶 List 计数 6→6（5s）；持久化 L0 后增至 12，随后 5s 不再增长 |
| bucket-proof 必须等实际 PUT | 普通提交返回 bucket-spool 时桶无对象；proof 提交在 PUT 阻塞 3s 内未返回且桶无对象；解锁后返回 bucket-batch，桶中对象可见 |

其他验证：owner/server/cellcapture/compaction 核心用例 race 连续 10 次通过；HTTPCommitter 证明模式和 EpochFold 用例定向 race 通过。实际 `TestCompactWholeObjectsBelowAndAbove1GiB` 和 `TestCompactOneGiBSnapshot` 普通测试通过（合计 63.936s），不是缩小数据量的替代测试。之前记录的 GiB 全量 race 超时未重新宣称通过。

测试异常与边界：

- 最初固定等待 18s 的驱逐断言过早：15s idle 加上 7.5s sweep，改为等待驱逐事件后通过，没有修改生产代码。
- 最初计数所有空链 List，混入独立 GC 每 2s 扫描；仅在测试 d 禁用 GC 后验证 Fold 休眠。优化不承诺消除其他后台任务的桶扫描。
- `TestSessionRecoveryProcesses` 在读取 B 的 `cells/__platform__/__control__/main.db` 时失败，尚未进入故障恢复断言。用旧本地镜像 `cellhive:local-20261122-renew` 的二进制重跑出现同样失败，因此是已有用例/fixture 缺口；没有把它记为通过，也没有修改该测试掩盖失败。
- 之前全仓测试/vet 的 WAL 测试导入环仍未解决。本轮未再次宣称全仓测试全绿。
- 真实对象存储为本地 MinIO，不是 COS。没有等待完整一小时验证默认宽限到期后的自动物理删除；使用真实年轻对象验证默认保留，以及显式 Safe GC 缩短测试宽限后的删除。生产 USW e14 是否释放仍需生产发布后观察。
- 读者 pin、错误重试、本地 ahead/behind、发布前失主、扫描时上传竞争等由前述定向 race 覆盖，不将其描述为本轮全部做过真实网络故障注入。

证据目录：`/var/tmp/cellhive-epoch-accept-20261123/`（仅本机访问）。包含 `source-manifest.json`、`image-build.log`、`results.jsonl`、`phase1.py` 至 `phase6.py` 及日志、`proxy.py`/请求记录、`do-smoke.py`、race/GiB/session 新旧对照日志。Compose 与凭据文件受限权限，不复制到仓库。隔离测试环境结束后停止并保留数据和日志；既有本地三个服务继续运行新镜像。


## 2026-11-23 测试阻塞修复（后续修订）

修复上述两个测试阻塞，仅修改测试和文档，不修改生产二进制行为。

- `internal/wal/wal_test.go` 改为外部 `wal_test` 包，显式调用公开 WAL API；保留同包 `overlay_test.go` 的内部实现测试。损坏 WAL 用例从真实、未损坏 WAL 的公开读取结果取得页大小，并使用 SQLite 格式规定的 32/24 字节头长定位损坏字节。解除 `wal 同包测试 → cellstore → wal` 循环，原有尾部增量、校验和、checkpoint、文件替换断言均保留。
- session 进程测试不再假设非 owner B 会在启动时创建控制库。显式生成已初始化 schema、但没有 ka/kb 注册的旧 SQLite，确认无注册后关闭并拷贝到 B。原有恢复注册断言保留。
- 越过缺文件后发现第二个 fixture 前提：启动时尚无 eligible follower 的控制注册可以只确认 spool，而故障注入随后删除所有本地 spool，造成控制链 `have 2 next 4`。测试专用上传间隔改为 100ms，并在准备阶段等待两节点 spool 排空，再保存业务基线和开始后续故障写入；故障仍删除后续业务桶对象与本地库/spool，依赖 follower 日志恢复，不放宽 RPO 验证。
- 修复后真实双进程 E2E 通过；WAL 和进程用例在 race 测试驱动下连续 3 次通过（子进程使用本地验收镜像同一普通二进制，不能据此声称子进程带 race 插桩）。包括 80 条已确认业务数据、旧控制注册、恢复中断重试、空替代盘拒绝、旧 session 封口、SIGSTOP 超 TTL 接管、旧 owner 恢复后退出与新 session 读取。

本轮日志位于同一证据目录：`session-fixture-final.log`、`fixture-race-repeat.log`、`full-test-after-fixture.log`、`full-vet-after-fixture.log`、`build-after-fixture.log`。此前失败日志保留，以上修订取代前文“两个测试阻塞仍开放”的状态；本地/生产部署边界不变。


最终检查：`make test` 全仓通过（最终文件状态再次运行也通过）；`make vet` 全仓通过；`make build BIN=/var/tmp/cellhive-epoch-accept-20261123/verified-build` 通过，产物仅写到独立验收目录；`git diff --check` 通过。最终全仓日志为 `full-test-final.log`，缺文件和测试导入环两项阻塞均已解除。GiB 全量 race、COS 和默认一小时后台物理 GC 的既有验证边界不因本次测试修复而改变。


## 2026-11-23 再次发布到本地测试栈

按用户要求将验证后的四个 Go 产物封装为 `cellhive:local-20261123-verified`，镜像 ID `sha256:5d272c5461721498bda3c3766fd1808a9f90e063cd530c699254ccdfebad08d7`。保留既有 workerd 与数据卷，顺序停止 runtimes、停止并更新 agent，待 ready 后更新两个 runtimes；三个服务均 healthy，容器内二进制 SHA256 与发布产物一致。升级前 KV 值仍可读，新 KV 写入/读取通过；原生 DO 从升级前值 76 恢复，递增至 77 后读取一致。证据在原验收目录的 `local-release/` 下，包含发布配置备份、构建日志、二进制哈希与 verification.json。只发布本地，未提交、未推送，USW/SG 未改动。
