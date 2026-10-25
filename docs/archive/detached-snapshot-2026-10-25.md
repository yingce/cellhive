# 后台基线快照与增量捕获解耦（ADR-294）

工作区 `fix/review-fixes-20261025`，保留已有未提交修复，未发布生产。

## 根因和实现

旧 RequestBaseline 把请求交给同一个 capture 循环：checkpoint、编码、上传全部执行完才能处理后续增量；SnapshotStreamPages 的回调还在 cell.writeMu 内执行，因此异步发起仍会阻塞写入。新增故障注入测试先在旧代码复现上传阻塞导致写入不能确认。

新 RequestBaseline 用 SQLite backup 在本地复制固定版本到临时文件，再在原库写锁和 capture 循环之外流式编码上传。backup 保留页编号，不能使用 VACUUM INTO。临时文件自己的 cell_meta txid 是唯一快照标签；原库不 checkpoint，不重置 WAL cursor，也不通过此后台快照推进原 capture barrier。后续写入继续获得各自增量持久性证明。没有可恢复链的首次快照、WAL 截断后的 rebaseline 仍同步执行；所有权/epoch/session 输出门不变。

本地复制仍短暂占用写锁，并需要额外磁盘空间；这不是“零开销快照”。复制前按实际 SQLite 页数（含 WAL 新页）检查大小预算，按分片预算编码。失败/取消不发布 done，不停止已有增量链。capture 停止取消并等待后台任务，正常结束清理临时目录；进程强杀后的遗留由启动时持有独占目录锁的清理回收。

参考 celld 的 epoch/恢复链边界：只有完整快照才能使当前 epoch 独立恢复，旧 epoch 不因后台快照开始而提前删除。没有修改 celld。

## 回归范围

- 阻塞 snapshot Commit，同时 Put + capture.Wait，要求后者先完成；上传结束后以快照和增量重建，验证并发键及 integrity_check。
- 上传失败/主动取消后继续写入并确认，从旧快照 + 增量恢复确认键。
- 快照成功但增量 Commit 被阻塞，原 barrier 必须仍停留在此前水位。
- capture 停止时取消后台上传，Done 之前等待其清理。
- 数据主要在 WAL 中时仍按完整页数拒绝超额副本。
- 启动清理只回收指定层级的专用临时目录，不删除 cell 文件和同名前缀的 namespace。

## 真实 HK COS

本机进程 + HK 专用测试桶，bucket 持久性、默认 restore parallel=4、3 MiB 预载、1s 门槛不变。没有部署到 HK 计算节点，不等于跨主机 fleet 性能验证。

第一轮功能版（停机/资源边界补齐前）冷重建 1433ms、重启 1254ms、接管请求 1571ms、接管 TTFS 14749ms；140 个已确认键正确、SQLite integrity 正常、应用清理 HTTP 200。原性能门槛仍失败。这一轮不能代替最终版本验收，也不能把与前轮的全部差值归因于优化。

最终产物 SHA256：`990aca777ab7a38a395d4eb12c0cdb3b7f5a9d7a7d0eca5478b2a61fffd6146d`，`bin/cell-agent` 与实际 HK 测试二进制一致。

| 阶段 | 最终版 |
| --- | ---: |
| 稳态 PUT p50 / p99 | 43.0 / 150.9ms |
| 稳态 GET p99 | 1.2ms |
| 句柄回收后首写 | 162.9ms |
| SIGKILL 重启首写 | 884.8ms |
| 换节点首请求 | 1443.2ms |
| 换节点故障到成功 TTFS | 14579.1ms |
| 清空 cells 后首写 | 1660.3ms |

[最终 HK 结果](detached-snapshot-2026-10-25/final-hk.txt)：140 个确认键 SHA256 校验正确、integrity_check=ok、应用清理 HTTP 200。**原 1s 门槛仍失败（exit 1），不是性能全部通过**。本轮删除本地 cells 的首次写依然需要 owner 查询、实际日志下载/应用和增量持久性确认；此修复保证后台快照不再占用原写锁上传或串行 capture 循环，不承诺网络和共享上传队列完全没有竞争。

`make test vet build` exit 0，见 [全量检查](detached-snapshot-2026-10-25/full-checks.txt)；cellstore/sqlcapture/cellcapture/server race exit 0，见 [相关 race](detached-snapshot-2026-10-25/affected-race.txt)。新增故障测试 race 连续 10 次通过，见 [重复回归](detached-snapshot-2026-10-25/fault-race-repeat.txt)。[本地真实双节点 Queue](detached-snapshot-2026-10-25/local-queue.txt) 在 auto 模式、文件 bucket 下通过：消费 5 条、DLQ=1，杀 n1 后消费至 8。未用日志证明每笔都采用 peer-fsync，不能将 auto 配置等同于已验证 fleet 确认路径。

未做受控的“进程恰好在快照分片上传中被 SIGKILL”跨机器故障测试；现有组合是可控单测阻塞/失败/取消 + 真实进程重启/清盘/接管，并分别验证恢复数据与临时文件清理。


最终版本 [真实 HK HTTP 回归](detached-snapshot-2026-10-25/bindings-hk.txt) 全部通过（exit 0）：R2 覆盖/HEAD/GET/range，D1 20000 行查询，Queue 重试/DLQ、双节点 kill owner 后消费接管。三个独立应用均清理 HTTP 200。D1 五次 HTTP 总耗时 66–113ms。额外 replica/recovery race 通过，见 [恢复检查](detached-snapshot-2026-10-25/recovery-race.txt)。

最终状态：本次代码与回归完成，冷重建仍未达到 1s 性能目标；未提交、未部署。
