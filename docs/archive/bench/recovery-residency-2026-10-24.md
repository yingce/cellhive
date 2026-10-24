# 异常恢复修复与 cell 驻留评估（2026-10-24）

源码基线 main 81dc5b6；修复已提交并推送为 456f6c2，已发布本地、SG、USW（详见文末发布验收）。保留原有未提交 OTLP 改动，未纳入镜像。设计契约 ADR-286，配置入口见 [configuration.md](../../configuration.md)。

## 修复内容

1. scopeAuth/queue transfer/resource cleanup 等所有 invalidateStaleLocal 调用方传播错误。busy、点查或失效错误不再被当作已清理，也不再继续接管。
2. queue/timer 使用 ClaimWithReset：先恢复前任 session，再清理旧本地副本，最后条件发布 owner。失败留下旧 owner，下次仍需清理。所有本进程 Claim 按 scope 串行化并支持等待取消，避免其他认领路径绕过准备中的接管；不同 scope 不共享慢恢复锁。生产 reset 使用 TryInvalidate，避免在 claim 门内等待正在等待 claim 的请求。
3. TryInvalidate 在 verify 成功后才移除句柄/LRU；verify 失败保留原句柄。
4. sealed tail 保存最后一个返回 frame 后的服务端游标；文件身份/大小/mtime、sealed final 和请求 after 精确匹配才复用。重试、游标未命中和重启可从头校验，不改变协议。大 frame 长度先验证不越过文件末尾。
5. 驻留检查发现并修复额外问题：最旧 LRU 被 Pin/busy 保护时，idle sweeper 原来会驱逐仍新鲜的其他 cell。现在逐候选检查实际年龄；超过数量上限仍按 LRU 回收。

不降低 bucket CAS、epoch fence、前任恢复或 RPO=0 证明要求；本地数据格式、HTTP 和 node-log 格式均不变。

## 验证

- 修复前 reproducer 失败：请求 HTTP200 接管且旧文件仍在；queue 第二轮绕过失败 reset；verify 失败遗失句柄。新加 idle sweep 测试也在修复前失败（pinned/busy 两种）。
- 修复后覆盖：busy 请求503且 owner 不变、释放后重试成功；reset 失败反复阻塞、成功前 owner 不可见；先 recovery 后 reset；并发 Claim 等待和取消、不同 scope 独立；verify 保留句柄；idle 不误驱逐；sealed tail 重试/重启/截断/替换文件。
- make test、make vet、make build 通过。随后五个相关包再次运行 -count=1 通过；最终测试清理后定向回归和容量探针编译通过。
- owner/peer/queue/cellstore/server 五包完整 -race -count=1 通过。
- scripts/e2e/session-recovery.sh 真实双进程、真实HTTP通过，8.35s：双方地址改变、恢复中强杀后再次恢复、同名空盘拒绝、换回原盘、80个已确认值和旧 control 登记恢复、旧 session 封存。
- E2E 使用本地 FS bucket，不等价于 COS/跨主机网络故障全覆盖；本轮未做线上故障注入；发布验收见文末。
- 具体输出在 [证据目录](recovery-residency-2026-10-24/)。红测中的 TestReview* 后续重命名为正式回归名称；make test 运行时尚未添加容量探针，最终单独编译并执行了探针。

## 恢复分页读取量

合成合法 sealed log，4KiB payload，256条/页。Linux /proc/self/io 的 rchar 包含页缓存命中，反映返回给进程的读取量。旧版在隔离 git archive 中执行；旧版量测版将已证实失败的读取量断言降为日志以收集所有档位，不是旧版通过修复验收。

| frame数 | 文件字节 | 旧实现读取字节 | 修复后读取字节 |
|---:|---:|---:|---:|
| 2048 | 8,556,544 | 38,548,156 | 8,581,912 |
| 4096 | 17,113,088 | 145,553,967 | 17,167,326 |
| 8192 | 34,226,176 | 564,926,569 | 34,338,143 |

8192条从约16.5倍读放大降到1.003倍；该轮耗时旧332ms、新49ms。墙钟受同机负载影响，不将比例外推为线上20GB恢复总耗时。上传/校验成本仍在，重启首次 resume 仍需一次前缀扫描；多个恢复者交错请求不同 after 时也可能游标未命中。回归验证读取量上界，不使用毫秒阈值。

## 驻留资源测量

工具：scripts/bench/cell-residency/main.go。每档独立 Linux 进程，taskset -c 0,1 + GOMAXPROCS=2；是2核亲和性，不是独占机器或完整生产容器。真实 cellstore、cellcapture 空闲轮询，每cell写一个值并预热两个独立SQLite连接。采样前回收Go堆，读进程RSS、FD、rusage进程CPU时间；原始样本3s，64个大值档再测5s。无HTTP、fleet上传或写入负载，不作为吞吐容量承诺。committer拒绝额外写入，最终探针检查采样中无意外提交。

| cell数 | 每cell 64KiB值RSS | 每cell 2MiB值RSS | 空闲capture CPU核数 | FD数 |
|---:|---:|---:|---:|---:|
| 32 | 26.7MiB | 221.2MiB | 0.12 | 199 |
| 64 | 40.1MiB | 421.3MiB | 0.21 | 391 |
| 128 | 67.6MiB | 820.2MiB | 0.43 | 775 |
| 256 | 123.2MiB | 未测 | 0.88 | 1543 |

64个2MiB值复测：419.6MiB、0.218核。128个小值、不启动capture的对照：63.2MiB、约0.00006核，FD647。CPU差异主要来自capture和owner监控循环；仅打开数据库不等于持续消耗这些CPU。

PRAGMA cache_size=-2000、page_size=4096、mmap_size=0；每cell最多3个连接，本次实际2个。SQLite C分配不计入Go heap；生产RSS还需为控制面、peer、快照、冷恢复、查询及大值缓冲留空间。2MiB值结果不能外推到25MiB值/复杂D1查询。

复现：

```bash
go build -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' \
  -o /tmp/cellhive-cell-residency ./scripts/bench/cell-residency
GOMAXPROCS=2 taskset -c 0,1 /tmp/cellhive-cell-residency \
  -cells 64 -value-bytes 2097152 -sample 5s
```

CPU编号须按所在机器允许的CPU集合调整。

## 数量与时间建议

- 2核/2GiB：32个，5m。
- 2核/4GiB：64个，5m；2核与其他服务共享时先32。
- 2核/8GiB：先64，读多写少且CPU/RSS有余量再压测128；不因内存足够就开256。
- 热集固定且周期访问可尝试10m；大量一次性访问可尝试2m。先固定数量再调TTL，观察open/evicted/ensure模式和HTTP p95。
- 数量指每cell-agent驻留的cell（含control），不是平台支持的数据库总数，也不是SQL并发数。
- 现有32/5m默认不变。上限由最长30s周期的sweeper执行，busy和pinned可超限；不是防OOM硬限额。
- Idle按Store.Cell访问计时，后台也可能触碰；5m通常是5m到5m30s后获得回收机会，busy延后、control常驻。回收保留数据文件，同owner重开一般可复用本地文件与hint。

## 线上只读快照

2026-10-24 本轮读取两地区Pod spec、/metrics 和 kubectl top；无配置修改。各Pod内存limit4GiB、CPU request250m，未配置CPU limit，不能把线上视为固定2核实验。

| 地区/节点 | open cells | 上限/idle | eviction累计 | CPU(top) | memory(top) |
|---|---:|---|---:|---:|---:|
| SG/0 | 2 | 32 / 300s | 4 | 110m | 32Mi |
| SG/1 | 8 | 32 / 300s | 12 | 134m | 43Mi |
| USW/0 | 5 | 32 / 300s | 4 | 90m | 35Mi |
| USW/1 | 4 | 32 / 300s | 8 | 125m | 29Mi |

单次快照没有达到驻留上限，无需仅因本建议立刻扩到64；累计eviction不能当作单位时间抖动率。先部署错误修复，再观察至少一个业务高峰窗口的驻留/驱逐/重开与延迟。

## 本轮补测 HTTP RPS 与 E2E 覆盖说明

修复版工作区二进制，本地两个独立 cell-agent，各自绑定不同的2个CPU（共4个），压测客户端另用2个CPU；共享本机文件系统 bucket，真实SQLite、HTTP、capture与fleet持久化，非COS/跨主机。后台循环保留默认值，驻留64/5m。单热cell、1000个key、64字节value，每档5s，16/32并发分别两轮；单并发一轮。不是最大吞吐承诺，也未做旧版同条件A/B，因此不据此宣称本次修复提高了热路径RPS。

| 32并发路径 | 两轮 RPS | p50 ms | p99 ms |
|---|---|---|---|
| owner PUT | 3893–3934 | 7.40–7.53 | 16.03–16.52 |
| follower转发 PUT | 3045–3056 | 10.06–10.18 | 19.64–21.31 |
| owner GET | 17084–17210 | 1.34–1.38 | 7.47–8.04 |
| follower转发 GET | 9247–9419 | 3.05–3.09 | 8.77–9.11 |

单并发owner PUT：168 RPS、p50 5.63ms；GET：4794 RPS、p50 0.20ms。并发32转发相对直连吞吐约低22%（PUT）、45%（GET）；不是单请求多了同等比例的固定延迟。

计入1000次seed但不计warmup，共683334次测量请求，Failures=0（HTTP 2xx）；结束后从两个节点抽查k0/k499/k999/verify，均为预期64字节，owner始终为rps-0/epoch1。并未对每个GET body逐字节断言。owner的fleet证明计数从1029到9509，新增8480；bucket-batch新增4。日志相应出现4次无eligible peer的回退警告，本轮未根因分析；不能称全程纯fleet，也没有跳过持久化。计数单位是提交证明，不等于HTTP请求数。

原始结果、前后指标及可复现临时harness在本目录证据子目录的 http-rps*、rps-*.prom.txt。服务仅限loopback与临时目录，完成后仅停止本次创建的两个进程；未运行旧脚本中的全局pkill或目录清空。

E2E具体边界：

- 两节点各写40个不同值后强杀；将测试bucket回退至baseline，移除业务SQLite和upload-spool但保留follower日志，模拟ACK后上传尚未完成，强制恢复依赖follower。
- 重启换session和监听端口，验证身份重新寻址；这是同机loopback地址变化，不是真实Kubernetes Pod换IP。
- A等待离线B时保持readyz=503，此时再强杀A并重启，旧session不得误封存。这不是进程级上传一半时的页checkpoint注入；后者由恢复包单测覆盖。
- B同名空目录产生新instance，A仍拒绝其替代旧follower盘；换回原盘后两节点恢复就绪。
- 保留B陈旧的pinned control副本，强制B接管后校验两个资源登记；80个已ACK值全部逐一读回，并检查旧session已封存且covered_seq存在。
- 该进程E2E关闭cron/timer/waker和idle回收；队列reset失败、busy失效、并发claim、句柄泄漏、误驱逐和分页损坏等新修复分支由定向回归/组件测试覆盖。它不是cron可靠性或队列业务投递的完整E2E。

## 发布验收（2026-10-24 15:59 UTC）

- 修复提交：`456f6c2`，已推送 origin/main。镜像来自隔离的已暂存树 `d6bc4d4bb462dfa6b85eb2f3db586f5ca32e6e42`，不包含工作区 OTLP 改动。
- 发布前对该隔离快照重新执行 make test、make vet、session-recovery E2E，全部通过；镜像内五个 Go 二进制构建成功。
- 镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:4af65583df757de94b5856b3725654b7d38b6c0df45ee3ed2236ebf80b46c979`，标签 `456f6c2-recovery286`。
- SG Helm revision 56、USW revision 46，均 deployed；各 6 个业务 Pod Ready，重启数 0，实际 imageID 全部匹配上述摘要。两区域升级后 Helm diff 均为空。
- 本地 Compose 的 cell-agent、do-runtime、user-runtime 三服务均 healthy，实际镜像摘要匹配；mysql/proxysql 未重建。正常停机后备份 cell-agent 和 DO 数据到 `/tmp/cellhive-pre-recovery286/`，等待旧 node lease 过期后启动，未清空数据卷。旧镜像保留为 `cellhive:pre-recovery286`；备份在本机临时目录，不是异地备份。
- 本地 1 节点、SG/USW 各 2 节点：升级前写入的值在升级后逐节点读回匹配，升级后各节点新 PUT/GET 均通过。详细输出见证据目录 release-*-before/after.txt。
- 验收单次 PUT：本地 10.4ms，SG 519.2/148.1ms，USW 354.3/84.2ms。属于启动后正确性 canary，不是稳态性能测试，不代表线上 RPS 或旧新版 A/B。
- 启动后日志检查：SG 与本地未发现错误；USW 有一条已删除资源的 `waker ownership check failed`（`resource incarnation is deleted`）警告，未见 ERROR/panic/fatal。本次不宣称修复该已删除资源告警。
- 本次仅发布修复，open-cell 默认仍为 32、idle 5m；容量建议和本地 RPS 边界不变。
