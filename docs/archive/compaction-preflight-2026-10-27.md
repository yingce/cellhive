# Compaction 预检第一步（2026-10-27）

状态：隔离工作区实现，未发布。工作区 `/opt/cellhive-worktrees/data-path-optimization`，分支 `perf/data-path-optimization`，基线 `056ce86`。

## 已复现问题

在相同 cell 上先生成 L1，然后再次调用 Compact：

- 没有新数据仍读取一次 L1 payload。
- 上次合并后尚未GC的旧L0也计入MinObjects，少量新写触发不必要合并。
- MinBytes包含旧L1，旧基线大小会推动新一轮重写。

新增计数器测试在原实现失败；失败输出归档为 `compaction-preflight-2026-10-27/before.txt`。

## 调整

先获取manifest、读取并校验L0，将已覆盖段排除；按剩余去重段数、字节、包含未覆盖尾部的物理对象数判阈值。只有需要合并时才读取L1。物理对象即使包含重复段，仍会贡献冷恢复读取成本，因此对象阈值计数保留这种对象，逻辑段/字节则去重。

零水位manifest也视为有效基线，已覆盖的txid=0快照不会反复计入阈值。实际合并继续使用原restore.Compact链校验、跨epoch回退、owner检查、索引/manifest发布及GC顺序；不修改桶格式或写入确认。

## 验证范围

- 回归覆盖空尾部、残留已覆盖L0、低于阈值、新增字节临界值、单batch多个逻辑段、损坏尾部不发布manifest。
- compaction/replica/restore三轮race；额外新增batch测试后compaction三轮race。
- 真实HK COS契约6/6，无跳过。生产COS provider的复制/分页恢复测试增加空合并探针，确认L1 payload GET=0，恢复500行完整。
- 不把本次改善描述为完整P0或P1a完成：L0正文仍逐个读取，尚无元数据预算预检；全量性能矩阵、dirty scheduler、spool和capture优化均未完成。

本次确定减少的是无效L1下载和错误触发的合并，尚未测得可归因的热PUT RPS/p99收益。单元计数器不是线上流量统计。

## HK 双 agent 持久性回归

隔离双agent、真实COS、等待实际fleet ACK后写100条，强杀owner，100/100恢复；存活节点graceful drain后删除本次测试SQLite缓存并重启，100/100再次恢复。完整首个冷HTTP耗时1.775463s，未作优化前后同负载对照，不能归因于此次compaction修改。测试应用DELETE HTTP 200。

该脚本显式将compaction周期设为1小时，因此本项证明持久性回归，不证明后台compaction竞争场景已做真实双节点验收。合并与恢复的实际COS路径由契约测试覆盖；定时合并与接管的并发真实E2E仍待补齐。

该次写入窗口观察到100次spool文件sync、194次目录sync、总sync耗时约108.9ms；窗口含后台删除/重放，不能直接解释为每个请求独占的额外延迟。保留为P0后续spool分段观测的线索。

全仓 `make build`、`make vet`、`make test` 均退出0；测试使用pinned stock workerd 1.20260916.1，HK HTTP恢复使用现有平台fork。格式检查及git diff --check通过。未推送，未运行此分支GitHub CI，未部署。
