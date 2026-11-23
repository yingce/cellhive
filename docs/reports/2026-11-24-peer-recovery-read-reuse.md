# Peer / Recovery 有界观察复用

## 设计与范围

依据 `2026-11-24-get-trace.md` 的已测来源，先减少重复观察，保留现行存储协议。方案选择：扩大有界观察复用并降低恢复维护频率；不直接取消恢复选主（当前没有独立的恢复 claim），不引入共享 fleet 对象或把 node-log 合进 lease（需跨版本格式设计）。

已知 peer 后台仍每 min(TTL/5, 2s) 检查本地状态，但只有观察已超过 min(TTL/2, 5s)，或租约不足以覆盖下一刷新周期时才发 GET。未知观察、未来时间、过期与读取错误不能产生存活证明。Recovery 候选发现使用相同复用条件。成功观察时间始终是权威读开始时间；复用不更新时间，不改变原始 expiry。NodeLeaseFresh、接管和恢复候选执行前的权威读取不变。

Recovery 默认检查从 5 秒改为 30 秒，保留现有选主、恢复互锁、失败退避与 60 秒历史发现上限。已知候选的死亡发现增加至最多一个 30 秒周期加当前工作耗时；未知节点/新 session 发现仍受 60 秒扫描周期及执行耗时影响。请求触发的同步恢复不变。选主租约可能在两轮之间到期，下一轮重新 Acquire，不能把上轮领导权缓存为下一轮证明。

peer 地址/session/load 观察可比原来更旧，正常 TTL=10s 时复用最多 5s，再加本地调度间隔；临近 expiry 主动刷新。原有 session 校验和实际接管权威点查保证旧观察不会直接授权写入。已有全量发现仍运行，其成本未被消除。自身续租 4 GET + Append 未改。

## 验证要求

请求计数覆盖复用窗口内后台和 recovery 不重复读取、越界刷新、临近到期、未来观察、失败与取消；显式 Fresh 必须读取并发现新 session/删除；慢扫描不得覆盖更新观察。Recovery 保留新活跃 session 不能被误封存、无请求死节点恢复与历史发现上限回归。运行相关 race 与全量 test/build/vet。

## 状态

已部署 SG / USW / GZ。测量只报告本地计数与源码频率，不推断生产总 GET 降幅。

## 本地对照结果

基线为改动前本地 HEAD 的干净 archive，目录 `/var/tmp/cellhive-peer-recovery-20261124/baseline`。两版使用同一个持续续租 peer fixture，TTL=200ms、检查 tick=40ms（均为生产默认值的 1/50），25 次检查耗时约 1 秒：基线 25 次逻辑 GET，新版 8 次。不包含全量发现、前台读取、COS 协议放大和集群实际负载，不能直接当作生产总请求降幅。

同一个 Recovery 虚拟时钟用例运行默认周期 61 秒：基线进入选主 12 次，新版 2 次。基线在新版“应为 2 次”的断言失败，说明测试能捕获原高频行为；实际 COS 请求数仍取决于持有者、争用、重试和分代。

新测试覆盖 3 秒旧但仍有效观察的双路径复用、超过 5 秒刷新、临近到期、未来观察不复用及 Fresh 强制读到替换 session。原有换主/删除、失败/取消、慢响应与扫描竞态、新 session 不能被旧 lease 误封存、死亡恢复回归保留。两项旧恢复用例等待由 15ms 改为 60ms，以越过其 100ms TTL 对应的新半周期观察窗口，未移除恢复与读取断言。

原始验证日志位于 `/var/tmp/cellhive-peer-recovery-20261124/`。已从独立发布分支部署 SG/USW/GZ，源码 `892c9797`；未更新本地运行服务。同批实现及上一轮诊断的 Helm/doc 改动随本次主分支提交收录；发布分支此前已提交并推送，主分支本次不推送。

最终验证：`make test`、`make build`、`make vet` 全量通过；`go test -race`（项目 SQLite tags）覆盖 internal/lease 与 internal/recovery 通过。`git diff --check` 通过。对应 test.log、build.log、vet-final.log、race-final.log。线上版本、采样及验证见 [发布记录](2026-11-24-peer-recovery-release.md)。
