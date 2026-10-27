# COS 异步日志合批（2026-10-27，本地未发布）

## 改动与边界

COS uploader 周期由20ms改为200ms；64项/1MiB阈值、scope/epoch分组、持久化spool、同步bucket确认和退出Flush不变。follower fsync仍使用独立2ms窗口。修复独立wake/item channel的选择竞态：等待项出队后重新发出wake，避免同步等待者错过唤醒。

这提高已经获得follower确认的写入的后台合批机会，不改变确认条件；不能把单节点顺序bucket确认的未来请求提前合批，也不减少LTX逻辑段数量。代价是正常空闲时COS副本排队窗口增加至最多约200ms，网络/积压/重试另计。

## 单元与构建

- 回归 `TestWaitItemRearmsFlush` 修复前失败：`wait item stranded until periodic flush`；修复后通过。
- `go test -race ./internal/upload -count=3` 通过（10.132s），覆盖错过唤醒、间隔写合批、队列满回压、spool重放、Flush失败保留记录等。
- `make build`、`make test`、`make vet` 退出0。
- 带项目SQLite tags的replica/sqlcapture测试通过。

## 本机双agent + 真实HK COS性能

前后均使用 `hk-review.py --suite performance-dual`，1KiB值，owner/转发/Worker三入口，1/8/32并发，两轮。所有请求成功，应用删除HTTP200。Wrangler额外性能测试因未设置CLI路径跳过，不计入本验收。

| 指标 | 原20ms | 新200ms |
|---|---:|---:|
| 主agent成功上传批数（全进程） | 213 | 114 |
| 上传逻辑段数 | 892 | 887 |
| 平均段/批 | 4.19 | 7.78 |
| 采样时spool积压 | 0 | 0 |
| 第二轮顺序owner PUT p50/p99 | 5.731/7.955ms | 5.515/8.222ms |
| 第二轮顺序owner GET p50/p99 | 0.283/0.488ms | 0.292/0.495ms |
| 第二轮顺序Worker PUT p50/p99 | 7.742/9.927ms | 7.891/9.826ms |

上传批数约减少46.5%，热请求没有增加200ms等待。批数是uploader成功AppendBatch计数，包含控制cell，不是桶List的对象存量。前后capture合并粒度会变化，逻辑段数并非请求数。

旧二进制SHA256 `91c7017e7f56cdb95e622241ba478950d8467fc1ee84985c4c13ddf1dbc3f94d`；新二进制 `18ab2cb463920735ec5e7d7c289c15173c918a4949f02a3ce4e3ea946d50d900`。

限制：新性能轮期间有本地完整测试和另一恢复套件的短暂启动，不能据此宣称尾延迟/RPS改善。转发仍有约百毫秒至850ms尖峰，未在本次消除。另一套件因共享平台控制cell但root key不同返回401，未创建app；该轮排除。HK harness已加跨进程排他锁，防止后续同类并发污染。

## 恢复验收

使用新 `hk-review.py --suite batch-recovery`：等待真实fleet确认，顺序写100个不同key后SIGKILL owner；survivor接管并逐个校验；优雅flush后移除测试peer的cells目录，重启并从COS冷恢复，再逐个校验。真实HK COS串行轮通过：

- 100次新写全部以fleet确认（101−1），bucket确认计数保持13不变。
- 写阶段97段经8次上传提交；强杀前仍有4个spool项，确实覆盖未完成COS后台复制的窗口。
- owner SIGKILL后，survivor接管并读回100/100正确值。
- survivor flush、删除SQLite缓存、重启后，再次读回100/100正确值；应用清理HTTP200。
- 首个冷GET为1815.432ms（ready之后计时）；hydrate日志113个逻辑段、1.110s。这证明合批格式可恢复，但冷请求仍超过1s，未证明冷恢复SLO已经满足。

串行轮日志 `recovery.log`，before/after计数保存在记录目录。

日志位于同名目录；原始进程目录留在工具输出中的scratch路径。此次未提交、未推送、未部署；HK冷恢复的既有>1s问题不能视为全面解决。
