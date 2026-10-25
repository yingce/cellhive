# 节点租约连续性修复与验证（2026-10-25）

对应 ADR-291，与 ADR-290 owner 路由优化一起提交为 `520b61b`，已发布本地、SG、USW。

## 原问题与修复

原 `lease.Manager.Publish` 无条件 Put；即使旧节点实际租期已经结束，迟到的成功返回仍可更新 publishedExpiry，恢复 Healthy。旧续租写也可以覆盖新 session 的节点记录。

修复：

- 首次发布点读节点记录，条件创建或按读到的 ETag CAS。同一个持久 instance 的重启允许替换其前任，由生产启动的数据目录锁保证排他；其它 live instance 不可覆盖。
- 正常续租使用上次成功写入的 ETag CAS，不额外 GET，不增加每 cell 续租。默认 TTL 10s、约 3.33s 续租一次不变。
- 在发请求前、返回后及健康检查中检查真实发布期限；单调时间或墙上时间到期即不可逆 fenced。迟到成功不能复活，CAS 冲突也永久隔离。
- TTL/5 的安全余量与实际失租分开：余量内暂停服务，但真正到期前完成续租仍可恢复健康；一次普通网络写失败不会立即永久隔离仍有效的租约。
- 独立 watchdog 默认每 100ms 检查（更短 TTL 使用 TTL/10），不被挂起的 bucket I/O 阻塞。真失租后非零退出，跳过 snapshot/release/handoff，由新 session 启动恢复。正常排空期间 watchdog 仍运行，不能因收到退出信号而取消失租保护。
- 初次发布未得到有效租约则启动失败。模糊写结果不猜测成功；若写实际成功但响应丢失，后续 ETag 冲突会保守退出，可能增加恢复次数，但不覆盖另一个进程的租约。

## 回归证据

新增测试在原实现上先失败：

1. 已过期的 session 重新 Publish 成功。
2. 正在等待的续租跨过原期限，返回后恢复健康。
3. 旧续租覆盖期间写入的新 session。

修复后全部通过，并补充：独立健康/fence 检查不被挂起写阻塞；fence 不可撤销；安全余量内续租可以恢复；普通失败在实际租期内可重试；watchdog 调用退出回调。owner 路由的延迟查询测试同步要求失租后 Publish 返回 ErrFenced、迟到路由不得授权。

既有测试夹具调整：恢复测试直接构造历史过期节点记录（不再通过负 TTL Publish 创建）；Tail 节点重启测试显式携带相同持久 instance、新 session。

## 真实双进程 E2E

命令：`bash scripts/e2e/session-recovery.sh`。

使用临时构建的真实 cell-agent 二进制、隔离目录、本地文件桶、真实 HTTP/peer 协议，租期 3s。结果 PASS（11.76s）：

- 原有双地址变化、启动恢复中断重试、错误空磁盘替代拒绝、原 follower 磁盘恢复仍通过。
- 80 个已经确认的值恢复，旧 pinned control 注册恢复，旧 session 的 node log 被正确封口。
- 找到实际 KV owner，SIGSTOP 整个进程 3.3s，另一进程通过真实 GET 完成恢复接管，epoch 增长，读回已确认值。
- SIGCONT 原进程，确认以退出码 1 结束，日志包含 permanently fenced；不能恢复旧身份。
- 同磁盘新 session 重新启动，ready 后通过 HTTP 读回已确认值。

以上故障注入使用本地文件桶，不代表实际 COS 网络故障验收。生产滚动更新与数据验收见下。

## 检查

- `make test`：通过（首次运行发现上述两个夹具，修正后全量通过）。
- `go test -race -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/lease ./internal/owner ./internal/recovery ./internal/server ./internal/queue ./cmd/cell-agent -count=1`：通过。
- `make vet build`：通过。

所有运行日志位于 `/tmp/cellhive-lease-*.log`；关键场景、命令和结果保存在本记录及仓库测试中。


## 发布验收（2026-10-25）

- 代码：`520b61b`（已推送）；镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:5bc84b3ab55c501d386bdef2448a3237c7635520505194dd8b3d9ae3ea9e6a25`。
- 本地 Compose：cell-agent、user-runtime、do-runtime 均 healthy，镜像核对一致，重启计数 0；保留现有数据及 MySQL/ProxySQL。
- SG：通过运维仓库 `bin/cellhive-helm sg upgrade` 发布，Helm revision 59 deployed。
- USW：通过同一 wrapper 发布，Helm revision 48 deployed。
- 两地各 6 个业务 Pod 的 spec image、imageID、Ready 均核对通过，最终重启计数均为 0。此计数不包含滚动替换和下述手动重建的旧 Pod。
- 三处均读回升级前 KV 值，写入并读回新值；两地分别经过两个 agent 入口。
- 三处 DO 验收均通过：升级前持久化值、二进制 00ff8041、headers、两条 cookies、独立 DO 的 deleteAll 300 keys、302 redirect。
- 最后检查两地 4 个 agent 最近 3 分钟日志，未见 lease publish failed、permanently fenced 或 WARN/ERROR。

### 发布中观察到的异常

USW 的 do-runtime-gated-1 在 agent 地址变化后持续返回 ready 503（cell_unreachable），但同容器直接请求 cell-agent readyz 返回 200。疑似 workerd 连接或地址缓存未刷新，根因尚未确认。仅重建该未就绪 Pod，保留 PVC，随后 Ready 且上述 DO 持久化验收通过。该运行时重连问题仍需单独跟进，不能以本次恢复视为根因修复。

SG DO 验收期间出现恢复指针 CAS 竞争、回退等待 bucket 的警告，请求最终成功。本地启动时一次 queue runner 的 database is closed 警告未在后续观察中重复。

本次是滚动发布及功能、持久化冒烟验收，未在生产注入暂停/网络故障，也未做生产稳态吞吐压测；首次请求包含冷恢复，不作为性能提升证据。构建与发布日志在 `/tmp/cellhive-291-*.log`。
