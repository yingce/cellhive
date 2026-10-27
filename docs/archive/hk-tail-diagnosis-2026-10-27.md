# HK 双 agent KV 长尾诊断（2026-10-27）

## 结论与边界

真实 HK COS、同机双 agent、HTTP keep-alive、1 KiB value、32 keys。采集 100% OTLP trace，六轮交替并发 1/8，每轮 direct PUT 64、等待 2.2s、forwarded PUT 64、forwarded GET 64。

正式请求 1152，错误 0；两节点提交计数增量 fleet=449、bucket-batch=0（计数为提交批次，不是请求数）。预热与首写另计。本次证明 58–150ms 长尾主要在请求前置元数据查询，不能归因于写日志同步 COS。没有复现上一轮 400–744ms 极值，因此其全部原因尚未确认。

## Trace 证据

| 请求 trace | HTTP 总耗时 | 主耗时 | owner 持久化确认 |
| --- | ---: | --- | ---: |
| f456e7c72a08595c16e3d4e2a54433a5 | 150.410ms | peer binding refresh 66.313ms，fence GET 17.609ms，另一次 owner GET 31.320ms | 8.040ms |
| f60735e97a3bfba0b0fd893939a0324f | 57.755ms | owner binding refresh 34.991ms，fence GET 15.273ms | 6.266ms |
| fe50908089ad15a362e479b4698de552 | 75.980ms | peer binding refresh 56.934ms，fence GET 11.858ms | 5.568ms |
| 1e1d11ec7b35ed8da5e3fd48e1188f10 | 77.219ms | peer binding refresh 58.465ms，fence GET 11.621ms | 5.888ms |

Span 有嵌套，不能把所有 span 求和。第一条 owner HTTP 总耗时仅 8.467ms；binding refresh 包含 control owner 查询。尚未独立覆盖的部分可能包含 lease 查询等，不把剩余时间全部算成网络转发。

预热曾出现 362.987ms 请求，其中 binding refresh 321.212ms、持久化确认 40.534ms；此时尚未确认 fleet ready，不能混入正式双节点结果。首写 882.851ms 也单独排除。

## 代码原因与待验证假设

- `internal/server/scope.go:bindingInfo`：默认 binding TTL 为 1s，过期同步刷新；`refreshBindingInfo` 没有按 binding 合并并发查询。
- `bindingFromOwner` 每次刷新都 `Invalidate(control.Scope())`，因此即使 control owner 未变也强制重新查桶。该逻辑涉及 handoff 后鉴权正确性，不能直接删掉。
- `internal/server/kv_access_warm.go`：活跃集合只保留空闲不超过 2s 的最多 64 个 cell；刷新 binding 完成后才预热 fence/owner。数据 SQLite 仍驻留，不保证这些元数据仍热。
- `internal/owner/route.go:ResolveRoute` 会在 cache revision 改变时重试。并发 binding refresh 会反复 invalidation，存在放大控制面查询的条件；本轮 trace 尚不足以证明上一轮 744ms 就由此产生。

建议首先合并同一 binding 的并发刷新（保留 revision 防旧结果回填），随后研究经接收端 owner/epoch 校验的 control 路由复用，避免每次刷新都无条件清 owner 缓存。调整预热窗口只能减少低频首请求抖动，不能替代资源删除 fence 与授权失效保证。应补并发过期、handoff、删除重建、旧 owner 响应测试，再复跑长时双节点压力。

## 测试退出异常

旧 harness 使用 SIGKILL，曾留下未封存 session，下一次启动 control 恢复因旧 follower 端口不可达而失败。通过重启保留数据的旧 follower 并正常 handoff 恢复。

本次 harness 改为 runtime 先退出、agent SIGTERM 等待，但依然不能宣称清理全通过：app DELETE HTTP 200，三进程均已退出；peer `handoff complete released=1`，owner 则在 SIGTERM 约 6.2s 后记录 `node lease permanently fenced; exiting without handoff`。

源码中 lease publish 使用被 SIGTERM 取消的 ctx，watchNodeLease 却使用独立 Background ctx；长时间关闭过程中 lease 过期，watchdog 可 os.Exit(1) 抢断 handoff。这是另一个可靠性风险，需要单独修复和验证，不能简单关闭安全 fence。保留数据目录，不清除恢复所需文件。未观察到本轮请求错误，但未执行本轮退出后的完整恢复验证。

## 复现与产物

```bash
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 \
  --suite performance-tail --trace-endpoint http://127.0.0.1:28480 --port-base 28400
```

需要预先运行本地 OTLP HTTP collector。结果与慢 trace：`hk-tail-diagnosis-2026-10-27/requests.jsonl`、`slow-spans.jsonl`。完整临时数据：`/opt/.hermes/cache/scratch/cellhive-e2e.zL0xSb`；采集器与原始 traces：`/tmp/cellhive-tail-diag/`。测试退出码 0；harness Bash/Node/Python 语法检查通过。未修改生产协议逻辑，未提交或部署。
