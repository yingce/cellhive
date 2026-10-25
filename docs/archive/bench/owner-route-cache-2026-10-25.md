# Owner 路由缓存优化验证（2026-10-25）

实现 ADR-290，未提交、未部署。普通 server 路由、capture Owner callback、queue OwnerGate 和热 KV 预热使用 ResolveRoute。Resolve 和 ResolveCached(ttl=0) 的权威点查语义保留。

## 行为

- 本进程同 session 的 node-lease owner：在连续观察到的安全租约窗口内随本机续租延长缓存，不按 1s 查询 owner；越过已观察期限，即使本机租约后来恢复，也重新解析。
- 远端 owner：缓存到本次观察的节点租约截止时间，墙上时间和单调时间双限，不因后续访问或远端续租无限延长旧 cell 路由；转发存活判断复用该条路由的租约证据。
- 同 cell 并发查询合并；等待可取消；leader 取消不强制取消其它等待者；显式失效 revision 同时阻止迟到结果回填与返回。延迟的本机 owner 读取跨过失租/接管/续租恢复时重查。
- claim/release 的失效同时清理旧 TTL 缓存与新路由；续租/释放发现 epoch 或 node 不匹配也撤销路由。路由最多保留 4096 条，超限淘汰一个提示只增加下次点查，不影响 ownership。
- Legacy/DO 记录保持最多 1s 的短缓存。没有把 OwnedScopes、已打开 SQLite 或 follower 副本当作授权依据。
- bucket CAS、接管恢复、输出门和持久性协议未放宽；不新增请求重放或热路径 List。

## 复现及回归

`internal/owner/route_test.go`：初始短 TTL 实现下，间隔 1.1s 的两次本地/远端查询各产生 2 次 owner GET；64 并发 miss 出现重复 GET，测试失败。新实现期望各为 1 次。

另覆盖：本地连续续租跨越最初期限不再查 owner、过期证据不能随恢复的租约复活、查询中失效拒绝迟到结果、查询挂起期间租约过期并接管、release/重新 claim 立即改变路由、等待者取消、远端期限不滑动、墙上时间到期、旧 session 拒绝、过期节点快照以 fresh 点查确认。既有 server/queue 接管及旧副本回归同时运行。

`internal/server/owner_route_http_test.go`：同一 Go 测试进程内、独立 HTTP listener 的 A/B 两个 Server，真实 TCP HTTP、独立 SQLite、共享本地文件桶、capture + bucket 提交。覆盖 owner 和 B→A 的 PUT/GET、内容校验、跨 1s 间隔查询次数，以及 A 释放后 B 取得更大 epoch、写入新值、A 虽保留旧 SQLite 仍转发读到 B 新值。此交接检查不等于真实 COS 灾难恢复或跨机器 fleet 验收。

## 可复现性能对照

对 owner.json GET 注入 15ms 延迟；绑定缓存设为 30s，仅为隔离 owner 解析成本。默认绑定鉴权仍可能强制刷新 control owner，资源 fence/持久性等独立开销也保留。不能把此实验描述为所有业务请求都不再访问 COS。

旧实现对照使用临时 Go overlay，使 ResolveRoute 回到 ResolveCached(1s)、LiveRoute 回到 Live；没有覆盖仓库代码。旧实现因查询次数断言而预期失败。

每隔 1.1s，依次 owner PUT/GET、转发 PUT/GET，共 3 轮：

| 入口 | 旧实现 PUT | 新实现 PUT | 旧实现额外 owner GET | 新实现额外 owner/node GET |
|---|---|---|---|---|
| A owner | 19.38 / 20.30 / 20.06 ms | 4.00 / 3.93 / 3.98 ms | 3 | 0 / 0 |
| B 转发 | 20.60 / 20.28 / 20.75 ms | 4.87 / 4.13 / 3.62 ms | 3 | 0 / 0 |

GET 紧随 PUT，因此旧实现 GET 已被 PUT 预热；此对照不声称 GET 同样降低 15ms。单独 owner 单元测试覆盖间隔访问缓存命中。

随后每种入口/方法顺序请求 200 次，新实现结果：

| 入口 | 方法 | p50 | p99 | 顺序 RPS |
|---|---|---|---|---|
| owner | PUT | 3.94 ms | 8.02 ms | 235.9 |
| owner | GET | 0.204 ms | 0.754 ms | 4291.9 |
| B→A | PUT | 4.17 ms | 6.54 ms | 235.1 |
| B→A | GET | 0.442 ms | 0.866 ms | 2099.3 |

这是同机回环、文件桶、单并发的结果，非 SG/USW、真实 COS、fleet follower fsync 或最大吞吐。连续热访问旧实现本来多数命中，因此该优化的主要收益是减少周期性查询和并发 miss，而非显著提高所有请求的稳态 RPS。

## 验证命令

- `make test`（包含项目必需的 SQLite tags）。
- `make vet build`。
- `go test -race -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/owner ./internal/lease ./internal/queue ./internal/server -count=1`。
- `go test -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/server -run '^TestOwnerRouteHTTP$' -v -count=1`。

最终结果：以上新实现命令全部退出 0；全量测试通过，owner/lease/queue/server race 通过，vet/build 通过。旧实现 overlay 对照按预期因查询次数断言失败。未部署，未运行真实 COS/跨机器 fleet 性能验收。

后续 ADR-291 补齐实际节点失租后的不可逆隔离和条件续租；原路由优化继续保留，完整过程见 [续租连续性验证](../lease-fence-2026-10-25.md)。
