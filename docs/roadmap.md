# 路线图

目标：以**最小闭环优先**推进，先验证两个根本假设，再逐层补齐。

## 两个根本假设（全程风险）

1. **stock workerd 的 DO/WAL 可观察性**：actor SQLite 是 WAL、可被外部只读、checkpoint 可对齐、DO 文件布局可复制。
2. **自研 cell 复制协议的正确性**：owner 条件写、epoch fence、ensemble、node-log recovery、RPO=0。

若假设 1 失败 → 退 FUSE（自管）或周期快照（降 RPO）。

---

## P0 — 可行性 spike（先做）

**交付**
- 最小 Go `cell-agent`：单 cell SQLite + bucket 条件写 owner/lease + epoch + `diagnose`；
- 最小 workerd host adapter：`workerLoader` 加载一个示例 worker，binding 走 cell-agent；
- **WAL 捕获 spike**：supervisor 只读 workerd actor SQLite，捕获并上报；
- 双节点 fleet 最小闭环（owner + 1 follower ensemble）。

**退出标准（性能门）**
- workerd WAL/checkpoint/文件布局验证结论；
- `kill -9` 接管 RPO=0、≤5s；
- 双节点写 p50/p99 ≤ 30/100ms；单节点降级 ~90ms；
- 跨节点"捕获→cell-agent→证明" p50/p99；
- 热路径 LIST=0；每 ack PUT 数 ≪ 1；
- bucket 供应商条件写/presigned 通过。

## P1 — cell-agent 完整 + 安全/路由

**交付**
- cell 复制完整：LTX/快照/compaction/paging；
- **node-log + recovery**（完整实现）；
- 租约/接管/handoff/drain token；
- 计时器（统一定时抽象）+ 派发 + 单 waker；
- owner 解析库 + 端点；
- 控制面骨架（应用/版本/路由/密钥，按 app 分片）；
- **security.md 落地**：租户网络隔离、scope 声明；
- **routing.md 落地**：路由投影（纯拉取 5–10s）。

**退出标准**：单/双节点故障注入稳定；控制面发布/回滚闭环；多租户隔离验证。

## P2 — 绑定 + Wrangler 兼容 + 资产

**交付**
- KV/D1/R2/Queue/Cron 的 binding facade + `cell-agent` API：✅（KV/D1/R2/Queue ADR-063；Cron ADR-070/076 + deploy 校验 `invalid_cron`）；**Workflows ✅ Partial（ADR-086）**。
- Go+esbuild 打包 + jsonc/toml 解析 + 严格拒绝 + 契约测试；
- **完整资产管道**（上传/版本化/_headers/_redirects/worker-first/ETag）：✅ 读侧（ADR-069/071）+ **写侧**（Go CLI `asset put`/`bundle put`/`deploy --assets-*` + `/v1/control/asset|bundle`；ADR-062）。
- CLI：deploy/dev/tail/secret + 资源 create 命令；
- `cellhive dev`（内置文件存储 + 单节点 + 热重载）。

**退出标准**：CF 兼容套件通过；框架/Vite 预构建产物可部署。

## P3 — DO 完整

**交付**
- do-runtime（原生 facet + supervisor）：**facet ✅（ADR-077）；supervisor/捕获/门 ✅（ADR-083）**；
- DO claim（cell-agent 驱动）+ 冷激活 **分页懒加载**：✅ claim（ADR-078/080）+ 冷激活/恢复/按需分页/按对象冷启动（ADR-084）；
- **alarm**：✅ 功能（ADR-079，机制改 shim + 统一 timer/waker）；
- **WebSocket CF 兼容**：✅ 连接+1012（ADR-080）+ **跨节点转发/1012 透传（ADR-084）**；
- in-flight 迁移 `result_unknown`：✅（ADR-080）；
- **输出门 + RPO=0 跨节点**：✅ 门（ADR-083）+ 跨节点冷激活/接管（ADR-084，对象粒度）；◐ 真实多主机/云验证未做（C 类环境）。

**P3 收口（2026-09-15 全部完成）✅**
- HTTPCommitter 证明放宽为 `fleet`/`bucket`/`bucket-batch`（拒绝 `bucket-async`）✅；
- **WS 跨节点转发** ✅（`proxyConnect`，1012 透传）；
- **`transferred_classes` 同 worker** ✅（= rename 别名；跨 worker `script_name` 拒绝）；
- **运行期 VFS 懒读** ⛔ DO 侧边界定稿（ADR-085）；ADR-159 后 cell-agent 侧 CGo 自定义 VFS 技术可行但未实现；替代=冷启动按对象/按页 materialize ✅；
- `refreshFacets` 增量缓存 ✅；`deleteAll()` shim（KV+SQL）✅。
- 剩余仅 **C 类环境验证**（真实跨主机 RTT、云端对象存储条件写、多主机接管）缺环境，如实记 blocker。

**退出标准**：DO 兼容套件（同步 SQL/事务/alarm/WS/迁移/eviction）：✅ **`TestDOCompatSuite` 统一入口，19 子测试全 PASS**（ADR-084）。

## P4 — 多租户 + 伸缩

**交付**
- 控制面完整（审计、发布日志、幂等）：✅ 审计（ADR-062）+ 发布日志 `Releases` + 幂等部署 `idempotency_key`（ADR-087）；
- CLI 全命令面；admin 后台：✅ Go CLI 覆盖全部 admin 端点（+`status`/`capacity`/`releases`/`workflow create`）；admin 后台=`GET /admin` 单页控制台（stdlib，无依赖）+ admin JSON API（ADR-036）；
- autoscaler（读节点 lease 信号）：✅ `internal/autoscaler.Advisor` + `/v1/control/capacity`（**信号**；实际编排属外部，C 类环境）；
- **配额/admission（ADR-035）**：✅ 每命名空间 token bucket + 写端点 429（ADR-087）；
- publish/rollback：✅（ADR-060，已有）。

**退出标准**：多租户端到端；扩缩容与迁移无数据损失。

## P5 — 运维与加固

**交付**
- `diagnose`、observability、升级/回滚流程：✅ `/v1/diagnose` 增强（proto/admission/capacity/计数）+ `/metrics`；
- 协议版本化落地（reader-before-writer）：✅ `cell.SupportedProtoVersion` + claim 握手 fail-closed（ADR-088）；
- 混沌测试、压测、供应商矩阵回归：✅ 恢复混沌测试（ADR-057/066）+ `cmd/*bench` 压测 + `TestVendorMatrixContract`（ADR-088）；真实多主机/云矩阵需环境（C 类）；
- 文档补全与发布：✅ glossary 存在 + `docs/release-notes.md`。

## 持续事项

- 跟随 pinned workerd / Wrangler 语义基线升级（显式动作 + 契约测试）；
- `known-issues.md` 的未验证项随 P0/P1 收敛。

_最后更新：2026-09-16_
