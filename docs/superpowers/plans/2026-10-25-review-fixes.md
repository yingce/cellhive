# Cross-module Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 修复审查 F1–F9，提供可重复回归、性能证据与真实 HTTP 验收。
**Architecture:** 保留现有 owner/epoch/RPO=0 协议。限制资源分配、恢复共享等待的取消能力、消除确定性的错误分支；R2 先核实对象与 sidecar 的版本边界，禁止将不匹配的元数据暴露为当前版本。
**Tech Stack:** Go、SQLite、workerd、现有 Helm/Compose、GitHub Actions。

## Global Constraints

- 主实现工作区：/opt/cellhive/.worktrees/review-fixes-20261025，基线 213a012。
- 设计契约以 docs 为准；同步 docs/decisions.md 和 docs/known-issues.md。
- 不放宽持久证明、owner/epoch 校验；不增加请求热路径 List。
- 不新增外部依赖，不修改 workerd fork，不部署生产。
- 先跑新增失败回归，再修复；共同验收后记录，未得到新指令前不提交发布。
- Go tags: sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook。
- 独立模块由不同 agent 实施；root 负责 cellstore、文档、集成复审。

## Task 1: Queue allocation safety / F1

Files: internal/queue/runner.go, internal/control/store.go, internal/server/cloudflare_api.go；相应单测。
Interface: queue.MaxConsumerConcurrency 常量（100）；runner 防御存量超额配置，两个配置入口使用同一上界。
- [x] 新增部署超额值拒绝、边界值接受测试；runner 输入 MaxInt 不进行大分配、空队列可安全返回。
  `if _, err := store.Deploy(ctx, ns, worker, specWithHugeConcurrency, actor); err == nil { t.Fatal("accepted oversized concurrency") }`
- [x] 运行新增用例确认旧实现失败。
- [x] 入口校验和运行时 clamp：`conc = min(max(ref.MaxConcurrency, 1), MaxConsumerConcurrency)`；避免依赖不可信配置直接分配。
- [x] 包测试、相关真实 HTTP consumer 配置回归。
- [x] root 复审并记录。

## Task 2: JWKS refresh control / F7

Files: internal/auth/jwks.go, internal/auth/auth_test.go。
Interface: 保持 Key(kid) 签名；刷新节流独立于命中 key，失败后短暂退避；不建立无限随机 kid map。
- [x] 用 httptest JWKS 测试顺序未知 kid 请求、刷新失败退避和轮换后新 kid 恢复。
  `for range 20 { _, _ = keys.Key("unknown") }; if calls.Load() > 1 { t.Fatal("refresh amplification") }`
- [x] 先失败，再实现最小刷新间隔（不超过配置 TTL），保留正常过期刷新与已有 stale 行为。
- [x] 运行 auth 全量/race，确认新 key 可在节流窗口后加载。
- [x] root 复审并记录。

## Task 3: R2 metadata correctness / F4

Files: internal/r2/r2.go, internal/server/r2.go, 对应测试与必要 metadata helper。
Interface: 对外 Stat/GET/HEAD 元数据必须与实际对象版本匹配；无 metadata 覆盖不得返回旧 metadata；现有 API 保持。
- [x] 新增先带 metadata 写 5 字节、再无 metadata 写 16 字节回归，HEAD 应返回 16 和新 ETag。
  `if got.Size != 16 || got.ETag != currentETag { t.Fatal("stale metadata") }`
- [x] 增加相同内容覆盖、sidecar 写失败、并发版本错配测试。
- [x] 在确认失败后修复覆盖失效和版本校验；不以 sidecar 覆盖权威 size/ETag；错误不可默认为成功。
- [x] 若同内容并发 metadata 原子性需要新存储协议，明确设计边界，不暗称完整原子；将兼容迁移方案作为单独决定。
- [x] R2 包及真实 HTTP HEAD/GET/range 回归，root 复审。

## Task 4: Runtime router lock / F2

Files: internal/userruntimepool/router.go, router_test.go。
Interface: hostView 保持返回类型，flight 等待后重新进入加锁判定。
- [x] 用子进程复现 flight 完成但缓存已过期，父测试要求正常退出；旧实现 fatal 应导致失败。
- [x] 修复为循环重新查询：`case <-wait: continue`，避免从无锁状态掉入临界区。
- [x] 覆盖请求取消、过期重查和并发单飞；run/race package。
- [x] root 复审。

## Task 5: Cellstore pools, cancellation, bookkeeping / F3 F6 F8

Files: internal/cellstore/cellstore.go, resource_generation.go, internal/server/scope.go, server.go, resource_cleanup.go；新增 cellstore/wait_test.go。
Interface: 新增 BeginRequestContext(ctx, key) (func(), error)，保留 BeginRequest 兼容 wrapper；所有生产请求调用迁移；等待同 cell 可取消，不取消持有者。
- [x] 在既有交互池测试中开启默认 HydrateTimeout=30s，证明后台不占交互池。
- [x] 增加同 cell cold open/eviction 的取消等待测试和 busy entry 归零删除测试。
  `ctx, cancel := context.WithCancel(context.Background()); cancel(); _, err := store.Cell(ctx, scope); if !errors.Is(err, context.Canceled) { t.Fatal(err) }`
- [x] 在内部添加 timeout 前确定工作类别；hydrateSlot 接受显式类别，避免依据包装后的 ctx 再推断。
- [x] 实现带 context 的 cond 等待（AfterFunc 唤醒，谓词在同锁内检查）；上下文取消不启动新请求、不继续打开 cell。
- [x] 请求计数变零时 delete；迁移 HTTP/resource generation gate，按既有错误约定返回。
- [x] cellstore/server 单测与 race；确保失效/驱逐仍遵守 in-flight 请求保护。

## Task 6: D1 linear result accounting / F5

Files: internal/d1/d1.go, 新增结果预算测试/benchmark。
Interface: 保持 Query/Exec/Batch；结果预算显式考虑字符串、BLOB 转换和数值；线性累计，不遍历此前所有结果。
- [x] 加总量/BLOB 超限与取消回归；复用 /tmp 诊断 SQL 形成 benchmark，记录旧实现数据。
- [x] `resultBytes += rowBytes` 使用最终表示计量；在分配扩展 BLOB 前执行预算校验。
- [x] 跑 D1 全量/race；HTTP 执行 20k 行与真实 result 读取，记录方法、样本数与局限。
- [x] root 复审。

## Task 7: CI tags / F9

Files: .github/workflows/ci.yml。
- [x] 复现 `go test ./internal/cellstore -run '^$'` 因缺少 tags 失败。
- [x] workflow 改用 `make vet`、`make test`，不复制第二份 tags。
- [x] 执行这些命令验证；不为 YAML 文本写自证式测试。
- [x] root 复审。

## Task 8: Integration and acceptance

Files: docs/decisions.md (ADR-292), docs/known-issues.md, docs/archive/project-review-2026-10-25.md，独立验收记录。
- [x] 逐项核对实现/契约/测试证据；并发 race 覆盖修改模块。
- [x] `make test`、`make vet`、`make build`、`make js-test`。
- [x] 本地真实 TCP HTTP 验收 D1/R2/Queue 配置；运行相关 bindings/queue E2E，所需临时实例隔离、不复用生产 namespace。
- [x] 整体 reviewer 检查 diff，处理实际缺陷后复验。
- [x] 完成文档和计划状态，报告是否仍有未覆盖边界；保持代码可供用户后续提交发布。

## 后续优化阶段（本次先规划，不混入 bug 修复）

- Queue：同队列 ack/retry 批量事务 + 单次证明，独立验证 DLQ 幂等、失败后重投。
- 调度：跨 scope 有限并发、公平性、队列延迟指标；保持同 scope 防重/持久性。
- DO：地址变更/旧连接真实 E2E 定位，自愈与有界并发续租；不能依赖 ready 失败就集体重启。
- 容量：SQLite soft cap 外加 RSS/FD admission 与冷恢复预算。
- Cron 补跑：单独设计 misfire 策略，不更改现有 best-effort 契约。

验收完成：见 [修复记录](../../archive/project-review-fixes-2026-10-25.md)。代码保留在上述工作区，未提交、未部署；后续优化阶段尚未实施。
