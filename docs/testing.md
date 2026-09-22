# 测试策略

## 分层

| 层 | 范围 | 方式 |
|---|---|---|
| 单元 | cell 协议逻辑、owner/epoch、timer 去重、路由解析 | Go 单测 |
| 契约 | Wrangler 语义基线、内部 REST/绑定 schema | 固定基线/契约测试 |
| 集成 | 真实 workerd + cell-agent + 对象存储（MinIO/本地 stub） | docker-compose 起栈 |
| 端到端 | wrangler 项目 → deploy → fetch/binding/DO/cron/queue | 兼容套件 |
| 故障注入 | 崩溃/分区/接管/checkpoint | 定向脚本 |
| 性能 | P0 性能门 | 基准脚本（wrk/hey + 自定义） |

## 兼容套件（每 binding 一个）

- 每种 CF binding 的 API 行为示例（KV/D1/R2/Queue/Workflows/Cron/DO/ASSETS/service binding）；
- `wrangler` 项目字节级部署 → 运行；
- 边角：大值/大结果、cap、错误码、翻页/游标。

## 复制与一致性测试（核心）

- 双节点稳态写：p50/p99、group commit 合批比；
- **`kill -9` owner**：RPO=0、接管 ≤5s；
- **双节点顺序崩溃**：node-log/recovery 收齐未上传段；
- **分区**：旧 owner 被 fence，不能确认写；
- **checkpoint 截断**：检测 + 快照对齐 + delta 衔接；
- **bucket 供应商矩阵**：条件写 + ranged read + presigned URL。

## DO 专项

- 同步 SQL 语义；事务；`blockConcurrencyWhile`；
- 冷激活/接管（分页懒加载）；
- alarm：本地触发 + 死 owner waker 定向 takeover；
- WS：部署/迁移 1012、客户端重连、handler 重启（CF 兼容）；
- in-flight 迁移 → `result_unknown`。

## 性能门（P0，见 cell-protocol.md §13）

- 双节点写 p50/p99、单节点降级、跨区；
- **跨节点捕获→cell-agent→证明** p50/p99；
- 冷恢复时间（100 MiB）；
- 每 ack PUT 数、条件写冲突率、接管读对象数、热路径 LIST=0。

## 升级/回滚测试

- reader-before-writer 滚动；
- 版本差 ≤1 的回滚；
- 不兼容变更的显式迁移演练。

## 验收矩阵（退出标准 → 证据）

| 阶段退出标准 | 证据 |
|---|---|
| P0 性能门（双节点写/捕获→证明/冷恢复/热路径 LIST=0） | `make perf-test`（`CELLHIVE_PERF_GATE=1`）、`make rpo-test`、[`benchmarks.md`](./benchmarks.md)（原始输出归档 `docs/archive/bench/`） |
| P1 单/双节点故障注入稳定 | `internal/recovery`、`internal/owner`、`internal/drain`、`internal/peer` 单测 + `make rpo-test`（`kill -9` 接管、顺序崩溃、分区 fence） |
| P1 控制面发布/回滚闭环 | `internal/control TestDeployIdempotencyAndReleases`、CLI e2e（`app create`→`deploy`→`releases`）、`TestPromoteRollbackInvalidatesBindingCache` |
| P1/P4 多租户隔离 | `internal/server TestAdminNamespaceAuthorization`、`TestScopedCredentialAuthorizationMatrix`、`TestScopedCredentialForwarding` |
| P2 CF 兼容套件 | `internal/wrangler`+`internal/wranglercompat`（配置面）、`internal/userruntime` 真实 workerd e2e（bindings/assets/workflow/service） |
| P2 框架预构建产物可部署 | ASSETS 管道（`_headers`/`_redirects`/SPA fallback）+ `wrangler.jsonc` 子集解析；逐框架适配用例仍属待细化 |
| P3 DO 兼容套件 | `TestDOCompatSuite` 统一入口（19 子测试，见下） |
| P4 扩缩容与迁移无数据损失 | `internal/autoscaler`、`internal/rebalance`、drain 测试 + `make rpo-test`；**多机编排属 C 类环境** |
| P4 多租户端到端 | `cmd/cellhive TestCLIEndToEndWithWorkerCode`（CLI→控制面→真实 workerd→facade→cell） |
| P5 协议版本化 / 诊断 / 供应商矩阵 | `cell.SupportedProtoVersion` + claim 握手 fail-closed、`/v1/diagnose`、`TestVendorMatrixContract`、`make s3-test` |

## C 类环境：本地替代（已覆盖）与残余（未覆盖）

**本地替代已验证**（最近一次全部 PASS）：

| 目标 | 本地替代 | 证据（命令 / 测试） |
|---|---|---|
| 对象存储条件写/条件删除 | 本地 MinIO（`make s3-test`，docker 不可用则 skip） | `TestS3BucketIntegration`（create/reject/CAS/reject-stale/ranged/**ListPage**）、`TestDiagnoseDetectsConditionalDeleteIgnored`、`cellhive diagnose` |
| 多进程崩溃 / 接管 / RPO=0 | 双进程真实 crate（`make rpo-test`） | `RPO-ZERO: PASS (1200 acked keys all present and exact)`，restoreverify `integrity:ok` |
| 跨节点 RTT / peer 流水线 | 合成延迟注入 | `internal/peer TestLatencyTransportInjectsRoundTripDelay`（`CELLHIVE_PEER_LATENCY` 往返 2×） |
| 慢/轮换/异常的 OIDC IdP | `httptest` mock IdP | `internal/auth TestJWTBearerRS256`、`TestJWTWithoutExpiryRejected`、`TestJWKSServesStaleDuringRefresh` |
| owner 时序/分区仿真 | 确定性模型 | `internal/owner`（`sim_test.go`）全部通过 |
| 云供应商行为契约 | 契约测试 | `internal/wranglercompat TestVendorMatrixContract` |

**不可在本机验证的残余（需环境）**：

| 项 | 原因 |
|---|---|
| 真实云端对象存储的条件写、`If-Match` 语义与 `ListPage` 延迟 | 无云凭据；S3 兼容实现忽略 `If-Match` 时条件删除退化（`cellhive diagnose` 可自检） |
| 真实跨主机（第二台主机）RTT、跨 AZ 放置、多机混沌与接管时限 | 单机/仿真不能代表真实网络与故障域 |
| 真实 OIDC IdP（真实签名/JWKS 运维轮换/网络故障） | 仅 mock；真实 IdP 未接 |
| 真实扩缩容编排（外部 orchestrator/云驱动） | autoscaler 只产信号，核心不含云驱动 |
| HTTPS 边缘 + 证书/通配符 | 边缘由运维静态配置（ADR-132），仓库不含证书 |

## Queue per-message ack/retry/delay（ADR-155）
- 单测：`internal/queue TestSendDelayVisibility`、`TestRunnerPerMessageAckRetry`（显式 ack + 显式 retry + 其余隐式 ack）、`TestRunnerRetryDelay`（延迟不可见→过后可见）、`TestRunnerRetryExhaustedGoesToDLQ`。
- JS e2e：`internal/userruntime TestUserRuntimeDispatchInjectsBindings` 断言响应 `ack`/`retry{duration}` 与 `typeof batch[0].ack/retry === "function"`。
- 真实 compose：`ack()` → acked=1；`retry(delay 5)` → retried=1 且 t+6 前未 ack、t+8 以 attempts=2 重投；`send(delay 6)` → t+6 前未消费、t+8 消费。

## 全栈 dispatch e2e（ADR-154）
- 真实 compose `--profile rpo0`：fetch+KV+DO、queue（2 条消息一个批次、无重试）、cron（`cron_last="* * * * *"`）、门 facets、桶内 KV/Queue/Timer/DO 段、cell-agent 0 dispatch 失败。
- 回归：`internal/dispatch TestHTTPDispatcherSendsNamespace`、`internal/userruntime TestUserRuntimeDispatchInjectsBindings`（CF MessageBatch 形状断言）。

## 兼容矩阵收口（ADR-153）
- `internal/bundler TestBuildExternalizesPlatformModules`（`cloudflare:*` 恒外置；`node:*` 仅 nodejs_compat，否则打包失败）。
- `internal/wranglercompat TestKnownFlagsMatchDevCLI`（Go 列表 ↔ `cli/src/validate.ts` 逐项一致）、`TestPinPairsWithCompatibilityDate`（pin ↔ 兼容上限）。
- `internal/wrangler TestFrameworkPrebuiltLayouts`（OpenNext/SvelteKit/Astro 布局 + 打包）。
- D1 sessions：真实 workerd e2e 断言 `withSession()` 报 "not supported"。

## dispatch / waker（ADR-152）
- `internal/waker TestBackoffDelay`（指数退避到封顶、disabled、base=0 回退）。
- `internal/config TestDispatchDefaults`（timer 256/24h、waker 256/24h/1m、queue 0/0/30s + 覆盖生效）。

## scaling / 多 AZ（ADR-151）
- `internal/autoscaler TestAdvisorCooldown`（窗口内动作变化 → `hold/cooldown`，窗口后恢复；Cooldown=0 旧行为）。
- `internal/server TestSelectFollowersPrefersOtherAZ`（跨 AZ 优先 / 无 AZ 按序 / 仅同 AZ 兜底）。

## 根密钥来源（ADR-150）

- 单测：`internal/config TestLoadRootKeyFromFile`（trim、env 优先、缺失文件失败关闭 + Validate 拒绝、ALLOW_INSECURE 兜底）。
- 容器：`-v /run/secrets` + `CELLHIVE_ROOT_KEY_FILE` → `/readyz` ready 且 `cellhive creds internal` 与 env 方式一致；缺失文件启动失败。

## CLI 兼容（ADR-148）

- Go：`internal/server TestDeployDryRun`（合法/非法 dry-run 都不产生版本）；`cmd/cellhive TestTranslateWranglerCompat`（`--dry-run`/`--var`/`--secrets-file` 透传 + compat 子命令转发）、`TestCmdCompatRejections`（结构化拒绝消息）。
- 容器冒烟：`deploy --dry-run`（合法 + 非法 bundle）、`--var`（deploy 响应含 vars）、`--secrets-file`（secret list 带 key）、`triggers list`/`versions list`、`d1 execute` 拒绝。

## secrets 管理（ADR-147）

- 单测：`internal/control TestSecretDeleteAndList`（列表不含密文、删除后 404、跨 worker 隔离、幂等）、`internal/server TestSecretDeleteAndListEndpoints`（HTTP 列表不泄露 value/`wrapped_dek`、审计 `secret.delete`）。
- 容器 CLI：`secret put×2 → list → delete → list` + 审计。

## trace 传播（ADR-146）

- 真实 workerd：`internal/userruntime TestUserRuntimeTraceContextPropagation`（租户看到生成的 traceparent；客户端给定时原样保留）。
- Go：`internal/server TestServiceFetchPropagatesTraceparent`、`TestDOInvokePropagatesTraceparent`（出向 body 带调用方 traceparent）。

## R2 list 分页/尺寸化（ADR-145）

- 单测：`internal/bucket TestFSListPage`（独占游标/有序/末页/越界）、`internal/r2 TestR2ListPaging`（2+2+1、尺寸、桶隔离）、`internal/server TestR2ListCursorPaging`（HTTP `truncated`+`cursor`）。
- 真实 MinIO：`make s3-test` 的 `TestS3BucketIntegration` 含 `ListPage` 断言（`StartAfter` 分页 a..e）。

## service binding ACL / 跨 ns（ADR-144）

- 单测：`internal/control TestServiceACL`、`internal/server TestServiceBindingACLDeployGate`（同 ns 通过 / 跨 ns 无授权 400 `service_binding_denied` / 授权后 200 / 撤销后再 400）、`TestServiceFetchCrossNamespaceACL`（运行期 403 → 200）。
- 真实 workerd：`internal/userruntime TestUserRuntimeCrossNamespaceServiceBinding`（`ns=acme&target_ns=team&worker=api`）。

## async 上传持久化（ADR-143）

- 单测：`internal/upload TestSpoolDefersFailedUploadAndReplays`（失败→deferred，恢复→replayed）、`TestSpoolReplaysAfterRestart`（重启重放）、`TestSpoolFullCountsSpoolError`、`TestSpoolRoundTrip`（编解码/顺序/坏文件跳过）。
- 容器：`/metrics` 有 `cellhive_upload_spool` 等指标、`/data/state/upload-spool` 建立。

## purge 数据侧（ADR-142）

- 单测：`internal/purge`（app 级、worker 级只删自己、`MaxDeletes` 分轮、非法 ns/worker）、`internal/cellstore TestForgetPrefixDropsMatchingScopes`、`internal/control TestRunPurgeLoopResumableHook`。
- 容器端到端：删 `acme/api` → `cells/acme/__do__/api~*` 与 `assets/acme/api/*` 被删，`api2~`/`__kv__`/`cells/other` 保留，审计 `purge.done`；删 app → `cells/acme/*`+`assets/acme/*` 全删。

## 一键门禁（ADR-141）

OTLP/collector 端到端：`bash scripts/otlp-collector-smoke.sh`（需 docker 与 `otel/opentelemetry-collector-contrib` 镜像；起真实 collector + cell-agent + user-runtime + 探针 worker，验 span/log/`ns` 指标，见 ADR-179）。
后端端到端：`bash scripts/openobserve-e2e.sh`（需 docker 与 `openobserve` 镜像；真实 OpenObserve + 仓库参考 collector 配置，验日志/trace 进 OO 且可按 ns/trace_id 查询）。

`make ci`（`scripts/ci.sh`）串起全部检查：gofmt、`go vet`、`go test`、`make build`、`js-test`、`cli-test`、`perf-test`、`rpo-test`、`s3-test`、`docker-build`、`compose-config`、`runtime-baseline-e2e`、`k8s-render`、`helm-lint`；缺工具则 skip（`REQUIRE_ALL=1` 转硬失败）。2026-09-22 现行验收：**GATE: PASS 14/14**；runtime-baseline E2E 末行 `RUNTIME-BASELINE-E2E: PASS`，无实际跳过项。

### ADR-186 runtime baseline 验收

- 聚焦：`go test ./internal/runtimeenv ./internal/workerdcompat ./internal/workerbudget ./internal/wranglercompat -count=1 -v`、`make js-test`、`make cli-test`。
- 真实二进制：`CELLHIVE_WORKERD="$(command -v workerd)" bash scripts/workerd-compat-probe.sh`。
- 最终门禁：`bash scripts/runtime-baseline-e2e.sh` 已以 `RUNTIME-BASELINE-E2E: PASS` 结束，随后 `REQUIRE_ALL=1 bash scripts/ci.sh` 为 `GATE: PASS`；任何实际 `SKIP`/`SKIPPED` 或缺工具仍不算通过（TAP 的 `skipped 0` 仅表示零跳过）。
- E2E 必须验证镜像版本、镜像内 esbuild 源码打包、用户自定义 `CELL_URL`/`CELL_TOKEN` 可见而 host canary 不可见、fetch/KV、gated DO 重启后继续计数、capnp/最终 WorkerCode secret 扫描、code/env 近限探针。

## compose 起栈 smoke（C，ADR-140/153）

`CELLHIVE_ROOT_KEY=<32B> CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788 docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d`：

- `ps`：`cell-agent Up (healthy)`、`user-runtime`、`do-runtime-gated`（+ 无门 `do-runtime` idle）全部 Up；
- `cell-agent /readyz` = `{"status":"ready"}`；user-runtime `:8081` 可连（`/`→404 正常）；
- 门 `GET :18901/status` 可用；
- runtime-baseline 在镜像内执行 `cellhive app create` + TypeScript 源码 deploy；公开 `user-runtime :8081` 的随机 loopback 端口由 Bun 客户端完成真实 WebSocket 101 与双向消息。共享 `Counter` DO 的持久计数为 HTTP `1` → WS `2` → 重启 gated do-runtime → WS `3` → HTTP `4`，并产生权威 bucket LTX。

## 部署产物校验（ADR-139）

不是 `go test` 的一部分，手动/CI 跑：

- `make docker-build` — 镜像构建（含 pinned workerd）；
- `make compose-config` — `docker compose config` 语法/变量校验；
- `make k8s-render` — `kubectl kustomize deploy/k8s`（客户端渲染，免集群）；
- 历史 ADR-139 证据：当时容器内 `workerd --version`=`2026-06-15`，服务可监听。ADR-186 新镜像必须读回 `2026-09-16` 与 esbuild `0.28.2`，并以 runtime-baseline E2E 重新验收，不能沿用历史证据。

## DO 输出门部署（ADR-140）

- 单测：`internal/doruntime TestRenewLoopPostsWithToken`/`TestRenewLoopReportsErrors`/`TestDrain`（续租带 internal token、失败可见、drain 打对端点）。
- 容器端到端（手动，见 ADR-140）：`do-runtime-gated` 起栈 → 门 `/status` 可用、续租失败 0、SIGTERM drain 正常；`/v1/do/invoke` 返回 `count:2..5` 且落桶 `cells/workerd/__do__/<host>.sqlite/ltx/e1/*.ltx`。

## 包级覆盖

`go test ./...` 覆盖 **54/68** 包。无独立单测的是 **14** 个包：

- 服务入口 `cmd/user-runtime`、`cmd/do-supervisor`：由集成/e2e（`internal/userruntime`、`internal/doruntime`、`cmd/cellhive` e2e）覆盖（`cmd/cell-agent`、`cmd/do-runtime`、`cmd/cellhive` 自身有单测）；
- 压测/工具 `cmd/{cellbench,realbench,sqlbench,kvbench,gatebench,s3init,s3probe,walscan,recoververify,restoreverify,cell-supervisor}`：一次性 CLI，靠 `make s3-test`/`make perf-test` 触发；
- `internal/workerdbin`：二进制发现，由 `make js-test`（真实 workerd）间接覆盖。

## 待细化

- CI 流水线与分片（当前入口：`make test` / `js-test` / `cli-test` / `perf-test` / `rpo-test` / `s3-test`）；
- 性能基准的可重复环境（固定机型/内核/workerd pin）；
- 混沌注入工具与场景库（目前靠 `internal/owner` 仿真 + `make rpo-test`）。

## S3 集成测试（本地 MinIO/rustfs）

一条命令即可（无需云凭据）：`make s3-test`（= `bash scripts/s3-integration.sh`）。脚本在未设 `CELLHIVE_S3_TEST_ENDPOINT` 时自动起一个 MinIO 容器（`S3_IMAGE` 可换 rustfs），跑完清理；也可指向已有 S3 端点。

覆盖：`s3init`（建桶 + presign）、`s3probe`（conditional create / reject-create / CAS / reject-stale / ranged read）、`TestS3BucketIntegration`、`TestS3ReplicationRestoreChain`（snapshot→Restore→ApplyFile + Compact→PageFetcher ranged→Materialize）。

```bash
make s3-test
# 或指定已有端点：
CELLHIVE_S3_TEST_ENDPOINT=http://127.0.0.1:9000 make s3-test
```

- `TestS3BucketIntegration`：conditional create / reject-create / CAS / reject-stale / ranged / presign。
- `TestS3ReplicationRestoreChain`：snapshot→Restore→ApplyFile + Compact→PageFetcher（ranged）→Materialize。
- 无 `CELLHIVE_S3_TEST_ENDPOINT` 时自动 skip（CI 默认绿）。

## DO 兼容套件（ADR-083/084）

真实 workerd e2e（`internal/doruntime`），逐步汇总 P3 退出标准的各面：

| 面 | 测试 |
|---|---|
| 同步 SQL / 事务 | `TestDOCompatSuiteSyncSQLAndTransactions`（`ctx.storage.sql` + `transactionSync` + 持久） |
| alarm | `TestDoRuntimeAlarmShim`（set/get/delete + 派发 + 重排） |
| WebSocket 1012 | `TestDoRuntimeWebSocketAbort` |
| 版本重启 / migrations | `TestDoRuntimeVersionRestart`、`TestDoRuntimeClassRenameKeepsStorage` |
| eviction 配置 | `TestRenderResidencyAndDisk`（resident/evictable） |
| owner 转发 / result_unknown | `TestDoRuntimeOwnerForwardAndResultUnknown` |
| 跨节点冷激活 | `TestDoRuntimeCrossNodeColdActivation` |
| 按对象冷启动 | `TestDoRuntimePerObjectColdStart`（只恢复 `storage_id/class/name` → 状态延续） |
| WS 跨节点转发 | `TestDoRuntimeWebSocketCrossNodeForward`（B 代理到 owner A；abort→`CLOSE:1012` 透传） |
| 公开入口→tenant→DO WebSocket | `TestTenantDoWebSocketPassesThroughPublicLoader`（真实 workerd：公开 101 + 双向帧 + 零 tenant 平台键）；runtime-baseline Docker 在 gated do-runtime 重启前后各连一次 |
| DO 内 bindings | `TestDoRuntimeBindingInsideDO`（DO 内 `env.KV` 往返） |
| deleteAll | `TestDoRuntimeDeleteAllCaptured`（KV）、`TestDoRuntimeDeleteAllSQLCaptured`（SQL + 冷启动为空） |
| migrations transfer | `wranglercompat` 同一 worker `{from,to}` 接受、`script_name` 拒绝 |
| 进程级接管 | `TestDoRuntimeTakeoverAfterCrash` |
| 输出门 | `TestDoRuntimeOutputGate` |
| 捕获/恢复 | `internal/dosupervisor`：`TestCaptureRestoreRoundTrip`、`TestParseFacets`、`TestPagedRestoreUsesRangedReads`、`TestObjectScopesAndRestoreObject` |

**统一入口**：`go test ./internal/doruntime/ -run TestDOCompatSuite -v`（19 个子测试全 PASS）。

对象存储原语：`objectstore.Objects`（`TestObjectsPrefixScopeAndSafety`：前缀规范化、穿越/绝对/越界拒绝、round trip、List）；保留前缀属主守卫（`TestReservedPrefixGuard`、`TestReservedPrefixesDisjointAndOwned`）。

热路径：`TestSyncAllSkipsUnchangedBlobs`（稳态 0 PUT）、`TestRefreshFacetsPicksUpNewFacet`（增量缓存失效）；提交证明 `TestHTTPCommitterProofModes`（fleet/bucket/bucket-batch 通过，bucket-async 拒绝）。

P4（ADR-087）：`internal/admission`（`TestLimiterBurstAndRefill`/`TestDisabledLimiter`）、`internal/server TestAdmissionRateLimit`、`internal/control TestDeployIdempotencyAndReleases`、`internal/autoscaler TestAdvisorScalesUpForLoadAndPressure`。

RPC entrypoint bindings（ADR-090）：
- DO 宿主：`TestDoRuntimeBindingInsideDO`（KV，this.env + 形参）、`TestDoRuntimeD1R2QueueInsideDO`（D1/R2/Queue）、`TestDoRuntimeDoInsideDO`（DO→DO）、`TestDoRuntimeWorkflowInsideDO`（Workflow）。
- fetch 宿主：`TestUserRuntimePublicLoaderBuildsBindingFacades`（KV）、`TestUserRuntimePublicLoaderD1R2Queue`（D1/R2/Queue）、`TestUserRuntimeDurableObjectBinding`（DO）、`TestUserRuntimeRunsWorkflow`（Workflow）、`TestUserRuntimeServiceBinding`（Service fetch）、`TestUserRuntimeServiceBindingRPC`（Service RPC）。
- cell-agent 处理器：`TestServiceFetchProxy`（service.fetch scope 校验/投影/派发）。

Workflows（ADR-086）：`internal/workflow` 单测、`internal/server TestWorkflowEndpoints`、`internal/dispatch TestWorkflowDispatcherRunAndSleepTimer`、真实 workerd `TestUserRuntimeRunsWorkflow`（step 记忆化 + sleep 重派发）、`wranglercompat TestWorkflowBindingRequiresClassName`。

无桶凭据恢复：`TestRestoreViaAgentNoBucketCreds`（`internal/dosupervisor`）+ `TestInternalBlobRoundTrip`（`internal/server`，`/v1/internal/blob` 鉴权/前缀/穿越）。
agent 路径按需分页：`TestAgentPagedRestoreUsesRangedReads`（`internal/dosupervisor`）+ `TestReadSegmentRange`（`internal/server`，Range 206）。

均已实现：`transferred_classes`（同 worker 别名，ADR-107 边界）、WS 跨节点转发（`TestDoRuntimeWebSocketCrossNodeForward`，ADR-084）。

对象 GC：`TestGCAdminEndpoints`（server：bundle/assets 两阶段标记→删除、引用保留）、`internal/objgc` 单元、`internal/artifacts` List/Delete、`internal/control` BundleRefs/AssetRefs/GCMark。
Queue 并发：`TestRunnerMaxConcurrency`（并发上限 + 默认不回归）、`TestConsumeSpecs`。
R2 multipart：`TestR2Multipart`（store：分片不可见/etag fail-closed/顺序拼装/abort）、`TestUserRuntimePublicLoaderD1R2Queue`（真实 workerd：facade 四端点 + 读回拼装）。
本地磁盘：`TestHasShedTarget`（只有存在有余量的活节点才进入压力态）、`TestSelectDiskEvictionsLazy`（超预算才判定、只判定被考虑的文件）、`TestCovers`（L1 manifest 覆盖判定）、`TestOverloadedRefusesClaims`（压力下 readyz/claim/写认领 503，解除后恢复，ADR-123）、`TestPlanDiskEviction`（预算/LRU/owned 保护）、`TestDiskFilesAndForget`（账本/幂等删除，ADR-122）。

写路径审计：`TestTimerUpsertForwardsToOwner`（timer upsert 转发/认领，ADR-121）。

读转发/重试：`TestReadForwardingAndRetry`（非 owner 读被转发且本地无副本；stale owner 409→恰好重试一次；dial 失败→502；ADR-120）。

Queue 归属/捕获：`TestOwnerGate`（只由 owner 消费，释放后 ≤1s 接管）、`TestRunnerFilterSkipsQueues`、`TestRunnerCommitWrapsMutations`（ADR-119）。

路由撤销/节流：`TestUserRuntimeRoutingScale`（真实 workerd：Host 扫描节流 ≤8 次上游查询；10s host TTL 下靠轮询 `/v1/control/routes` 的 ETag 令撤销 <5s 生效，ADR-116）。

路由读取：`TestControlHostAndWorkerEndpoints`（host/worker 端点 200/304/404、指针裁剪、revision 失效）、`TestUserRuntimeHostCacheGovernance`（真实 workerd：负缓存/单飞/TTL 重验证/失败退避/stale-on-error/删除与新增 host，ADR-115）。

dev CLI（`make cli-test`，Bun）：`cli/test/assets.test.ts`（assets 映射单测）、`cli/test/dev-assets-e2e.test.ts`（真实 Miniflare：`_headers`/`_redirects` 302/`not_found_handling` 404/`run_worker_first` 路径/worker 回退）。

isolate/env 缓存：`TestUserRuntimeBindingOnlyRedeployTakesEffect`（真实 workerd：同 `bundle_sha`、新版本号、改 `MODE` 必须生效；已用回退 id 验证会失败，ADR-126）。
内部派发版本键（ADR-127）：`internal/userruntime TestUserRuntimeServiceDispatchVersionRefreshesEnv`（内部 `/v1/services/run` 同 sha、version 1→2、vars 变必须生效）与 `internal/doruntime TestDoRuntimeSameShaRedeployRefreshesFacet`（DO facet 同 sha、version 变必须重建 env）——两条均已用“回退修复”验证会失败。
本地连接复用（ADR-130）：`internal/doruntime TestDoRuntimeHoldsConnectionAcrossRequests`（真实 workerd：租户 DO 持有 loopback TCP 连接，两次 invoke 复用 → 服务端 accepts==1；已用「每请求重连」验证会失败）。
路由挂载语义（ADR-131）：`TestUserRuntimeRouteStripPrefix`（真实 workerd：`/api` 挂载后 worker 看到 `/users`、POST body/query 保留、`path=''` 不剥离、`/apix` 不匹配 `/api`）与 `internal/server TestRouteMountSemantics`（投影/宿主视图带挂载路径）。
ADR-134 review 修复批次：授权 `TestScopedCredentialAuthorizationMatrix`（rollback/asset/logs/deploy/secret 跨 ns 403）、`TestScopedCredentialForwarding`（授权先于转发 + apps 取回后过滤）、`TestPromoteRollbackInvalidatesBindingCache`；RPO 水位 `internal/cellcapture TestWaitBlocksUntilCommitted` + `internal/server TestCapturedStoresAdvanceCellTxID`（回退 control.tx 会失败）；加载 `TestUserRuntimeColdLookupFailureRecovers`、`TestUserRuntimeNodejsCompatFlagApplied`（回退修复均会失败）、`TestWorkerEnvVersionSHAAndCompat`；ADR-135 遗留修复：`internal/control TestPurgeSkipsResurrectedWorker/App`（回退 Deploy 清理会失败）、`internal/server TestCaptureCommitEpochFence`、`internal/doruntime TestDoRuntimeConnectMatchesInvokeIdentity`、`internal/cellstore TestDeleteNamespaceDrainsAndDrops`、`internal/cellcapture TestEnsureDoesNotBlockOtherScopes`（回退锁内快照会失败）、`internal/auth TestJWKSServesStaleDuringRefresh`/`TestJWTWithoutExpiryRejected`、`cmd/cellhive TestCLISubcommandArity`。
存储 `internal/bucket TestDiagnoseDetectsConditionalDeleteIgnored`（模拟忽略 If-Match 的桶，diagnose 必须点名失败）与 CLI e2e 的 `diagnose()` 调用、`internal/cellstore TestForgetWaitsForInFlightRequest`（回退会失败）/`TestForgetWithVerifyRefuses`、`TestValidateRejectsInsecureFleetSecrets`、`internal/autoscaler` 毫秒修正、`internal/wrangler` hyperdrive 不被拒。
ADR-138（wrangler 别名）：`cmd/cellhive TestTranslateWrangler`（deploy 自动发现/`--name`/`--env`、`delete`/`versions list`/`secret`/`tail` 映射、未支持 flag 与命令报错）、`TestTranslateWranglerExplicitConfig`（`-c` 优先、缺配置缺 bundle 时明确报错）、`TestDiscoverWranglerConfig`（jsonc 优先）。
ADR-137（配置简化）：`cmd/cell-agent TestDerivedSecretKeyDecodes`（派生 secrets-root 必须能被 `control.ParseRootKey` 解成 32B——回退成 RawURL 会失败）、`cmd/cellhive TestCmdCreds`；`make rpo-test` 在 root 派生凭据下端到端 **PASS**（1200 acked keys 全在且精确）。`internal/config TestDeriveCredentials`（确定性/域分离/异根不同）、`TestLoadRootKey`、`TestFromEnvDerivesRoleCredentials`、`TestFromEnvStorageNames`（AWS_*）、`TestFromEnvDurationKnobs`、`TestFromEnvMergedKnobs`、`TestValidateRootKey`、`TestLegacyEnvWarnings`（旧变量名必须告警）；`cmd/do-runtime TestDoLeaseSeconds`/`TestRuntimeRoot`；`cmd/cellhive` e2e 改用共享 `CELLHIVE_ROOT_KEY`。
ADR-136（内部协议/接线）：`internal/config TestFromEnvAdvertiseDefault`（ADVERTISE 默认 `127.0.0.1:7001`）、`cmd/cell-agent TestNewLogBufferIsWired`（缓冲非 nil + 往返 + 源码守卫 `Logs: newLogBuffer(cfg)`，去掉接线即失败）、`cmd/do-runtime TestEnvTrue`（`DO_OBJECT_INDEX` 与 cell-agent 同语义）。
控制面 schema v2 / 域名 / 授权（ADR-131）：`internal/control`（软删 + purge 作业、`hosts` 归属与 pending 并存、`bindings` 派生表）、`internal/server TestBuiltinDomainOnDeploy`（deploy 生成 `<ns>-<worker>.cell.base` 的 hosts 行与路由）、`TestCustomDomainRegistration`（未注册 404、注册即路由、`domain rm` 后 404）、`TestHostOwnershipConflict`（pending 可接管、verified 409、内置域不可认领）、`TestAdminNamespaceAuthorization`（JWT `cellhive_ns` 授权、列表按 ns 过滤、平台级需 `*`、审计记 `sub`）、`TestSoftDeleteApp`（软删即停路由 + purge 清理）。
Hyperdrive 资源（ADR-129）：`internal/control TestResourceConfigSealed`（密文不含明文、无 root key 拒绝）、`internal/server TestHyperdriveResourceAndBinding`（注册→兼容门通过、未注册仍拒、内联 URL 兼容、internal 端点、spec 注入）、`internal/userruntime TestUserRuntimeHyperdriveResolvesFromPlatform`（真实 workerd：env 取到解析后的 URL 而非 id；解析失败省略绑定）、CLI e2e 增补 `resource create --connection-string` + `deploy --hyperdrive NAME=NAME`。
非 fetch handler 的 env（ADR-128）：`TestUserRuntimeDispatchInjectsBindings`（queue/scheduled 派发注入 bindings+vars、bindings 请求带 `version`、spec 失败时 fail open）与 `internal/server TestInternalBindingsVersionPin`（`version=1` 取 v1 env、缺省取活跃、未知版本空 spec、非法参数 400）——前者已用“回退修复”验证会失败。

Hyperdrive：`internal/wrangler` 映射用例 + `TestUserRuntimePublicLoaderD1R2Queue`（真实 workerd：租户读到 `env.HYPERDRIVE.{connectionString,host,port,user,database}`，ADR-125）。

wrangler 映射：`internal/wrangler`（jsonc 注释/尾逗号、绑定/consumer/cron/assets/migrations 映射、env 继承、toml 拒绝、被拒 section，ADR-124）；CLI e2e 里追加 `--config` 二次部署（releases=2 + vars 生效）。

CLI 端到端（`cmd/cellhive` `TestCLIEndToEndWithWorkerCode`）：真实 CLI（`app create` / `resource create --scope` / `deploy --bundle-sha --kv --route`）→ 进程内 cell-agent 控制面 → **真实 workerd loader** → 租户 worker 代码 → `env.KV` facade → KV cell；断言租户响应 `kv:v1`、投影里出现 CLI 写入的路由/版本、KV cell 里有权威数据。workerd 缺失自动 skip；`make test` 覆盖。

ADR-156（vwork 运维接口）：`internal/control TestResourceRevoke`（引用报告/幂等/墓碑立即生效且重新登记解禁）、`internal/server TestResourceDeleteEndpoint`（`resource_in_use` 409 → `force=1` 200 → 列表消失 → 吊销后 deploy 声明该 binding 被兼容门拒绝 → 404 → 审计 `resource.delete`）、`internal/queue TestStatusCounts`（`depth/visible/leased`，延迟消息不可见、租约不计入 visible）、`internal/server TestQueueStatusAndReplayDLQ`（死信深度 → `replayed:2` → 主队列 depth/visible=2、DLQ 清空 → 无 DLQ 400 `no_dead_letter_queue`）、`internal/userruntime TestUserRuntimePublicLoaderBuildsBindingFacades`（真实 workerd：投影拉取后 `/ready` 200 + `cell:"ok"`；错 internal token `/drain` 401；正确 token 后 `/ready` 503 `draining`）、`internal/doruntime TestDoRuntimeReadyAndDrainProbe`（真实 workerd：`/ready` 200，错 token `/v1/do/drain` 401，drain 后 `/ready` 503 `draining`）。部署产物：`make compose-config`/`make k8s-render`/`make helm-lint`（user-runtime、do-runtime 探针改用 `/ready`）。

ADR-157（每域资源 + stats）：`internal/cellstore TestDiskStatsAndKVStats`（page/file 字段、新库必须有 `kv_expires` 索引、`expired`/`next_expiry_ms`、dbstat 估计与 `estimate_note`、`?exact` 语义、prefix 作用域）与 `TestKVExpiryIndexUsed`（`EXPLAIN QUERY PLAN` 必须出现 `kv_expires`——回退修复会失败）；`internal/d1 TestD1Stats`（内部表排除、`?tables=1` 的 dbstat 明细、行数估计）与 `TestD1StatsLargeCellSkipsDetail`（超阈值必须跳过而不是变慢）；`internal/queue TestStatusCounts`（新增 `oldest_visible_ms`/`max_attempts` + `DiskStats`）；`internal/r2 TestStatsBoundedListing`（总量/前缀/`truncated`+cursor/`multipart_*` 计数/暂存区对用户 List 不可见/非法桶名）；`internal/workflow TestStats`（估计 vs `?exact=1` 的状态分解）；`internal/server TestPerKindResourceEndpoints`（每域 CRUD、服务端补 scope、body/path kind 冲突 400、409+`force`、**与 `/v1/control/resource*` 别名等价**）、`TestPerKindStats`（七种 stats 的真数据断言 + 旧 `/v1/control/queue/status` 保持扁平）、`TestStatsFailClosedAndScoped`（未知/已吊销 404、JWT 跨 ns 403）、`TestMetricsBucketAndCellGauges`（`cellhive_list_calls_total`/`cellhive_bucket_ops_total{op="list"}`/`cellhive_resident_cells` 必须出现）；`cmd/cellhive` e2e 追加 `kv namespace|queue` 每域 CLI（create/list/stats/delete）。

ADR-160（按页冷启动）：`internal/pagedvfs`（`TestPagedOpenFaultsPagesOnDemand`：点读只 fault 少量页且 `HydrateAll` 后文件逐字节等于源；`TestPagedWritesMarkHydrated`：写/checkpoint 不被旧 cut 覆盖；`TestBackgroundHydrateFillsTheFile`：限速后台补齐到全量且逐字节一致）、`internal/cellstore TestPagedCellServesKVAndSnapshots`（KV 点读正确 + `SnapshotPages` 先 hydrate + 写可用 + Close 释放）、`internal/compaction TestPagedCellAgentEndToEnd`（真链路：真 cell→桶快照→Compact 出 L1 索引→PageFetcher→冷 cellstore paged open；实测 8/771 页 fault、8 ranged 读、整对象 3158B vs 镜像 3158016B；**全表扫描 20 次 ranged 读**即窗口预取）、`internal/ltx TestPageMapV2Compression`/`TestPageMapV1StillReadable`（`WAL3` 压缩帧 + v1 兼容；基准 `BenchmarkFrameDecode` 1.1GB/s）、`internal/compaction TestL1SnapshotCompression`（L1 payload 4.4%）、`TestCompactFoldsLegacyV1AndGCsCompressedL1`（**v1 WAL2 基线 + 压缩 L1 + GC**：旧 L1/折叠 L0 删除、index/manifest 保留、压缩 L1 的 paged 读与整库 restore 仍正确）、`internal/pagedvfs TestChildPrefetchIsConcurrent`（peak=4 worker、6 散列 child 45.7ms）、`TestRunPrefetchReducesBucketReads`（336 页全扫 = 6 次窗口读）、`TestParseChildren`/`TestFaultPolicyChildAloneAndWindow`（内部页 child 单页取、顺序 child 命中预取缓存、顺序非 child 才开窗）、`TestHydrateUsesRuns`（批量 hydrate）、`internal/cellstore TestPagedCellReopenAfterClose`（**回归**：进程重启后经标记位重新注册稀疏缓存，数据正确而非零页/损坏）、`internal/server TestMetricsPagedStats`、`internal/cellstore TestDiskUsageCountsAllocatedBytes`/`TestDiskEvictionUsesAllocatedBytes`（稀疏文件按 `st_blocks` 计量与驱逐）。

冷恢复边界：`internal/replica TestColdHydrateIsWholeObjectNotPaged`（**实测**：cell-agent 的 hydrate 路径 = 1 次整对象下载 4198464B、**0 次 ranged 读**；分页原语可只物化部分页；ranged 读路径见 `internal/compaction TestPageFetcherOnDemandFromL1Index`）——回退成"分页 hydrate"必须显式改这个测试。

ADR-161（压缩开关）：`internal/ltx TestPageMapCompressionDisabledWritesV1`（`CELLHIVE_LTX_COMPRESSION=false` 时输出 `WAL2` 固定帧 + 固定 off 布局，`PageLocs`/`DecodeWALPageMap` 往返正确）。
ADR-167（OTLP 追踪）：`internal/telemetry TestExportsSpanAndRemoteSpan`/`TestSamplerHonorsRemoteFlag`/`TestDisabledIsNoop`（进程内 OTLP 接收器解码 protobuf，验证导出/父子/采样/关闭 no-op）、`internal/userruntime TestUserRuntimeTraceExport`（真 workerd：loader `http.server` span 打到 `/v1/internal/telemetry/spans`）、`internal/doruntime TestDoRuntimeSpanExport`（真 workerd：`do.invoke` span）、`internal/dosupervisor TestGateEmitsSpan`（`do.gate` span 导出，且 trace/父 span 延续调用方 `traceparent`）、`internal/doruntime TestDoRuntimeBindingInsideDO`（断言 DO 内 props-bound 绑定调用当前不带 traceparent 的已知边界）、`TestUserRuntimeTraceContextPropagation`（props-bound binding 现带 traceparent）。
ADR-166（复制/定时器/do 指标）：`internal/timer TestFireStatsAggregates`/`TestDispatchDueObservesFireOutcomes`、`internal/dosupervisor TestSupervisorMetricsAndStats`（`/internal/do/stats` 增量 + `/metrics` 渲染 + ws 钳零）、`internal/doruntime TestDoRuntimeMetricsReportedToSupervisor`（真 workerd：alarm 后 host 上报增量）、`internal/server TestMetricsObservability`（replication/waker 行）。
ADR-165（可观测性指标）：`internal/server TestMetricsObservability`（`cellhive_binding_calls_total{kind,outcome}`、`cellhive_durability_proof_seconds` 桶/sum/count、`cellhive_route_projection_version`、owner/takeover 与 hedge 行）、`internal/owner TestClaimStatsTakeoverAndEpoch`（fresh/blocked/takeover success + epoch bumps）、`TestClaimAsStats`、`internal/peer TestShipBatcherHedgeStats`（fired/won）。
ADR-164（peer 自适应 hedge + 幂等 spool）：`internal/peer TestSpoolIdempotentPerSequence`（同段重复 append 为 no-op，重启后从日志重建身份集）、`TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast`（primary 及时 ack 则不联系第二 follower）、`TestShipBatcherHedgeFiresOnSlowPrimary`（超时发第二份，先到者胜）、`TestShipBatcherHedgeOffSingleCopy`（`=0` 只发 primary，primary 失败顺序 failover）、`TestShipBatcherHedgeAllFail`、`TestShipBatcherHedgeAsync`（异步流路径：primary 同步写、hedge 后发）、`TestShipBatcherAdaptiveHedgeWait`（4×最近最慢、250ms 下限、backstop 上限）。
ADR-173（日志订阅 fleet 广播）：`internal/server TestLogSubscribeFleetFanout`（admin 订阅向另一活节点广播内部订阅、内部端点本地生效）。
ADR-175（兼容性收口）：`internal/dispatch TestDoAlarmDispatcherAddsScheme`（无 scheme 的 do-runtime 地址）、`internal/server TestDOAlarmOccurrenceCarriesIdentity`（occurrence 带 storage_class/storage_id，`|` 安全）；DoAlarmDispatcherRoutesAlarms 断言 alarm body 回放 storage 身份。真实门控栈矩阵：alarm/wsecho/r2 等全 PASS。
ADR-174（示例兼容性）：`internal/userruntime TestUserRuntimeKVPutStreamAndBytes`（KV `put` ReadableStream/Uint8Array 原样存储）、`internal/server TestKVValidationLimits`（key≤512B/metadata≤1KiB/ttl≥60s/expiration 未来）
ADR-177（wake 索引修复）：`internal/timer TestUpsertFailsClosedWhenIndexDown`（索引不可用时 timer 不提交）、`TestSyncIndexRepairsMissingEntry`、`TestStoreSyncsWakeIndex`（提交前先发布）；`internal/cellstore TestLocalScopesAndPendingTimerMin`（本地扫描 + 只读读取最早 due）。、`internal/doruntime TestDoRuntimeClassicDOAndStatus`（legacy DO 类包装 + 426 状态与 `x-cellhive-do-app` 透传 + legacy state 持久）、`internal/server TestDOProxyForwardsTenantResponse`（cell-agent 透传 426/content-type/标记）。
ADR-172（OTLP 日志导出）：`internal/telemetry TestLogExportModes`（`off` 不导、`all` 全导、`tail` 订阅前不导/订阅后导/TTL 过期停）、`TestLogSubscribeTTL`；`internal/server TestLogSubscribeEnablesExport`（`POST /v1/control/logs/subscribe` + `LogSubscribed`）。
ADR-171（spool 断电安全）：`internal/upload TestSpoolDurableAppendSyncs`（每次 append file fsync、目录 fsync 记忆化、Load 可读）、`TestBatcherStatsIncludeSpoolDurability`（`/metrics` 暴露 fsync 计数）。
ADR-170（DO 并发捕获 + ordered sweep）：`internal/server TestOrderedDispatcherDoSweepsIdleScope`（Do 触发空闲 scope 淘汰）、`internal/dosupervisor TestSyncAllPipelinesAcrossFiles`（6 文件/4 并发：max in-flight≥2、总耗时<串行、失败报错）；`-race`。
ADR-169（purge 分页删除）：`internal/objectstore TestObjectsListPage`（游标分页/前缀边界/保留前缀拒绝）、`internal/purge TestPurgePaginatesAcrossBudget`（预算 10 / 25 键多轮 drain）。
ADR-168（R2 list include/delimiter）：`internal/r2 TestListPageDelimited`（delimiter 归并、prefix 收窄、limit/truncated+cursor）、`internal/server TestR2ListDelimiterAndInclude`（`include` 按需回填 sidecar metadata、`delimitedPrefixes`、未知 include 400）、`internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta`（真 workerd：`list({delimiter,include})` 透传 + 返回 `delimitedPrefixes`/对象 metadata）。
ADR-163（R2/D1 契约保真）：`internal/r2 TestR2MetadataStatAndChecksums`（metadata sidecar 往返/`Stat`/checksum 校验/删除清 sidecar）、`internal/d1 TestLastInsertID`（exec/query/batch 的 `last_row_id`）、`internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta`（真 workerd：R2ObjectBody 字段 + `text()` + `head()` + list `truncated/cursor` + put metadata 头 + D1 `meta.last_row_id`/`D1_ERROR`）。
ADR-162（DO RPC）：`internal/doruntime TestDoRuntimeRPCDispatch`（真 workerd：普通 tenant 方法的原生 JSRPC、tagged Map/Date 往返、tagged Map 参数、共享引用/环、handler 错误 500、缺失方法 404、保留/`__ch*` 方法 400、不可序列化结果 500）、`TestDoRuntimeDurableObjectToDurableObjectRPC`（facet 内经注入 facade 调兄弟 DO）、`internal/userruntime TestUserRuntimeDurableObjectRPC`（真 user-runtime：`getByName` → tagged 往返 → 结构化 Error 的 `code`/`message`）、`internal/server TestDOProxyRPCPassthrough`（rpc 字节透传如 `1e21`、request/rpc 互斥、kind 校验、8 MiB 超限 400）。
ADR-159（CGo + vec1）：`internal/vectorize TestVec1CellReplicatesThroughLTX`（**LTX 复制端到端**：vec1 索引 + ANN 模型 → cellcapture 快照/delta → `restore.ApplyFile` 还原到新 cellstore → 查询结果/模型/删除/元数据一致，还原后可写）、`TestVec1ExtensionRegistered`（无 `.so` 也能 `vec1_info()` + 建 vec1 vtab；证明静态注册成功）、store 单测改为 vec1 后端（insert-only/upsert 替换、cosine/euclidean 的 score 映射、namespace 下推、9 种 metadata 过滤、上限与 config 不可变、dot-product 拒绝、`BuildANN`/`DropANN`）、`BenchmarkQueryVec1`（20k×256 flat ≈ 6.3ms；ANN ≈ 0.22ms 由 `cellhive vectorize rebuild` 复现）；`internal/server TestVectorizeANNEndpoints`（rebuild/stats ann/drop-ann/dot-product 400）；驱动回归 `go test ./...`（全量包）+ `scripts/ci.sh`（tags 由 Makefile/ci.sh 传递，S3 复制/恢复链已复测）。

ADR-158（Vectorize）：`internal/vectorize`（store 单测：`insert` 不覆盖已存在 id / `upsert` 全量替换、cosine/euclidean/dot-product 三种 score 与排序、namespace、9 种 filter 操作含嵌套点路径与隐式 AND、维度/metadata/id 上限、`topK` 夹取、metadata index 目录与 10 条上限、`describe`/`listVectors`/`stats`/`queryById`；`TestTopKWindowOrdering`（部分填充窗口的中间插入必须位移、不得留空槽——容器 smoke 抓到的真 bug）、`BenchmarkQueryExactScan` 给出延迟表）；`internal/server TestVectorizeEndpoints`（缺 config 拒绝 → 创建 → `/v1/vectorize/stats` → 绑定面 insert/query/filter/queryById/get/list/describe/维度 400/delete → metadata index CRUD → 吊销后 403 `binding_not_registered` → 跨 ns scoped token 403）；`internal/userruntime TestUserRuntimeVectorizeBinding`（**真 workerd**：facade 全方法、`ns=acme&index=docs` 寻址、JS 铸造 token 可被 Go 验证、响应形状与 CF 一致）；`internal/wranglercompat TestVendorMatrixContract`（vectorize 在支持矩阵内 + 用 `index_name` 查登记）；CLI `cli/test/bindings-parity.test.ts`（dev 对 vectorize 明确报错且不再是平台拒绝项）。

_最后更新：2026-09-19_
