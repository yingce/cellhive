# DO 地址变化后不恢复：后台日志配置请求阻止客户端回收

2026-10-26，延续 `stability-performance-review-2026-10-26.md`。本轮检查时SG、USW所有业务Pod均Ready、容器重启计数0；没有对生产注入故障或重启。USW此前故障Pod已被删除，本轮证据不构成对当时单次事件的唯一归因。

## 确认的故障链

DO host每次fetch（包括/healthz、/ready）触发maybePollLogCapture；旧实现的pollLogCaptureOnce既没有响应头超时，也没有JSON body读取超时。一次不结束的请求同时导致：

1. captureInflight永不清空，日志配置不再刷新，前台refreshLogCapture也可能一直等待。
2. workerd/KJ按hostname缓存NetworkAddressHttpClient；只在客户端drained后移除对应host。仍在进行的配置请求使旧地址对应的客户端不能drain。
3. DNS已变、新节点正常，但后续ready仍复用旧地址；ready自身800ms AbortController不能终止另一条后台请求。

源码证据：CellHive `workerd/do-runtime/host.js` 的maybePollLogCapture/pollLogCaptureOnce；相同实现也在user-runtime的loader.js及internal.js。workerd实际走NetworkService→KJ NetworkHttpClient.getClient/handleCleanup，NetworkAddressHttpClient.isDrained检查activeConnectionCount及availableClients。这里不是external service的PromisedNetworkAddress路径。简化成单独fetch的早期测试遗漏了后台请求，因此不能复现此问题。

## 实际runtime隔离实验

使用已发布镜像digest `9e9811870fe03e673b5e1ef4e0fb1cb54ed1984af8b34c1a7e524667b939fd89` 内的do-runtime与workerd，独立Docker网络、两个可控HTTP后端及无生产凭据的数据目录。运行真正的DO host，而非简化fetch worker。

- 旧后端先令日志配置请求挂起；ready成功，证明runtime已初始化。
- 让旧地址继续接受TCP但不响应ready，把容器内hostname映射改到新后端（改变解析结果，不测试CoreDNS传播延迟）。
- 容器内curl同一hostname的readyz为200；旧版本DO /ready持续503。手工窗口22次、约40秒全部失败，随后仍失败；可重复脚本15秒验收失败。
- 对照A：上线镜像内原JS，失败。
- 对照B：已有的readiness分类诊断/释放响应体改动，但不加后台拉取超时，仍失败（15秒连续timeout）。
- 实验组：其余条件相同，仅配置读取改为完整2秒上限，首次探针timeout，约2秒恢复200；此后连续3次200，新后端stats确认log-capture轮询已迁移；runtime没有重启。

命令：

```sh
# 旧镜像JS：预期退出1
CELLHIVE_E2E_USE_IMAGE_JS=1 bash scripts/e2e/do-network-recovery.sh
# 当前源码JS + 同一镜像内workerd：预期退出0
bash scripts/e2e/do-network-recovery.sh
```

脚本使用唯一容器/网络名，退出自动清理，不挂生产卷。支持CELLHIVE_E2E_IMAGE、CELLHIVE_E2E_WORKERD_SOURCE、CELLHIVE_E2E_OUTPUT。它验证地址解析结果变化，不模拟Kubernetes调度、云网络或完整租户DO持久存储。

## 修复（ADR-297）

platform/log-tail.js新增共用readLogCapture，DO host、公开loader、内部dispatcher三处调用；2秒预算覆盖连接、响应头及JSON读取，finally结束AbortController作用域并清理timer。错误仍沿用已有capture策略/legacy fallback，超时使captureInflight清空，允许下一轮重试。没有修改租户请求超时、owner/epoch/session fence或数据持久确认。

这是控制面配置GET的故障上限，不是每个请求固定增加2秒。正常配置读取仍立即完成；内部派发首次恰逢配置故障最多等本次预算。没有把workerd全部出站请求统一设成短超时，也没有变更headless/ClusterIP布局。其他无限期出站请求仍可能形成类似客户端保留，需要按各自语义审查，不能据此宣称所有DNS切换问题均已消除。

## 回归证据

- 真实workerd `TestDoRuntimeLogCapturePollTimeout`：无响应头、已返回半截JSON两种情况。修复前均polls=1/cancelled=0而失败；修复后约2.2秒内取消并重试。与readiness诊断/后端挂起恢复测试一起通过（6.510s）。
- 真实user-runtime `TestUserRuntimeLogCaptureTimeoutDoesNotBlockDispatch`：日志配置返回半截JSON后挂起，约2秒取消；真实queue handler正常返回handled:1:hello（2.078s）。
- Docker实际DO host换地址E2E：旧版失败，diagnostics-only失败，当前源码通过，且验证后台轮询到达新后端。
- 全量 `make test`、`make vet`、`make js-test` 均退出0；js-test最后串行执行实际runtime测试（userruntime 32.543s、doruntime 30.075s）通过。本轮没有修改peer实现，因此复用上一轮peer race与session恢复证据。

原始证据 `/tmp/do-network-recovery/`：`poll-timeout-red.log`、`poll-timeout-green.log`、`script-red.log`、`diagnostics-only-run.log`、`script-green.log`、各运行目录的探针/后端stats/runtime日志、`user-poll-timeout.log`、`all-test.log`、`vet.log`、`js-test.log`。所有隔离容器和网络已清理。

修复已随代码 `0e7c67d` 推送并部署，见下方发布验收。此前线上USW故障与本机制症状一致，但缺少被删旧Pod的完整请求证据，不能唯一追溯其触发原因。


## 2026-10-26 发布验收

- 应用代码：`0e7c67d`，已推送 origin/main；包含 ADR-296 peer stream 修复与 ADR-297 runtime 配置读取超时。
- 镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:50fc86bc80bd7476680fd929f33a0911a642466d9b63cd1fbc7c702218758506`。
- 新镜像原生 JS（不挂源码）的 `do-network-recovery.sh` 通过：elapsed=1s HTTP503，elapsed=2s HTTP200，随后连续成功，重启计数0。证据 `/tmp/release297-image-e2e/`。
- SG Helm revision67、USW revision52，均通过官方 preflight/diff/atomic upgrade；diff仅业务镜像及对应generation变化。两地各6个业务Pod的imageID均回读为上述digest，全部Ready、restartCount0。
- 本地沿用三份Compose配置和原env/volume，只更新cell-agent、user-runtime、do-runtime；三者上述镜像、healthy、restartCount0。回滚镜像保留为 `cellhive:pre-297-local`。
- 三处发布前写入KV与DO标记，发布后均读回；两地KV分别经两个agent验证，新增写入读回通过。DO持久值、binary、headers、双Set-Cookie、deleteAll、302/Location全部通过；SG又重复通过一次。
- USW两个DO均记录一次 `do_readiness_failed` network，随后 `do_readiness_recovered` failures=1；滚动自动恢复，无额外删除/重建DO Pod。
- KV单次发布后PUT样本：本地11.049ms；SG419.039/57.980ms；USW1099.383/11.760ms。这是恢复后验收样本，不能当作热请求benchmark或RPS提升证据。

### 发布后仍需跟进

不能把本轮部署视为全部稳定性问题已闭环：

- 两地DO验收对象出现 `DO recovery pointer CAS contention`，触发 `do_coordinate_unavailable`，写入等待bucket确认后成功。SG重复验收仍观察到，不能仅归为启动瞬时告警；未修改该逻辑。
- SG `vwork-77/__invocations__/main` 多次compaction失败：`restore: non-contiguous chain: expected txid 3772, got 3770`。本轮未验证该scope的冷恢复，不能由测试namespace读写通过推断该数据链安全；根因和是否历史遗留未确认。
- SG滚动窗口一次cron dispatch500，user-runtime记录invocation persistence502；未声称该次业务任务已经补偿成功。
- USW一次waker告警：`resource incarnation is deleted`，scope为 `vwork-74/__kv__/kvns_2dacf8ed-8eef-4c52-a060-47a9c3ac5b78`；需要区分删除后的残留任务与业务异常。

发布命令与现场日志保留于 `/tmp/release297-{build,push,local,sg-upgrade,usw-upgrade,sg-diff,usw-diff}.log`、`/tmp/release297-{sg,usw}-{pods.json,agent.log,do.log}`。旧故障隔离证据与全量/race/session测试见前文。
