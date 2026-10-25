# Workflow 事务栅栏与事件恢复验收（2026-10-26）

修复提交 `dd614b4`（基于 `720f5b9`），ADR-299，已推送并发布 SG/USW/本地常驻服务。

## 修复

- 所有运行时回调写入在实际 SQLite 事务内检查 run token；前置校验后发生 restart 的旧 step/finish 不再污染新执行，返回 409。重试记录、等待记录删除和错误终态同样保护。
- 按 `(instance, step name)` 原子选择事件、标记消费、保存步骤结果并清理等待记录。响应丢失后重读步骤，事务失败回滚消费；并发不同步骤只能取得不同事件。
- 暂停/终止/错误终态与清空 lease 原子提交，避免分开释放误伤新 run。
- 已保存 sleep deadline 的提前重派发重新 park，保留原截止时间并释放新 lease；不让 SleepSignal 单独退出后占用 60 秒租约。
- schema 迁移缓存按实际 cell 句柄校验，防止删除重建同 scope 后沿用已建表标记；本地删除后重建的复现从 `no such table: instances` 转为通过。
- 不改变 schema、owner/epoch fence 或 capture 输出门。旧无步骤名的可信内部消费接口仅保证原子消费，不具有步骤幂等重试语义；运行时携带 token 的消费必须有步骤名。

## 本地验证

- 原始审查的 handler 交错复现：旧 step 返回 200 并写入新执行；旧 finish 返回 200 并将新执行标为 complete。修复后两者返回 409，新执行保持 running、无旧步骤及输出。
- 单事件 32 并发消费：修复前 30 轮中 19 轮重复，最高 32 次；修复后事务串行选择。
- 新增失败注入：保存步骤时 SQLite trigger 主动失败，消费一并回滚，删除 trigger 后仍能消费原事件。
- 重新 claim 后步骤仍保留；相同步骤重复请求返回同一结果，不同步骤不能复用已消费事件。
- sleep wrapper 旧版本测试失败（末次调用 step.get，未释放 lease），新版本通过（再次 sleep，原 deadline 不变）。

验证输出：[race](workflow-atomic-recovery-2026-10-26/race.txt)、[JS/workerd](workflow-atomic-recovery-2026-10-26/js.txt)、[构建](workflow-atomic-recovery-2026-10-26/build.txt)、[二进制 SHA256](workflow-atomic-recovery-2026-10-26/binaries.sha256)。

## HK COS E2E

计算进程本机，存储为香港专用 COS 测试桶 `vwork-e2e-hk-1376795072`，durability=bucket。真实 cell-agent + user-runtime + pinned workerd，不是内存桶或模拟 COS；不代表香港计算主机同区延迟或跨 AZ peer 验收。

运行命令：

```bash
make build
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite workflow --port-base 26700
```

最终整轮 **PASS，退出码 0**：[HK 完整输出](workflow-atomic-recovery-2026-10-26/hk-e2e.txt)。namespace 为 `hk-review-1792955684-83bde0-0` / `hk-review-1792955684-83bde0-1`，本地数据目录 `/tmp/cellhive-e2e.6YG0fp` / `/tmp/cellhive-e2e.87fknu`。

- 普通 sleep 等待后完成；waitForEvent 超时进入 errored；step.do 失败两次、第三次完成；Workflow 内 R2 metadata 读回正确。
- 真实 runtime 产生的旧 token 在 restart 后 step/finish 返回 409，新执行输出为事件值 7。前置检查与事务之间的精确交错由 handler 回归测试覆盖。
- 真实 workerd 的 16 路并发 waitForEvent 消费预置的 16 个事件，输出排序后严格为 0..15，无重复无遗漏。
- 代理等待 agent 完成 COS 持久证明后扣住 event.consume 响应；在 runtime 尚未收到事件、无法发送 step.put 时 SIGKILL runtime 和 agent。
- 使用不同 node ID 和全新本地数据目录启动 agent，从 COS 恢复，等待原 run lease 自然到期后继续执行，最终输出事件值 42；此前两个实例的值 7、0..15 也全部读回一致。没有把 lease 绕过、手工状态改写或重发事件算作恢复。
- 两个 namespace 均通过 DELETE app 清理，HTTP 200；所有本轮测试进程已停止。本地测试目录保留供审计，不进行整桶删除；历史 node/诊断记录仍按现有协议保留。

最终本地门禁：`make build`、`make test`、`make vet`、`make js-test` 均退出 0；新增 Workflow/store/handler 故障回归在 `-race -count=10` 下通过。[Go 全量输出](workflow-atomic-recovery-2026-10-26/go-test.txt)、[vet](workflow-atomic-recovery-2026-10-26/vet.txt)。没有设置集成测试凭据的默认 `make test` 不等于所有云端测试均运行，真实 COS 覆盖以本节用例为界。

首轮未通过：普通 sleep 出现 run lease held；另一个故障注入脚本使用资源名而实际 cell 使用绑定名，旧回调错误访问不存在的实例返回 404。已修复 sleep 提前唤醒行为和脚本取实际回调 workflow 名，再从新 namespace 完整重跑；首轮失败不算通过。

历史上已经被消费但未保存步骤的事件无法自动确定所属步骤，不做自动重放。

第二轮故障注入代理采用 HTTP/1.0、默认小连接 backlog 时，实例在消费前报 Network connection lost（16 个事件均未消费）；测试代理改为 HTTP/1.1 和 128 backlog，并分离串行投递与真正的 16 路并发消费，之后并发消费和冷恢复成功。代理只打印操作路径/异常类型，不记录内部 token。

随后功能用例全部通过，但两个 namespace 的 app 删除报告 `workflow drop ...: no such table: steps`，不能将该轮整体算通过。已确认 Workflow schema 缓存按 scope 而不是句柄导致重开空库时漏建表，添加红绿回归并修复；前一 namespace 由新 agent 重试清理 HTTP 200，后一 namespace 也已由最终验收环境的新 agent 清理，HTTP 200；见 [清理重试](workflow-atomic-recovery-2026-10-26/cleanup-retry.txt)。

## 发布验收

镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:afdc37dfa36e1b9c96edbf8cc1290dd6e714d93214813b9fda6b6ebd529be879`。SG Helm revision **69**，USW **54**；官方 wrapper preflight/diff/atomic upgrade，diff 仅三个业务工作负载的镜像与对应 generation。运维仓提交 `9235f4e`（该仓未配置 remote）。本地 Compose 保留现有 env、配置、数据卷及 Hyperdrive overlay，只更新三个业务服务；回滚标签 `cellhive:pre-299-local`。

两地各 6 个业务 Pod 的实际 imageID 与新 digest 一致，全部 Ready、restartCount=0；本地三个服务使用同 digest，healthy、restartCount=0。[发布及探针输出](workflow-atomic-recovery-2026-10-26/release.txt)。

- 三处发布前建立 Workflow 实例。SG/USW 旧版本的 waiting 预检未通过（实例停在 running），本地在一次 app create owner 拒绝后重试创建成功并 waiting；未把旧版本预检记作全部通过。
- 发布后三处都读到 waiting，投递事件后 complete，结果 value=299；测试 app 删除均 HTTP 200。
- 发布前 KV 写入并从两地各两个 agent 交叉读回；发布后原值保持一致，新 PUT/GET 正确。本地同样通过。这些单次请求耗时不是热性能基准。
- 三处 DO 原有持久值正确；binary、headers、双 Cookie、deleteAll、redirect 探针通过。SG 首次 DO 探针退出 1，未记录其响应体；随后 supervisor 完成冷恢复，直接 GET 为原值且 HTTP 200，完整探针重跑通过，不将首次失败隐藏为无中断。
- 部署观察窗口内仍有已知 DO recovery pointer CAS 告警（写入等待桶确认）、短时 workflow run lease held，以及 USW 对已删除 resource incarnation 的失败关闭告警。没有把这些说成已修复或全程零告警；本次不修改其协议。有限发布验收不替代全桶审计或长期混合负载验证。

三处 Workflow 测试应用已清理；KV 探针带 TTL，DO 使用既有运维探针实例。受限本机证据 `/tmp/workflow299-release/` 保留构建、push、Helm diff、Pod 回读和日志；部署没有做故障注入，SIGKILL/COS 冷恢复验证仅在专用 HK 测试桶完成。
