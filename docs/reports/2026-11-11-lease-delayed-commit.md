# 续租延迟提交误判修复

## 问题与修复

USW 旧 agent 在 07:06:37.713Z 发起续租，客户端写超时后确认读取仍见旧版本，清除了 pending。COS 中该请求实际成功：原 session `1794380759775330762` 的 expiry 为 07:06:47.713Z。下一轮生成不同内容并使用旧 token，条件写冲突，误将自己的前次写入判为 `lease_replaced`。INFO 日志不能确定最初网络超时的具体阶段或服务端实际提交时间。

现在看到旧版本或初次创建尚不存在时保留完整 pending。后续 Publish 先精确确认；若仍见旧值，仅重试原 payload、expected token 和 expiry。不会生成更晚到期时间，未确认的写入不延长本地权限。真实不同值、明确冲突且无法精确确认、原租约到期仍 fail closed；不以相同 session 代替精确确认。不改变对象格式、数据恢复或业务请求重放策略。

## 验证

- 修复前确定性复现失败：超时、读取旧值、延迟提交、再次 Publish 导致自我 fencing；修复后通过。
- 本地组合覆盖初次创建/续租、连续超时、确认前延迟提交、原请求在重试期间获胜、仅重试成功；保持 payload/token/expiry 不变，确认后可正常进行下一次续租。
- lease/bucket race 通过。已有不同内容同 session 拒绝、过期不能复活、冲突确认不可用仍 fencing 测试通过。
- 真实专用桶 `cellhive-e2e-hk-1376795072`：`TestCOSRenewalDelayedCommit` 4/4，通过普通追加与 generation rollover。通过测试包装器确定性注入超时/延迟提交，再调用真实 COS 原生条件追加；不是生产网络延迟复现。仅使用唯一测试 node 路径，测试后按精确 key 清理。
- 双进程 `scripts/e2e/session-recovery.sh` 通过：恢复中断重试、空替代盘拒绝、80 条已确认数据恢复、SIGSTOP 超 TTL 接管、旧进程恢复后退出和新 session 恢复。
- owner/server/cell-agent 回归通过（启用项目 SQLite build tags）。
- `go vet ./internal/lease ./internal/bucket`、`git diff --check` 通过。

本次为源码修复与验证，尚未部署。修复消除了已复现的延迟提交误判；不承诺对象存储不会超时，也不取消真实失去权限时的安全退出。
