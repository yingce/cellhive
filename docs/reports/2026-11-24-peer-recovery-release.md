# 2026-11-24 Peer / Recovery 三地区发布验收

仅更新 cell-agent，源码 `892c979739f236e205c0ce309b643e3e340b5b57`，独立分支 `release/peer-recovery-read-reuse-20261124`，基于上次已上线版本。未包含主分支其他待发布的 shared 调度代码。使用干净 git archive 构建、推送不可变镜像，并经 cellhive-helm wrapper 逐台更新。

镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive:20261124-cellhive-892c9797-peer-recovery`；digest `sha256:dc68536738177a541c061667ff0bc111912d2bab2d13826ca3d2bb113ddc3f25`。

**节点 lease TTL 仍为 10 秒**，三地区均复核默认配置。改的是 Recovery 周期 5→30 秒，及 peer/discovery 对成功租约观察的有界复用（最多 min(TTL/2,5s)，临近到期仍读桶）。自身续租与 COS 根校验不变；后台恢复可能在 lease 到期后再等一个 30 秒周期及执行耗时，未知 session 仍受历史发现周期影响。请求触发同步恢复不受影响。没有实施 lease TTL 10→30 秒。

## 验证

确切发布分支 make test/build/vet 与 lease/recovery race 通过。三地区各覆盖至少十分钟、11 次健康检查，全部服务 Ready、零重启。SG/USW 真实 Queue 发送、自动消费、ACK 排空以及 KV 连续/空闲后读取通过；GZ 使用既有隔离测试 Worker 验证 KV PUT/GET/DELETE，结束时测试键删除。其他 runtime 和 workerd 未更换；PVC/HPA 身份保持；升级 diff 仅 agent 镜像，升级后 diff 为空。

## 采样

各区双 agent 合计的对象 GET attempts/s，发布前 120 秒、发布后 360 秒。工作负载未固定，不能当作同负载基准或固定账单节省。

| 地区 | Helm revision | 前 GET/s | 后 GET/s | 降幅 | lease 前→后 | recovery 前→后 |
|---|---:|---:|---:|---:|---:|---:|
| SG | 147 | 15.179 | 13.602 | 10.4% | 5.240 → 4.211 | 2.312 → 0.602 |
| USW | 139 | 11.538 | 7.683 | 33.4% | 4.849 → 3.588 | 2.304 → 0.530 |
| GZ | 3 | 7.911 | 4.960 | 37.3% | 4.537 → 3.388 | 2.276 → 0.363 |

GZ wrapper 首次运行在删除 agent-0 后立即 wait Ready，遇到新 Pod 尚未创建的 NotFound；Pod 随后正常创建，补等创建与 Ready 后继续验收，未重复升级，revision 仍为 3。运维 wrapper 已在 wait Ready 前增加 wait --for=create，并通过 bash -n 与实际等待命令验证。该运维改动尚未提交，其他原有未提交运维改动保留。

自身续租、全量发现、业务 owner/wake 读取仍有开销，不能宣称后台 GET 已消除。日志检查详情与版本校验见 artifacts；本次未修复既有租户 Cron 错误或 Fold deferred。SG 与 USW 观察中出现 lease publish confirm_read 超时，发生时剩余有效租约约 3.9–4.9 秒；USW 发布前亦有同类告警。最终十分钟日志 SG 为 1 次、USW 为 4 次（USW 发布前为 2 次），均未造成探针失败或重启；短窗口不足以判定频率变化的因果，不能将健康通过解释为完全无告警。

证据目录：`/var/tmp/cellhive-peer-release-20261124/`（测试、镜像构建、before-metrics.json、post.json、observations.json、Pod/PVC/HPA、日志、Helm diff）。

二进制 SHA256：cell-agent `7b6cf2d46b572e908c13f5690227c3c01b4ec78f0a2f75ac11b84e9095a05ae6`；workerd `63009b9b507a7b4fefb77de9ecb4ef87252476783da96054502965fbb984760c`。各区实际运行文件与构建产物一致。
