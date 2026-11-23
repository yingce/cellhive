# 2026-11-24 SG / USW / GZ 读取复用发布验收

## 发布范围

仅更新 cell-agent。生产使用独立发布分支 `release/celld-read-reuse-20261124`，源码 `638bf6c7c7633399d799f159ae42e23b967552c4`，基于三地区原镜像源码 `a6740ff2`。主工作区同批实现提交为 `d7281f8d`；主分支上尚未上线的 shared 调度等其他改动没有随本次发布进入生产。

镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive:20261124-cellhive-638bf6c7-read-reuse`，固定 digest `sha256:ceecf1903b8542b2a583d2b868a0852f1c11dec94b9200a57fafad5d88941775`。SG revision 144，USW revision 138，GZ revision 2。操作均通过 cellhive-helm wrapper，agent 逐台替换，部署 diff 只有 agent 镜像。workerd 与其他 runtime 保持原版本，不改变存储协议、调度配置，不清理业务数据。

cell-agent SHA256 `46789a6bdf12451df3dc130613835885fbf799f654c2465876aa9d8701128fca`；复用的 workerd SHA256 `63009b9b507a7b4fefb77de9ecb4ef87252476783da96054502965fbb984760c`。各区运行实例与镜像校验一致。

## 验证

独立发布分支重新执行 make test/build/vet 与相关 race，全部通过。SG/USW 原有测试 Worker Queue 发送、自动消费和 ACK 排空通过，DLQ 原有深度保持不变。三地区分别完成连续 KV 读取和间隔 2.2 秒后的读取。GZ 使用独立测试 namespace `read-reuse-gz-20261124`，验证 PUT/GET/DELETE，结束时删除测试键。

三地区各 11 次健康采样，覆盖至少 10 分钟，服务容器持续 Ready、零重启。原有非 agent 工作负载未被替换，PVC/HPA 对象身份保留。升级后 Helm diff 为空。

## GET 采样

发布前各 120 秒，发布后各 360 秒，统计各区两个 agent 合计的对象 GET attempts（含 Range GET）。业务负载未固定，结果是实际采样窗口的差异，不能换算成固定成本节省承诺。

| 区域 | 发布前 GET/s | 发布后 GET/s | 降幅 |
|---|---:|---:|---:|
| SG | 24.813 | 19.694 | 20.6% |
| USW | 14.041 | 11.182 | 20.4% |
| GZ | 7.840 | 8.039 | -2.5% |

GZ 发布前后为新区域的低负载窗口，总 GET 未下降（约增加 2.5%）；其发布前没有 pinned KV 定时刷新，历史 node-log 读取也很少，本批不能声明该区节省了请求。发布后剩余主要为 lease 与 recovery 的 fleet/lease 点查。SG 与 USW 的收益不应外推至 GZ。

调用来源详情与延迟原始样本见发布 artifacts。KV 延迟包含本机到集群的端口转发网络开销，样本少，未进行旧版本同负载对照，不能据此声明延迟无回归。

GZ API Server Pod proxy 无法直达 metrics，改为 Pod 内 localhost 读取；两次快照方式一致。滚动时出现的取消/owner unowned 与 peer 连接失败单独保留在 initial-agent.logs；稳定日志摘要见各区 log-summary.json，既有租户 Cron 失败与 purge retained 不作为本批已修复事项。USW 观察窗有 1 次 lease publish confirm_read 超时（发布前窗口 5 次），发生时剩余租约约 4.68 秒，服务探针与零重启检查通过；SG/USW 仍有 Fold deferred，包括等待驻留 cell 与无法恢复 durable chain 的既有维护路径，本批未宣称修复这些问题。

Artifacts: `/var/tmp/cellhive-reuse-release-20261124/`（测试、构建、planned.diff、post.diff、observations.json、kv-latency.json、before-metrics.json、post.json、日志与最终 Pod 清单）。
