# SG Queue 发现缓存发布

## 发布范围

用户授权发布当前优化，沿当前问题范围只更新 SG cell-agent。源码提交 `0620be29` 已推送，从干净 git archive 构建；两个无关临时目录未纳入镜像。

- SG Helm revision **137 → 138**，通过 cellhive-helm wrapper 执行预检、diff、逐个替换 OnDelete agent。
- 镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive:20261123-cellhive-0620be29-read-cache`。
- digest `sha256:337531bfdb3c7fd7a6939eb1ec8af64e9cb155c8654a5bb4bb0d80712d460cdf`。
- agent 二进制 SHA-256 `1e7c14fef98b6e3912438142a9660cf1c60e9f9c022c3bbd410204b9700daf2a`。
- 本机默认 workerd 文件与生产不同，构建复用上一版已验证镜像的 workerd，校验与线上相同：`afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`。
- 最终发布 diff 仅 cell-agent 镜像；user-runtime、DO runtime、Vector 的 Pod UID 未变化。所有 PVC 与 HPA 的 UID/spec 未变化。无数据迁移、不直接清理或改写桶对象、不强制业务 owner 切换、不操作 USW；滚动重启发生正常 owner 接管。
- 升级后 Helm diff 为空。运营仓 SG values 保留本次 digest 差异，未单独提交。

## 功能与观察

独立 namespace `read-cache-sg-20261123` 的真实 Worker Queue 发送、消费副作用、ACK 清空、失败重试进入 DLQ、源队列清空通过。测试结束后将这个临时 Worker 的 consumers 设为空，保留 Queue/KV 数据与路由，避免新增消费者污染生产请求率对比。

首个本机探针误转发内部 8088，/ping 返回 405；改为服务公开端口 8081 后原测试正常完成。这是探针接线问题，不是生产入口故障。测试通过内部端口转发加 Host 路由执行，不覆盖公网 DNS/TLS。

滚动期间出现控制面 connection refused / owner unowned、prepare follower 暂不可用；写入等待 bucket 证明。稳定期日志只见此前已有 purge retained 与租户 Cron handler 失败，观察 601 秒，11 次定时 Queue 状态检查全部通过；六个服务 Pod 全程 Ready、零重启。最终五分钟 agent 日志没有新的 Queue/control/fleet 故障，仍有既存 purge retained 和租户 Cron 失败。发布前已存在的 queue-fix-sg-20261123 主队列深度 0、DLQ 深度 1、KV 消费副作用读回均通过。

## COS 请求率

发布前 60 秒采样，两台 agent 合计约 **89.79 GET 类操作/秒**。发布后首个 180 秒窗口合计约 **87.64/秒**，此时额外运行了新建验收消费者；该窗口保留，不将小幅变化宣称为明确收益。停用临时消费者后的第二个 180 秒窗口为 **89.40/秒**，未观察到总 GET 请求显著下降；该性能问题尚未整体解决，不能用单元测试的局部调用减少推断线上总量已下降。第二轮没有恢复文件增长。计数含 Range GET，来自 agent 指标，不是 COS 账单；业务负载和后台维护并非严格受控实验。

## 证据

本机 `/var/tmp/cellhive-sg-read-release-20261123/` 保存构建日志、digest、前后 Pod/HPA/PVC 元数据、Helm diff、Queue 结果、观察日志与 Prometheus 计数。没有备份业务数据。代码与本地回归见 [优化报告](2026-11-23-control-read-cost.md)。
