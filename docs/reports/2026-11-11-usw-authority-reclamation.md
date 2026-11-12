# USW authority 退休分段回收

仅更新 USW cell-agent；SG、本机 RustFS、user-runtime、do-runtime/workerd 未更新。

## 实现与发布

新增 `CELLHIVE_AUTHORITY_MODE`：默认 legacy；validate 启用根校验及写确认，退休时仍写 stub；reclaim 在相同校验路径上删除退休段。reclaim 还要求 `CELLHIVE_AUTHORITY_RECLAIM_ACK=all-readers-validated`。ACK 是人工部署确认，不自动证明不存在未知旧客户端。

两阶段通过 `/opt/cellhive-helm/bin/cellhive-helm usw upgrade` 发布：

- Helm 121：两节点全部 validate，确认实际 Pod 镜像、模式、Ready、无重启；最近一分钟无新续租失败，公开 KV 10/10 HTTP 200。
- Helm 122：两节点全部 reclaim，实际二进制 SHA256 均与构建产物一致。原 user-runtime/DO 镜像保持 `127f15ba…`。

agent 镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:e9f8511a7a45077ea57c832740deab8f08d71c0b48a6ea9580e6dc7c29f36cd8`。

构建使用当前已审阅的未提交 agent 代码，基于此前 agent 镜像只替换 cell-agent 二进制；不重建/替换 workerd。二进制 SHA256：`0d67eb060b32d59fa3640656481d5f094838eb436b2b6e84cc91394f71220cc8`。

## 验证

- bucket/config/lease 全包 race，通过。
- agent/owner/server 回归，通过；双进程 session 恢复 E2E 通过（默认 FS 模式，非开启回收的完整多节点 E2E）。
- 真实 HK COS authority 全套通过。
- USW 同桶仅发现两个活跃节点，与部署两个 agent 对应；namespace 无 Job/CronJob。
- USW 独立且未注册的 probe scope：validate 换代保留 g0 stub，reclaim 换代物理删除 g1，validate 缓存读者读到 g2，旧 token 拒绝。探针的确切对象在结束后清理；未删除业务根或批量历史分段。
- 第一阶段启动期间一次续租确认 timeout，之后恢复；阶段一观察期无新超时。滚动期间公开 KV 均返回 200，但包含约 3.9–5.4s 的接管/冷路径请求，不能称为性能无回退。

## 边界与回退

只启用正常换代回收。历史 backlog、删除失败和延迟重建垃圾的后台补扫尚未接入；永久根继续保留、不能删除/重置，代数不复用。当前探针验证同桶物理删除，不代表已等待业务节点的自然小时换代，也不是全量业务 E2E。

回退只能改为 validate 并保留此次或后续支持根验证的二进制；禁止回到 legacy 或旧版本。安全配置副本：`/var/backups/cellhive/usw-gn-20261111/values-validate.yaml`。

证据：`/tmp/cellhive-gn-usw-{unit,regression,cos,session,probe,validate-upgrade,reclaim-upgrade}.log`，构建/探针源码和部署前配置在 `/var/backups/cellhive/usw-gn-20261111/`。

最终验收：两 agent 均 Ready、restartCount=0，模式 reclaim，二进制 SHA256 匹配。第二阶段 agent-0 启动期同样出现一次续租确认 timeout，随后自行恢复；最终最近 60 秒两节点无新续租失败/隔离日志。公开 KV 10/10 HTTP 200、响应 ok=true，requests 从 131 到 140 严格递增；本机到 USW 的请求耗时约 0.48–0.71s（非生产内网基准）。最终 Helm diff 为空。记录：`/tmp/cellhive-gn-usw-final-kv.json`、`/tmp/cellhive-gn-usw-final-diff.log`。

## 补充异常场景验收

- bucket/lease 定向测试启用 race，重复 30 轮通过：延迟旧写/旧 seal/初始化、旧段复活、读取根失败、读后根校验、并发读与回收、分页边界、部分删除失败后重试、续租确认及过期隔离。
- HK 专用 COS 桶 authority 7 个子场景重复 3 轮通过，包含真实旧段删除、重建、拒绝旧 token 和显式补扫。
- 扩展真实 COS 续租异常测试为 legacy/validate/reclaim 三种模式，每种覆盖首次发布/续租与换代/不换代四个组合，共 12 个组合，重复 3 轮 race 通过。注入的是提交结果不确定的完成顺序，不代表复现真实网络故障。
- USW 仅检查健康及公开 KV：两个 agent Ready、0 重启，最近两分钟日志未见续租错误；5/5 HTTP 200，计数 147→151，耗时 0.461–0.777 秒。未对生产注入故障。
- 测试函数响应包含敏感环境变量；证据文件只保留 ok/cacheHit/requests。该函数的响应泄露应另行修正并轮换已暴露凭据。

证据：`/tmp/cellhive-gn-exceptions-cos.log`、`/tmp/cellhive-gn-exceptions-usw.json`。本轮仅扩展测试、补充报告，未修改或重发生产二进制。历史垃圾后台补扫和永久根增长仍是未解决边界。
