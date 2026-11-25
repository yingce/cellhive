# 平台 JS 四项修复本机部署（2026-11-25）

用户指定仅本机 Compose。代码 `23d6821f` 已推送 `origin/main`，从该提交的干净归档构建；部署到既有 `cellhive-migration-test` 栈。SG、USW、GZ 未操作。

## 产物与发布范围

- 镜像：`cellhive:local-23d6821f`。
- 实际 image ID：`sha256:59c267465b0e2fbbde3e1225f2aabc4571642f30c632debf7700668f114b0922`。
- 更新两个 cell-agent、user-runtime、do-runtime；先逐个更新 agent 并等待 healthy，再更新 runtime。
- 复用旧本机镜像作为运行基础，替换归档编译的四个 Go 程序及平台 JS。workerd SHA-256 保持 `afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`；没有使用工作区另一份 fork 二进制。
- 当前 main 同时包含此前尚未在本机镜像发布的 `6ec5b4cb` invocation 日志修订；未修改数据格式，无迁移或重新初始化。
- Compose 仅改四个 service 的 image。Redis/MinIO 容器 ID、所有挂载、现有端口和配置均与发布前一致；未删除或重建数据卷。旧镜像及私有 Compose 配置备份保留，可供回退。

## 验证

- 发布前回读 20 条历史 KV，通过。
- 将本机现役 workerd 复制到独立路径，针对新源码重新运行 userruntime、doruntime、plangen 整包测试，全部通过（38.306s / 36.084s / 0.173s）。
- 四个新业务容器均 healthy，二进制与 bindings/facades/log-tail JS 哈希匹配归档构建；现役 workerd 哈希不变。
- 发布后 20 条历史 KV、5 条新 KV 写读、Queue 首次消费、显式重试后第二次成功、DO Alarm、新 Cron tick 均通过。Queue 结果尚未产生时的轮询 404 是等待过程，最终结果成功。
- 独立 canary namespace `jsfix-23d6821f`：模块顶层捕获 imported env 的 R2 binding 正常，1 MiB 对象完整读回校验通过；text 消费后 bodyUsed=true，重复读取拒绝。512 KiB Assets GET 内容一致，HEAD 经 RequestInit 传递后返回 200、正确 Content-Length 和空 body。
- 这些是本机 Compose 的真实路径验证，不是云端或跨主机验收。没有在运行栈制造 OOM；Tail 预算及并发 trace 的压力/交错证明见修复验收报告中的隔离回归。

## 稳定性观察

10 分钟观察通过：600 秒内共 11 次采样，两个 agent、两个 runtime 及 Redis/MinIO 始终 healthy，重启次数均为 0；真实业务 `/status` 请求均成功，Cron 每次采样持续推进。观察末段再次验证 R2 消费状态及 Assets HEAD，均通过。结构化日志未发现 ERROR/FATAL，仍有下述后台告警。

启动时记录了 owner unowned、短时 peer 不可用及一次 shared schedule index degraded；业务验证随后通过。仍见 epoch fold deferred、旧 Queue 资源 control not found、非 owner purge retained 和对象存储 SDK checksum 提示。本次未修复这些告警，不能把服务 healthy 等同于没有历史后台问题。

## 证据与边界

证据目录 `/tmp/cellhive-local-23d6821f/`：包含 build、live-workerd-tests、rollout、artifact-proof、business、canary、observations 和日志汇总。目录中的 compose-before.json 含私有配置，不得公开整个目录。

本轮 R2 修复消除平台 JS 的全量缓冲，agent 的 R2 Snapshot.Get/GetRange 原有聚合路径仍保留；不宣称端到端零缓冲。滚动重建期间可能短暂影响请求，未测量连续外部可用性，不宣称零停机。
