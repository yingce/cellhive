# USW epoch Fold 发布记录（2026-11-23）

用户授权先本地验收、再发布 USW。只操作 USW，SG 未部署；运维仓原有 SG values 未提交改动保留。

## 发布

- 源码：`351c23b7`，已推送 `origin/main`；包含此前未提交 Fold、纯读 continuation、重开 owned 登记、空链休眠、bucket-proof 和测试修订。临时测试目录不在提交或镜像上下文中。
- 从 `git archive 351c23b7` 构建，经 `/opt/release-tools/release-image.sh` 推送；没有复用 dirty 本地镜像。
- 镜像 tag：`sgccr.ccs.tencentyun.com/vwork/cellhive:20261123-cellhive-351c23b7-epoch-fold`。
- 镜像 digest：`sha256:f6e14b943dff6b973fc5567c3204e8d9461747416c1bdf1ae7b3b59f24d3a59e`。
- 旧 digest：`sha256:9e4c5febf4614372e6f9697e7bfa8e4654b57d63534456a3fe9f9fbd99f56438`；如需回退，保持现有 authority reclaim/validated 协议和存储配置，只通过正常发布流程回退到兼容镜像。
- `/opt/cellhive-helm/environments/usw/values.yaml` 固定新 digest；preflight 通过，render diff 仅三个 workload 的 image 与 generation 改变。
- `/opt/cellhive-helm/bin/cellhive-helm usw upgrade` 完成，Helm revision **131→132**，两个 OnDelete agent 顺序替换，两个 DO runtime 与两个 user-runtime 完成 rollout。
- 复用旧线上镜像的 workerd，SHA256 `afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`。本机 `deploy/workerd-fork/workerd` 是不同版本，未使用。线上 host.js 与提交源码均为 `d77ea83c9e3c82fe39c6d67b5e6abc6d42cf120bcd22e81205a2de7dddd42421`。发布后重新检查两者均保持不变。

## 真实读写与回收

独立 namespace `epoch-release-20261123`，仅创建验收数据；未删除业务数据或强制调用业务 scope 的 GC。

- 发布前 KV CHECK 写入 20 个值，发布后逐一读取一致；新增 20 次写入和读取通过。另经 agent-1 验证原 20 个值，覆盖非 owner 请求转发。
- 新 Worker `runtimecheck` 的原生 DO CounterA 完成 SQL seed=132、increment=133、read=133。
- 注册仅用于 Host 路由测试的 `.invalid` 域名后，透过 user-runtime 服务端口的真实 Worker 请求返回 HTTP 200 与预期正文。这验证服务内部路由，不代表公网 DNS/TLS 验收。
- TEST 路径：`cellhive-usw-1376795072/cells/vwork-74/__kv__/TEST/ltx/`。
- 发布前 e14：605 个 L0＋3 个 L1，共 608 个对象；e15：22 个对象；e16：8 个对象；`retired_below=15`。
- 发布后对 TEST 仅执行 KV List（HTTP 200），接管为 e17。03:04:51 UTC 日志确认自动 continuation，cut=27416；03:04:51.680 UTC Fold 完成，max_txid=27417。
- **03:07:58.947 UTC** 后台 GC 记录 `scope=vwork-74/__kv__/TEST epoch=17 objects=630`。随后真实 COS List 确认 **e14、e15 均不存在，仅保留 e16/e17**，`retired_below=17`。没有手工缩短宽限、修改 marker 或调用 admin GC；旧对象已满足默认年龄条件。回收后 TEST List/GET 均返回 200，经两个 agent 读取同一键的结果一致。

## 告警与验收边界

- `vwork-1/__queue__/jobs` epoch=29 持续出现 `maintenance cannot resume durable chain`。同时间日志显示 baseline/local=2、durable=1：维护路径按设计拒绝为本地领先状态自动建立 continuation，不回退全量快照。这个 queue 的跨代维护尚未收敛，本次不修改其数据，也不宣称根因或修复闭环。TEST 的回收不受此任务影响。
- 03:03:54 UTC agent-1 出现一次 lease publish 超时，保留剩余租约并重试；后续 Ready 与 session 保持。持续观察确认没有因该次超时重启。
- DO 探针出现两次 recovery pointer CAS contention，明确回退 `bucket-batch`，只有桶持久化完成才返回；DO 写读探针通过。
- `purge: retained / owner: caller is not the owner` 是拒绝非 owner 删除的保护日志，不能把 WARN 总数当成请求失败数。
- 最初 port-forward 仍绑定被替换的旧 Pod，导致本机连接失败；重连新 Pod 后再执行验证。最初路由探针未注册域名，被 `host not registered` 拒绝，按正常 domain→route 流程注册后成功。这些失败不记为通过证据。

本机证据：`/var/tmp/cellhive-usw-351c23b7/`，含 archive、build-push、preflight、diff、upgrade、发布前后 COS 对象统计、KV/DO/Worker 探针、Pod/imageID 快照、结构化日志与观察记录。受限目录内的工具直接从 Secret 读取凭据，不把凭据写入报告。

## 最终观察

发布后连续观察 10 分钟，11 次采样中六个业务 Pod 始终 Ready、零重启、实际 imageID 均为目标 digest，11 次 KV 读探针全部通过。agent 日志未发现 ERROR；仍有上述 queue 安全延期告警，不宣称全部后台维护均已收敛。
