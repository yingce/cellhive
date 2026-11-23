# 借鉴 celld 的后台读取复用（已部署 SG / USW / GZ）

本批只改变本进程调度与发现，不改变 COS owner/epoch 仲裁、恢复记录格式或复制耐久证明。celld 的节点级恢复摘要需要单独的跨版本协议迁移，本批不实施。

## KV

删除 pinned KV 的定时授权/资源 fence/路由预热。请求仍先校验签名、token 到期和作用域，再使用已有 BindingCacheTTL（从查询开始计时）的授权缓存；过期后 singleflight 查询权威控制面，失败不返回过期值。本地控制变更仍失效缓存。相同 namespace/kind/name/issuer/worker/version/resource 的 token 共用授权结果，token 到期字段不参与授权缓存键，但每次请求独立验证到期。取消请求不接受缓存。资源删除和 owner 检查仍走实际请求路径。代价是 TTL 过期后的首请求等待校验；不扩展现有远端撤权窗口。

## Recovery

增加显式 filtered resolved discovery：仅调用者已经证明不可逆终态的逻辑 key 可跳过权威读取；普通 List/GET 语义保持不变。nodelog 在成功解码、身份与 key 一致且 status=sealed 时记录进程内终态集合（上限 10000，满后不添加，退回重复读）。后续全局/单节点发现都在 provider 读正文之前排除这些 key；新 session 使用新 key，必须读取。session ID 不可复用、sealed 不可重新打开，延续已有协议；不依赖 key 缺失/错误证明完成。重启缓存清空，恢复中或损坏记录不缓存。缓存仅优化发现，显式 Get/Recover 仍读权威记录。底层不支持过滤时继续普通 List，保持正确性但可能仍有 List 隐含 GET。

## Lease

新增只供 discovery 使用的有界观察接口：复用最近最多 1 秒（或 TTL/10，取更短）的成功权威观察，且 lease 必须覆盖下一次已知节点刷新间隔；否则读桶。过期、缺失、未知、读取失败都不当作可复用的存活证明。NodeLeaseFresh 与所有接管校验保持原样。发现恢复候选后、封存前额外权威点查 lease，避免发现读取的新 session 被缓存的前任 lease 误判为历史 session。复用可能稍微推迟发现换代，但不以过期缓存授权接管。

## 验收

KV：缓存请求计数、token 到期独立校验、不同授权身份隔离、TTL 到期/撤权/失败/取消，以及无定时器注册。Recovery：真实 provider 统计第一次 sealed 验证与后续零 GET、新 session、解码错误、重启冷扫描、普通 List 未受影响。Lease：新鲜成功观察复用、临近到期/旧观察/失败强制读取、NodeLeaseFresh 始终读取。运行相关 race、全量 test/build/vet。本地与独立发布分支验证通过，生产验收见下方发布记录。

## 本地验证结果

- `make test` 最终工作区全量通过（`/var/tmp/celld-reuse-full-final.log`）；`make build`、`make vet` 通过（`/var/tmp/celld-reuse-build-final.log`、`/var/tmp/celld-reuse-vet-final.log`）。
- bucket/objectstore/nodelog/lease/recovery/server 的相关 race 测试通过（`/var/tmp/celld-reuse-race.log`、`/var/tmp/celld-reuse-race-final.log`、`/var/tmp/celld-reuse-discovery-race.log`）。
- 相同授权身份的 100 次请求（不同 token 到期时间）没有新增控制 owner 读取；过期 token 仍在 HTTP 认证层被拒绝；缓存过期重新读取；取消、撤权、授权身份隔离及读取失败测试通过。
- Provider 真正的分段 authority 发现测试：全部 key 已终态时零 authority GET，部分过滤只读取剩余 key，普通 List 保留完整校验。nodelog 测试覆盖冷重启、新 session、损坏正文和身份不匹配；objectstore 验证前缀隔离。
- 有效 lease 观察可复用，但近到期/过旧/失败不授权恢复；NodeLeaseFresh 始终读取；新活跃 session 不会被缓存前任 lease 误封存。

已从独立发布分支部署 SG / USW / GZ；部署版本与实际采样见 [三地区发布验收](2026-11-24-three-region-read-reuse-release.md)。首次发现、进程重启和超过终态缓存容量时仍可能读取历史记录；不声称完全消除 recovery 扫描或节点 lease 读取。
