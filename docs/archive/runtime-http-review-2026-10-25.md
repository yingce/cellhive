# user-runtime 合并审查与 DO HTTP 修复（2026-10-25）

基线提交 2b83230。审查开始时已有 ADR-287 user-runtime 未提交改动，本轮保留这些改动并一起验证。最终发布包含 ADR-287/288/289，版本及验收见文末。

## 已有 user-runtime 改动的审查

- HostWorkerPtr、pool FunctionKey、dedicated target、可信内部头增加 storage_id，避免删除重建后相同 version/SHA 复用旧 env/实例。
- 固定身份与 active 查询缓存隔离；旧 404 不污染其它身份，错误不回退另一版本；旧缓存可供已固定身份的在途请求使用。
- host refresh 的 fetch/body 共用 5s deadline；AbortController 与 generation/route epoch 防止超时请求覆盖新的结果或清除新的 flight。
- bindings 端点使用已解析 version 取配置，内部 loader 将返回代际加入 ID。没有完整代际的 legacy lookup 不永久缓存。
- 保留并验证上述实现，未发现需要撤回的逻辑；尚未统一消除 legacy metadata fail-open、SHA-only service 环境不完整 pin、控制面多次读取之间的快照竞态。不能由本轮测试反推某个历史生产请求命中了这些问题。

## DO 修复与失败证据

1. deleteAll 误把原生 Map 当成 cursor page，300 keys 实际剩 44。本轮重复删除第一页直到不足一页，并删除所有 alarm shim 元数据；不新增并发写入下原子清空保证。
2. 共享 DO facade 和 host 多次 text()，原始 00 ff 80 41 变为 00 ef bf bd ef bf bd 41。请求采用 body_base64，响应保持字节，旧文本 body 仍可读。
3. host、Go proxy、facade 只保留 Content-Type。现保留租户头及多值 Set-Cookie，剥离内部标记/逐跳头；302 在各代理边界不自动跟随，204/205/304/HEAD 不构造非空 body。
4. Go renewal 无 deadline，挂起 8s 后测试失败；JS 一条租约抛错中断整轮，旧失败还会删除新 claim。现本地调用 5s 超时、单 scope renew fetch/body 2s 超时，异常继续下一项，并核对 claim 对象身份。

## 验证边界

- 旧实现失败日志保留在同名目录；JS 测试执行真实平台函数，外部 fetch/时钟由 harness 控制。
- TestPublicDOHTTPRoundTrip 启动 3 个真实 workerd 进程：user-runtime → DO binding → forwarding do-runtime → owning do-runtime → 原生 facet。覆盖原始二进制请求/响应、租户响应头、重复 Set-Cookie、内部头过滤、204、302、418、300 keys deleteAll。控制面、lease 服务及 agent forwarding 是本地 stub，不是线上 COS/RPO 故障验证。
- cell-agent 的真实 Go proxy handler 另外覆盖字节、重复 Cookie、Location、302 不跟随及内部头剥离。
- 续租测试覆盖挂起后下一轮可继续、单 scope 失败不阻断后续、迟到失败不删除替代 claim、响应体挂起超时。
- user-runtime 身份隔离 JS、真实 workerd 重建身份测试、真实 reverse proxy 可信头测试沿用已有改动。
- 仍是缓冲的 DO fetch，沿用约 8 MiB + 256 KiB envelope/响应上限；base64 请求占用约 4/3 原始字节，实际可传原始请求体受 envelope 剩余容量限制。本轮没有吞吐基准或线上故障注入。
- 新字段不能由旧 host 解码。发布需暂停/排空 DO 流量，完成所有 do-runtime 与调用方升级后再恢复；do-runtime 自身也可作为 DO 调用方，不能把普通混合版本滚动发布称为安全。

## 本轮验证结果

- 45 项 JS 回归通过（身份/路由刷新、response 生命周期、DO HTTP、续租）。
- `CELLHIVE_WORKERD=deploy/workerd-fork/workerd make test` 全量通过；首次执行仅旧 DO binding 测试的文本字段断言不符合新 envelope，更新为固定 base64 字节断言后重跑通过。
- `make vet`、`make build` 通过，JS syntax 和 `git diff --check` 通过。
- 真实 HTTP 测试通过；其最终重复 Set-Cookie/内部伪造头验证也包含在全量 Go 回归中。
- `go test -race -p 1 -count=1` 的 doruntime/server/control/userruntimepool/workerdpool 五包全部通过，见 race.txt。

## 发布前追加验证

- 精确 staged 导出首次全量测试遇到临时端口 Address already in use；原快照重跑全量 Go、45 JS 和 vet 通过。
- SG/USW 旧版本均在 agent-0 返回 worker has no such Durable Object binding，而 agent-1 能读取新 Worker。新增真实双 Go server 回归复现 403；修复改为向 control owner 点查 active Worker，保持原 DO invoke 代理路径及拒绝非法 class。

## 最终发布与恢复补丁（2026-10-25）

- 业务提交：`629072e`（runtime 身份/HTTP/续租及控制 owner 查询）和 `004401c`（DO capture 恢复 fence、失败 LTX 保留）。均已推送 main。
- 最终镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:a49a31196cf71791308fe705a5a86c27a8c46c09ece0878792d87208032b0a4f`；构建源树 `43088d6355f53a5bff3030864056020f5797660f`。
- 本地 Compose 三服务 healthy；SG Helm revision 58、USW revision 47，各 6 个业务 Pod Ready、restartCount=0，spec image 与运行 imageID 均核对。部分 CRI 的 status.image 返回 config digest，因此以 spec image + imageID 核对发布 digest。
- 每区先停止 user-runtime、do-runtime 并确认旧 Pod 不存在，随后通过 `/opt/cellhive-helm/bin/cellhive-helm` 发布固定 digest。cell-agent 逐个更新，保留数据卷。HPA 未在停止阶段重新拉起调用方。
- 三环境 KV 升级前值保留、新 PUT/GET 通过；DO 升级前值保留、非 UTF-8 二进制、重复 Set-Cookie、租户头、302 不跟随、300 keys deleteAll 全部通过。SG/USW 另外通过两个 agent 分别读取同一 DO，验证 follower 控制数据误拒绝问题。
- SG/USW 旧版本公开 DO canary 被 follower 的本地 Projection 误拒绝，升级前持久化基线由 scoped DO API 经控制 owner 建立，仍执行绑定鉴权；升级后从公开 user-runtime HTTP 入口验收。脚本见同名目录。

### 发布验收追加修复

SG 初次发布 revision 57 后，旧 DO 数据恢复输出门持续报 `snapshot commit (baseline): ... epoch_mismatch`。capture LTX epoch 固定为 1，而 cell-agent ownership epoch 已增长；ADR-289 分离这两个字段，并在接收及最终提交边界保留 capture-owner 和 DO-shard 双重 fence。普通 cell 的 stale epoch 仍拒绝。旧代码上的真实双 epoch HTTP 回归失败日志为 `capture-epoch-red.txt`。

追查提交失败恢复时，真实 SQLite 回归还复现了 WAL cursor 已推进、下一次 gate 遗漏未确认 delta 的问题。supervisor 现在保留原始 LTX 字节及快照剩余分片，后续 gate 先以相同字节补确认，再 Poll 新 WAL。测试覆盖 baseline/delta × 提交前拒绝/提交成功但确认丢失，之后继续写并从对象存储恢复，结果正确。重试的是不可变 LTX，不是租户操作。

最终 `make test`、`make vet`、`make build` 通过；四个相关包的目标 race 测试连续 5 轮通过，见 final-*.txt。另修正已有 cellcapture 测试的退出同步：Wait 的错误发布先于 Run 的 Done 关闭，断言 stopped 之前必须等待 Done；重复 100 次通过。45 项 JS 及此前五包全量 race 证据保留，最终补丁未更改 JS。

### 验收边界与日志

- 本轮是功能/恢复验收，不是 RPS 或延迟基准。验收中的首次 KV PUT 包含冷恢复，USW 一次约 1.07s；不能据此宣称热路径性能已改善。
- 两区出现少量 `DO recovery pointer CAS contention`，输出门回退为等待 bucket 确认后成功，没有放宽 RPO=0；该竞争与耗时尚未优化。
- USW 滚动期间记录过 invocation persistence 502 和 cron dispatch 500；发生在 cell-agent 全部 Ready 之前。UTC 03:08:20 至 03:09:32 的最终日志窗口未再出现 error/warn/failed/panic/fatal；这只是观察窗口，不是所有 cron 的端到端验收，不能将短维护窗口称为无中断发布。
- 不声称覆盖真实 COS 大规模故障、所有调度任务、长时高负载或全部 metadata fail-open 兼容边界。
