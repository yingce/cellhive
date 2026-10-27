# KV 完整双 agent 压测 trace（2026-10-27）

## 测试

本机双 agent + HK COS，1KiB/32keys，真实 HTTP keep-alive；两轮 c1/c8/c32，direct/forwarded/Worker PUT+GET，参数与上一轮一致。新增 PERF_TRACE 开关，只启用请求 traceparent、>=50ms 请求日志及6s导出等待，不切换为缩短的 performance-tail 模式。全量采样有额外CPU/IO开销，不能将延迟与无trace结果视为严格等价。

153,960正式请求，失败0；fleet提交批次增量821，bucket-batch0。启动/peer发现的bucket提交单列。

## 已确认的阻塞

### 转发 PUT 759.247ms

trace `525458209610c4b14c5132c29e0f1d51`，第1轮c1首条。

- peer HTTP758.610ms。
- binding_refresh660.046ms，其中peer owner_bucket_read41.668ms。
- 后续resource fence GET21.372ms、另一次owner GET36.657ms。
- 真正owner HTTP6.436ms，durability_proof6.067ms。

因此该极值主要在绑定授权元数据刷新，不是写日志的COS持久化等待。期间owner端另一个HTTP trace `ba125c89019a6dded7400b53930891a8` 为582.686ms，owner_bucket_read582.477ms，起止时间包含在上述binding刷新内，与控制面查询路径吻合；但合成control binding请求未显式Inject trace头，不能把这条跨trace时间关联当作严格父子证据。

### Worker GET 446.784ms

trace `e03e63570363f07c6263cb052fa3f299`，第1轮c8。

- runtime HTTP span444ms。
- agent KV HTTP439.787ms。
- version_authorization_refresh439.305ms。
- 同时间后台授权续期trace `5855e506300713eeb6823bf3004f8428`：owner_bucket_read1000.279ms，整个刷新1000.307ms，碰到后台1s超时。

背景刷新开始于17:54:33.206，前台请求于33.807开始等待，后台于34.207结束；前台于34.246完成。与共享flight等待剩余约400ms、后台超时后重新校验的代码行为一致。长期压力证明此前短测“只有首次请求同步授权”的结果不能外推：后台权威查询慢于剩余授权有效期时，前台仍必须fail closed并等待刷新。

### 尚未完全解释的延迟

Worker PUT最大130.811ms，trace `ea437f470aecc72427160f14ef422261` 仅匹配到33ms的runtime span，未关联到完整agent调用。并发Worker部分trace还出现重复agent请求或只有runtime span，需检查global trace context在并发下的传播；不能把剩余约98ms直接归因SQLite/COS。未观察到direct PUT/GET或forwarded GET超过50ms的正式成功请求。

## 下一步建议

优先处理control owner的权威COS点查频率：授权续期当前仍调用localBinding中的Owner.Resolve。需要基于本地持有的ownership/epoch、同session健康node lease以及release/handoff的即时失效来证明可安全读取本地控制SQLite，然后才可能复用该证明。不能直接删Resolve或延长授权TTL。必须覆盖lease过期、同名重启、handoff、撤销以及CAS失败。

单纯增加预热频率不能消除1s级COS波动；继续后台续期只是将正常查询移出请求。旧token的直接HTTP binding查询仍需独立优化。后续补control合成请求的trace注入与租约读取span，才能精确拆分所有binding刷新时间。

## 退出与产物

脚本exit0、app DELETE HTTP200，但清理并非完全健康：peer handoff complete released0；owner再次触发既有 `node lease permanently fenced; exiting without handoff`。保留恢复目录，未修复该退出竞态，不以测试PASS表示handoff成功。未提交或部署，仅改测试脚本追踪开关。

请求汇总及选定span位于同名目录；完整trace `/tmp/cellhive-full-perf-spans.jsonl`；完整结果 `/opt/.hermes/cache/scratch/hk-review-1793037187-d78345-rmijjl6a/0/result.log`；保留数据 `/opt/.hermes/cache/scratch/cellhive-e2e.KGVMNG`。collector已停止。
