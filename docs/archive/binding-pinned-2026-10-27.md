# Worker KV 固定映射（2026-10-27）

## 行为

版本的 KV resource ID 随 worker/version 进入平台 HMAC token；Go 生成的 binding spec 和 JS loader 使用同一格式。agent 在验证签名、namespace/name/kind、registered-binding ACL 后，将固定声明放入请求 context，gate/fence 与最终 KV handler 都使用它。原 token 保持动态映射兼容。新声明仅允许平台精确 KV token，委托 token 不允许携带；跨 namespace、错误 class 和非法 scope 拒绝。

同名 KV 的 v1/v2 可以分别访问 old-cell/new-cell；即使当前 active 版本已变，旧版本也不会静默改为访问新资源。未带 ID 的旧 binding 仍走原兼容路径。保留 ACL 撤销，因此旧版本能否继续访问仍取决于当前授权，不等于永久有效。

## 边界

本次固定的是映射，尚未构建独立撤销分发协议。registered-binding 的 1s ACL 查询、资源 fence 与 owner 查找依旧存在，不能宣称消除长尾。热预热仍按现有规则工作。D1/R2/Queue 已按运行时资源名寻址；DO/service 等无改动。

发布必须先升级全部 agent，再升级 user-runtime / 重新加载 Worker。旧 agent 会忽略新字段，混合期间不能保证固定映射；旧运行实例的旧 token 也不会自动变为固定映射。本次未提交/部署。

## 测试

- Go HTTP handler 测试：两个版本固定到不同 KV cell，实际 PUT 后分别检查 SQLite 值；旧资源 fence 后旧 token 403；注册撤销后旧 token 403。
- token 正反例：固定字段签名往返、委托/缺失字段/非 KV/通配 name 拒绝；跨 namespace/class/非法 scope 拒绝。
- server/scopedtoken/user-runtime 全量通过；固定映射、binding、KV scope 专项 race 连跑 3 次通过；对应 vet 通过。
- JS loader 语法检查通过；实际抽取 scopedToken 函数，legacy/pinned 两种 payload 与标准 Node HMAC 结果一致。首次脚本因尝试重设 Node 只读 crypto 属性失败，去掉无须的赋值后通过。
- HK COS 本机双 agent：36 正式批次，153,960 请求（PUT 3,960 / GET 150,000），失败 0，正式 bucket-batch 提交增量 0。覆盖 direct/forwarded/Worker，c1/c8/c32，各两轮。脚本 exit 0、app DELETE HTTP 200、两个 agent 均 handoff complete + stopped。
- 仍观察到旧 token 转发 PUT 最大 708.260ms；未为该请求采集 trace，不做精确归因。固定映射不等于解决所有长尾。
- 原始汇总 `binding-pinned-2026-10-27/requests.jsonl`；完整临时日志 `/opt/.hermes/cache/scratch/hk-review-1793035584-0b3cd9-5f43zy0l/0/result.log`；保留数据 `/opt/.hermes/cache/scratch/cellhive-e2e.aBX4bc`。真实测试使用新 agent 和改动后的 JS loader。
