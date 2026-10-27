# Binding 刷新第一阶段（2026-10-27）

本地实现，未提交/部署。按 binding key + invalidation revision 合并前台/后台在途刷新；取消等待者不影响其他请求；失效后新请求不加入旧 flight，旧结果不回填缓存。保留原先已开始请求返回结果的语义。

不再为每次 binding 刷新强制失效 control owner 路由。所有本地 binding 读取（包括转发接收端）先点查权威 owner，核对 node/session/liveness；失去 owner 返回 409 触发已有重定位，查询失败 fail closed。保守保留每次实际刷新时 owner 侧的 COS 点查，因此不是完全移除 COS 热路径。TTL 仍为 1s、预热空闲窗口仍为 2s；资源 fence、token、持久化协议不变。

验证新增：并发刷新合并、等待者取消、已释放 owner 不读本地 binding、入口保留旧路由后的真实 HTTP control handoff；同时跑原有 invalidation 与绑定 owner 测试。加强本地 ownership 检查后，测试夹具直接写控制 SQLite 的场景需要显式 claim control，已补相应 fixture；不允许靠 unowned 本地副本授权。

HK COS 同机双 agent，6 轮，每轮 64 direct PUT / 64 forwarded PUT / 64 forwarded GET，交替 c1/c8、PUT 间空闲 2.2s。正式 1152 请求，失败 0，bucket-batch 增量 0。forwarded PUT 最大值依轮次为 119.957 / 12.431 / 51.295 / 15.186 / 42.455 / 28.131ms；c1 p50 6.558 / 6.550 / 7.272ms。上一轮最大 150.410ms，但短测、网络波动、上一轮启用全量 tracing 而本轮未启用，不能据此承诺稳定改善比例。仍有 120ms 长尾；后续版本化 binding 和进一步安全路由复用尚未实施。

本轮 app 删除 HTTP200，owner handoff released=18、peer released=1，均记录 stopped；旧退出 watchdog 竞态本轮未触发，不代表修复。

原始请求汇总：`binding-refresh-2026-10-27/requests.jsonl`。
完整日志：`/opt/.hermes/cache/scratch/hk-review-1793034510-6a2acf-8ogllg_m/0/result.log`，进程日志及保留数据：`/opt/.hermes/cache/scratch/cellhive-e2e.QTWbmW`。

最终验证：`go test`（项目 SQLite tags）`./internal/server ./internal/owner` 通过；binding/本地授权/control handoff/owner HTTP 路由专项 `-race -count=3` 通过；两包 `go vet`、`git diff --check` 通过。cell-agent 新二进制构建成功并用于上述 HK 实测。未执行整个仓库测试，不覆盖本工作区其他未提交功能的验收。
