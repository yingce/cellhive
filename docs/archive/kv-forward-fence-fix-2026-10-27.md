# KV 纯转发准入与 fence 查询合并（2026-10-27）

状态：此项修复已完成本地与HK验证，未发布。基线3e448de，隔离分支perf/data-path-optimization。

## 改动

- scopeAuth仍在入口完成token签名/有效期、namespace、kind/name及原有binding授权。原生KV GET/PUT/get-batch可先尝试远端转发，实际转发时不做入口本机fence/旧副本清理/SQLite gate；目标仍完整准入。无路由或本地执行保持原顺序；内部转发头不能跳过资源校验。其他API不改变顺序。速率准入由执行节点处理，入口纯转发不重复消耗写入配额。
- 同scope普通fence缓存miss与后台刷新共用in-flight，协调表最多1024项，溢出回普通点查。失败不续期，等待者独立取消，leader取消允许其他请求重试。没有新增异步持久化窗口、没有改变ACK条件或1秒TTL。
- 修复新回归暴露的旧问题：查询途中发生本地删除导致fenceRev改变时，旧实现仅阻止缓存回填，仍将旧fence返回当前请求；现在重新读取。删除CAS等写侧Fresh读取不参与合并。

## 验证

旧版本Go overlay运行两条回归失败：入口fence不可用导致503无法转发；删除期间前台读取返回删除前状态。修改后replica/server三轮race通过，覆盖8个并发查询合并、前后台合并、leader/waiter取消、删除期间重新读取、非法token/namespace入口拒绝、owner拒绝删除资源、已转发请求仍准入。既有owner/删除重建/授权测试包含在全包运行中。

make build、make vet、make test均退出0；全仓使用pinned stock workerd。真实HK使用平台workerd fork。证据保存在同名目录。

## 真实 COS trace

运行 `python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance-dual --port-base 29500 --trace-endpoint http://127.0.0.1:28480`，153960热请求、0失败，fleet=835批、bucket-batch=0。app清理DELETE200。Trace完整文件 `/tmp/cellhive-fence-fixed-trace.jsonl`，测试目录 `/opt/.hermes/cache/scratch/cellhive-e2e.ciFaF6`。collector已停止。Wrangler附加测试未配置而跳过，不影响KV运行覆盖。

遍历全部已采集span：peer HTTP下的resource_prepare为0，peer fence后台读取仍有50次。先前835ms中794ms的入口fence等待路径已移除。

不宣称热写长尾全部解决：修复后仍有567ms样本，trace `54d70e93b7dd1922cf7f22af3ad5c1f3` 中binding刷新34.854ms、owner_bucket_read403.850ms、owner HTTP18.601ms，其余并发请求共享route刷新等待。这是owner路由元数据更新路径，下一步需独立优化lease-bounded route提示复用与权威校验，不可跳过session/epoch校验。

本轮与部分Go检查同机并行、全量trace有较大开销；只用于功能与阶段定位，不作为严格前后RPS/p99收益验收。未修改runtime trace关联缺口，不能声称Worker全部长尾已定位或修复。

## 真实 COS 故障与生命周期验收

`--suite batch-recovery`：owner SIGKILL后100/100已ACK值恢复；survivor drain后清除本次测试SQLite缓存，COS冷恢复100/100完整，首GET2.143115s。结束时spool仍有13条未完成上传，恢复验证仍完整。该场景compaction周期1小时，不代表合并竞争验收。

新增 `--suite routing-safety`：双agent真实HTTP验证转发GET/PUT/get-batch、无效token与跨namespace拒绝；删除后等待既有1秒缓存界限，两个节点GET/PUT均403；同名重建后两个节点均读不到旧key，新写跨节点可读。平台旧式namespace token按现有契约可以访问重建后的当前绑定，此测试不声称其永久失效；version-pinned授权由原有Go回归覆盖。

两场测试应用清理DELETE200。首次routing-safety运行采用 `/tmp/fence-routing-hk.py` 固定runner副本（仅根目录改为绝对路径，其余内容与新增suite完全相同），避免修改正在运行的原runner；可复现命令为 `python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite routing-safety --port-base 29500`。

已验证的修复只针对入口资源准入与fence并发查询，不包含owner路由长尾消除。未推送、未跑分支GitHub CI、未部署。
