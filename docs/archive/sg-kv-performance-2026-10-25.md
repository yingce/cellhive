# SG KV 分批性能实测 — 2026-10-25

## 范围与方法

- 时间：2026-10-25 20:54–20:58（Asia/Shanghai）。SG 真实集群、两个 cell-agent；无发布、无重启、无运行参数修改。
- 通过 kubectl exec 在 agent Pod 内运行 Go HTTP 客户端，直接请求各 agent 的 Pod DNS :7001；不含公网、Traefik、user-runtime/SDK 开销。客户端与服务共用 Pod，不能据此推断独立压测机的容量上限。
- 独立 namespace `ops-sg-kvbench-20261025-b1`，KV 绑定 `KV`，scope `ops-sg-kvbench-20261025-b1/__kv__/bench`。1 个 cell，100 个 key 循环覆盖，值为 1 KiB。不是多 cell 容量测试，也不是不同 payload 大小测试。
- owner 前、中、后均为 agent-0、epoch=1、同一 session，resolve 返回 expired=false。target=0 为 owner；target=1 转发给 owner。
- 使用 cmd/kvbench 的临时副本，增加 max、响应体读取错误检查、PUT ok/applied 检查和 GET 全量字节比对。每组独立 HTTP client，复用连接；warm=10（并发4，不计入统计）；首次请求 warm=0。超时30s，脚本遇到失败即停止。
- 第一阶段：首次 PUT/GET 各1次；串行每客户端3轮，每轮分别测试两个 target，PUT200次、GET1000次。
- 第二阶段：客户端0，并发4/8/16，每档2轮、每个 target/mode 各1000次，轮间休息2秒。
- 第三阶段：客户端0，并发8，3轮；每轮两个 target 各持续 PUT10秒、GET3秒，轮间休息3秒。按到期停止发新请求并等待在途请求结束；RPS=成功数/实际总耗时。
- 下表的区间是各批次统计值的最小–最大值，**不是合并样本的分位数**。GET/PUT 分开运行，不是混合负载。

## 结果

新建 cell 首次 PUT **918.76ms**，紧接 GET **0.84ms**（各1样本）。这是新 cell 初始化，不能代表已有大 cell 的冷恢复时间。

### 客户端0：分批结果

|阶段|并发|入口|操作|总请求数|每批 p50 ms|每批 p99 ms|每批 RPS|最慢请求 ms|
|---|---:|---|---|---:|---:|---:|---:|---:|
|串行3轮|1|owner|PUT|600|9.52–9.77|15.62–63.1|62–99|73.54|
|串行3轮|1|owner|GET|3000|0.31–0.32|0.45–0.99|2840–3102|3.55|
|串行3轮|1|转发|PUT|600|10.3–10.8|21.02–66.18|81–89|86.84|
|串行3轮|1|转发|GET|3000|1.59–1.62|2.29–3.4|548–581|76.14|
|短批2轮|4|owner|PUT|2000|9.91–10.05|18.57–105.46|191–383|108.6|
|短批2轮|4|owner|GET|2000|0.36–0.53|4.89–7.33|5344–5485|19.81|
|短批2轮|4|转发|PUT|2000|11.47–11.49|125.16–133.47|173–236|236.9|
|短批2轮|4|转发|GET|2000|1.61–1.65|4.65–5.08|2149–2325|13.09|
|短批2轮|8|owner|PUT|2000|11.06–11.68|22.1–97.05|474–650|116.47|
|短批2轮|8|owner|GET|2000|0.71–0.76|9.63–10.22|5520–5761|19.01|
|短批2轮|8|转发|PUT|2000|16.85–18.02|111.39–211.1|200–417|242.37|
|短批2轮|8|转发|GET|2000|1.83–1.95|4.94–5.52|3566–3800|8.38|
|短批2轮|16|owner|PUT|2000|14.54–15.04|133.01–143.38|743–906|171.4|
|短批2轮|16|owner|GET|2000|1.94–2.11|13.59–16.39|5857–6334|23.6|
|短批2轮|16|转发|PUT|2000|18.69–19.1|104.66–116.94|758–775|124.95|
|短批2轮|16|转发|GET|2000|3.12–3.38|10.28–15.31|4246–4331|27.07|
|持续3轮|8|owner|PUT|12096|11.04–12|88.43–104.79|326–494|147.8|
|持续3轮|8|owner|GET|55647|0.96–0.98|7.78–8.16|6100–6246|29.51|
|持续3轮|8|转发|PUT|9144|15.74–15.9|163.81–200.04|241–338|440.97|
|持续3轮|8|转发|GET|31885|1.8–1.85|4.78–5.39|3400–3586|147.6|

### 换客户端位置的串行对照

客户端在 agent-1 时，访问 owner 的 GET p50=0.80–0.84ms，访问本机再转发的 GET p50=0.92–0.98ms。客户端0的对应结果为0.31–0.32ms / 1.59–1.62ms。客户端位置改变了网络路径，不能把两个入口的差全部当作纯转发代码成本。

客户端1的 PUT 到 owner p50=9.65–43.68ms、p99=15.91–98.67ms；经本机转发 p50=9.88–10.31ms、p99=19.64–117.19ms。不同批次持久化抖动明显，不能据此称转发更快。

## 持久化证据与结论边界

- owner GET 串行中位数约0.32ms；热 PUT 大多数批次中位数约10ms，持续负载下有明显尾延迟。热 PUT 不等于纯 SQLite 提交耗时，包含持久化确认。
- 持续8并发 PUT，服务端该 namespace 的 durability proof 平均耗时14.61–25.80ms；对应 HTTP 均值16.19–33.12ms。确认等待占据主要耗时。fleet 和 bucket-batch 计数均增长。
- 指标不能单独证明 bucket-batch 回退原因，也不能证明每个尾延迟都来自 bucket。应进一步按请求/提交关联 trace，排查 fleet 证明可用性和批次确认时延；本次没有修改逻辑。
- 短批16并发 owner PUT743–906 RPS，不是稳定容量承诺；持续8并发 owner PUT326–494 RPS，更能体现批间波动。
- 所有计入串行、短批、持续统计的 **147,172** 次请求零失败、零内容不符；另有首次2次和结束时双入口各100个 key 校验成功。预热不计入此总数。
- 热请求最大440.97ms；本轮未见热请求超过1s。不能由数分钟测试推断不存在低频长停顿。
- 最终100个 key 经两个入口逐字节验证通过。这不是重启/故障后 RPO=0 验证；本次未做故障注入。
- 抽样 top 中 agent0 CPU约0.3–0.4核，agent1约0.17–0.22核；抽样不是峰值。所有Pod最终Ready、零重启。agent日志检查结果见原始记录。

## 清理与复现证据

测试 binding 通过正式 DELETE API 撤销；cleanup job `ds_a09eaad3f1ab3df4816612044ce81654` 达到 `state=complete`、attempts=1。随后删除独立测试 app，返回 ok=true。两个Pod中的临时探针和采样文件已删除。

原始证据位于 `/tmp/sg-kv-bench-20261025/`：`setup.log`、`serial-client0.log`、`serial-client1.log`、`load-client0.log`、`sustained.log`、`results.json`、`stage-metrics.tar.gz`、`proof-summary.txt`、`observe-end.txt`、`cleanup.log`、`finish.log`、`pods-end.txt`。包含探针源文件 `probe.go` 和阶段脚本，未保存认证token。

持续 PUT 阶段指标差值（commit为节点级计数，proof为测试namespace计数；包含每阶段10次预热）：

```text
round=1 target=0 fleet_delta=619 bucket_batch_delta=66 proof_count=4953 proof_mean_ms=14.61
round=1 target=1 fleet_delta=699 bucket_batch_delta=53 proof_count=3351 proof_mean_ms=17.82
round=2 target=0 fleet_delta=325 bucket_batch_delta=112 proof_count=3269 proof_mean_ms=23.08
round=2 target=1 fleet_delta=700 bucket_batch_delta=57 proof_count=3396 proof_mean_ms=17.38
round=3 target=0 fleet_delta=431 bucket_batch_delta=107 proof_count=3904 proof_mean_ms=19.02
round=3 target=1 fleet_delta=436 bucket_batch_delta=106 proof_count=2427 proof_mean_ms=25.80
```

## 追踪复测（22:24–22:27 CST）

使用现有 Vector tap 读取 traces；只对隔离测试请求发送 sampled traceparent，没有修改全站配置、没有部署/重启。1 KiB、单 cell、8并发，先各入口25秒，再各入口15秒。四批32,520次PUT全部成功。后两批11,643个请求中，Vector tap得到11,387条可关联trace（tap是有界观测，不声称全量无丢失）。临时探针的trace ID使用随机ASCII字节，避免Vector tap JSON把二进制ID有损转成UTF-8；仍为合法W3C trace ID。

### 关键请求：不能将400ms全部归到持久化

|trace ID|客户端耗时|入口 agent1 span|owner agent0 span|durability proof|
|---|---:|---:|---:|---:|
|7372646f6c6674797474786364687570|455.960ms|454.623ms|18.189ms|17.901ms|
|6462656e67716d666d6c6e6271717a65|477.744ms|474.419ms|241.234ms|17.982ms|
|61726c7a676c736966796b6561737964（直连owner）|390.105ms|—|386.846ms|116.717ms|

第一条owner开始处理约在客户端发起后436.945ms；第二条约235.551ms。需要注意跨Pod时间比较受时钟偏移影响，但入口span与owner span持续时间之差仍显示大量耗时在owner处理之外。这些span没有进一步拆分DNS/连接等待、binding、fence、owner route、SQLite mutex，不能把差值精确等同某一个缓存刷新。

### 已确认代码缺口

`handleKVPut` 在 `forwardOrClaim` 已转发时直接return，末尾 `noteHotKVAccess` 不会运行。GET路径也存在同类早返回；本次回归只覆盖PUT。`noteHotKVAccess`是激活热KV后台刷新集合的唯一入口。因而只负责转发的节点不会为该cell主动刷新binding/删除fence；两者常规缓存TTL均为1秒，过期后走同步读取，fence miss还没有并发合并。

临时回归 `TestSGDiagnosticForwardedPutsActivateWarmCache`：注册合法binding，将KV owner指向HTTP测试服务，连续3次经过完整Handler转发，均200；断言入口热缓存已激活，按预期失败：`3 successful forwarded PUTs never activate cache refresh on ingress node`。这证明了代码缺口，不等于已逐段证明线上某条trace的436ms全部花在该缺口。

另一个需继续拆分的问题：owner直连390ms中，proof只有116.7ms，约270ms在proof之外，不能据此直接认定SQLite锁或checkpoint。现有trace粒度不足以精确区分binding/fence检查、capture Ensure和SQLite提交。

下一步修复方向：覆盖成功转发的热访问登记，并用双节点HTTP测试验证后台刷新；保持鉴权、删除fence和owner lease有效期规则。进一步添加这些入口步骤和本地写入的分段观测，再验证owner侧残余慢请求。本次仅诊断，没有提交/发布功能修复。

证据：`/tmp/sg-kv-trace-20261025/` 的 `joined.json`、`summary.txt`、`requests*.log`、`forward-diagnostic_test.go`、`forward-diagnostic-test.log`、`cleanup.log`、`finish.log`。临时测试文件已移出仓库；测试KV/app通过正式API清理。

## 转发热缓存修复（后续发布）

在 `forwardIfNonOwner` 完成成功响应转发后，为 scoped KV PUT/GET/get-batch 登记热访问，含 GET 404。失败响应不登记。只复用原有后台刷新机制，不改变鉴权、owner/fence有效期或持久化确认。回归覆盖七种成功/未命中/失败场景，实际HTTP转发后核验热集合及刷新后owner身份不变。部署结果将在验收后追加。

发布前验证：`make test`、`make vet` 全部退出0；`go test -race`（SQLite完整tags）对 `TestForwardedKVAccessWarmsIngress|TestHotKVAccess|TestSuccessfulPuts` 通过。Docker生产镜像构建通过，保持同一workerd fork二进制。首次诊断回归在未修复代码上失败、修复后通过。

## SG 初次发布与连接池问题

`bb03fef` 已推送并发布SG revision62，digest `48f2acd462b7e90b38d22f21dd80f6ff2782207cfa58d1b1988b5fcc9e6a4763`。六个业务Pod Ready、零重启、镜像一致；旧KV/DO持久值及DO行为验收通过。随后持续8并发GET出现2次失败；错误增强探针复测30s/45s分别出现3/18次HTTP502，响应明确为owner转发dial `cannot assign requested address`。agent-1 `/proc/net/tcp` 显示TIME_WAIT=28,261，可用临时端口32768–60999（28,232个）。不能将第一次发布视为性能验收通过。

原因：默认转发HTTP client使用默认transport，每目标仅2条idle连接，并发8时反复建连/关闭导致TIME_WAIT积累。修复：共享默认client，克隆默认transport，MaxIdleConns=256、MaxIdleConnsPerHost=128，保持30s超时及原重试边界，支持显式ForwardClient覆盖。真实HTTP并发回归（5轮×8，上一轮完全读完再开始下一轮）在默认池2时新建32条连接、按复用预期失败；池128时复用8条连接、通过。此修复不改变写入持久性。

连接池修复发布前检查：`make test`、`make vet` 均退出0；定向 `go test -race`（`TestForwardTransport|TestForwardedKV|TestHotKVAccess`，完整SQLite tags）通过。

## 最终 SG 发布验收

代码 `a8496c7`（含前一提交 `bb03fef`）已推送；镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:1ffeb257fece77306b7147df32888338eebba4222e690aa28806840f901ce1a7`。通过正式Helm脚本发布SG revision63，六个业务Pod Ready、restartCount=0、imageID一致。USW和本地本次未更新。

两次滚动发布后，原有KV/DO持久值、DO binary/header/cookies/deleteAll/redirect检查均通过。最终压测前，测试cell的100个1KiB值经两个入口读取全部正确；owner仍为agent0，新session/epoch3，expired=false。

最终同区HTTP、1KiB、单cell、8并发（warm10不计入）：

|入口/操作|持续时间|成功数|失败|p50 ms|p99 ms|max ms|RPS|
|---|---:|---:|---:|---:|---:|---:|---:|
|owner PUT|15s|5884|0|11.95|100.65|526.46|392|
|转发 PUT|15s|6590|0|13.18|114.13|264.36|439|
|转发 GET|90s|410208|0|1.59|4.11|284.49|4558|

总计422,682次计量请求无失败。入口agent1的6次TCP采样中TIME_WAIT为28、23、16、27、24、33；ESTABLISHED为15–24。未再复现临时端口耗尽/502。发布前10秒转发PUT基线387 RPS/p99 156.72ms，GET3400 RPS/p99 4.88ms；最终测试时长和进程状态不同，不能把单组差值当作严格A/B或稳定容量保证。

owner热PUT仍有526.46ms尖峰，转发GET仍有284.49ms尖峰；本次只确认热访问登记与连接池复用问题修复，未宣称消除所有尾延迟。

测试结束再次从两个入口逐字节核验100个键，然后通过正式API删除binding和app。cleanup `ds_9daded439ac671ea2c41bcf4f30aeec1` 达到complete、attempts1，app删除ok=true；Pod中的临时探针已删除。证据见 `/tmp/sg-forward-warm-release/` 和 `/opt/cellhive-helm/reports/sg-forward-warm-2026-10-25/`。

## 尾延迟分段诊断

新增请求 spans：binding lookup/refresh、resource prepare/gate、实际 owner/fence bucket read、capture preflight/ensure、SQLite write，并给 durability proof 和 SQLite write 标注 txid。后台 capture 不继承某一次请求的取消上下文；通过 scope/epoch/txid 区间与请求关联。提交超过50ms时记录实际 mode、fallback reason、owner/selection/session/replication/bucket 等耗时及选择当时的 lease snapshot；不额外请求对象存储。观测不改变复制、租约、缓存和持久化确认规则。`bucket_ms` 包含上传队列等待和上传，不能直接等同单次 COS HTTP 耗时。
