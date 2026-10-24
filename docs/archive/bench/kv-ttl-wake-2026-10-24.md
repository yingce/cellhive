# KV TTL 写入与 wake 索引对照（2026-10-24）

## 问题与线上基线

SG 上一个带 `expiration_ttl=300` 的公开 KV put 返回 200，但 curl 测得 DNS 3.3ms、建连 3.4ms、TTFB 590.9ms。对当前线上旧镜像做同 Pod `127.0.0.1:7001`、`vwork-24`/`TEST`、1 字节值的交替请求（各 6 次，独立诊断 key）：普通 put 的 TTFB 为 9.3–164.4ms，TTL put 为 190.7–299.9ms；首次单独 TTL put 为 326.2ms。旧镜像 `vwork-24` 的持久化证明直方图 165 次中 128 次 ≤25ms、161 次 ≤100ms，均值约 23.6ms。指标包含 capture 等待，不等于单独 follower fsync。公网与同 Pod 样本不是同一请求，不能逐毫秒相减。

代码路径中，KV 数据获得 RPO=0 证明后，`handleKVPut` 同步调用 `armKVExpiry`。原实现每次 `RemoveByOccurrence`、`Upsert`，并通过 `wake.Index` 重写桶里的 pointer 和 due entry；后台扫描只负责消费这些记录。普通写也可能遇绑定/owner 缓存失效和对象存储竞争，因此本次修复只归因可重复的 TTL 增量。

## 修复与验证

ADR-282 让服务器在按 cell 串行的对账中读取 SQLite 的最早 KV 到期和唯一已持久化 `kv-expire` timer。timer 尚未到期且不晚于当前最早到期时复用；缺失、重复、已到期或过晚才重建。过期清理重新读取最新到期，避免并发 put 后按旧结果删除新 timer。RPO=0 的 follower/bucket 证明不变。

同一台机器的临时单节点 FS 进程、同一类 256 字节请求，修复前普通/TTL 各 10 次中位数为 4.21/5.36ms，修复后为 3.85/4.11ms。顺序测试只验证本地开销下降，不代表 COS 延迟幅度。新回归断言重复 TTL 不再产生索引写、提前到期会重建、并发对账保持最早到期、已到期 timer 不可复用、重复 timer 会重建。`make test`、`make vet`、`make build` 及定向 `-race` 通过。

真实进程 FS TTL E2E：1 秒和 3 秒的两个 key 分别在约 1.11 秒、3.12 秒被物理清理。修复版 8 秒五场景 FS 冷写 E2E `COLD-E2E: PASS`：稳态 1714 次 put，p50/p99 为 4.0/5.1ms；接管、重启、冷恢复后的最终 140 个已确认 key 均可读回，SQLite `integrity_check=ok`。这套五场景不带 TTL，作为普通写回归。

真实 COS 测试桶 `vwork-e2e-hk-1376795072`、本机跨地域访问、`CELLHIVE_DURABILITY=bucket`、同一测试方法各 8 对 256 字节普通/300 秒 TTL put（独立 namespace，顺序非同时 A/B）：

| 版本 | 普通 put 中位数 | TTL put 中位数 | TTL 样本范围 |
|---|---:|---:|---:|
| 旧版 | 56.0ms | 257.2ms | 195.4–280.4ms |
| 修复版 | 65.7ms | 47.0ms | 45.2–243.1ms（首次冷请求 243.1ms） |

修复版另用真实 COS 新 namespace 验证两档 TTL：写入后两 key 均返回 200；3 秒时 2 秒 key 返回 404、6 秒 key 仍 200；7 秒时两 key 均 404。三次临时 namespace 均通过产品 `DELETE /v1/control/app` 返回 200 完成清理。小样本、跨地域与 bucket 模式不能代表 SG 同区 fleet 模式的绝对延迟，也不能把两轮差值全归因于单个代码改动。

**未验证**：修复版在 SG 同区 fleet 模式及公开入口的延迟、失败注入下的 wake 索引修复。仓库代码尚未部署；当前 SG 的线上测量仍对应旧镜像。

## 扩样与 GET（2026-10-24）

在同一个香港 COS 测试桶、本机单 agent、`CELLHIVE_DURABILITY=bucket` 下，使用修复版真实进程再做两轮独立 namespace 测试。每轮先创建 App 和 KV 资源、预热，再交替提交 120 次普通 put 与 120 次 `expiration_ttl=300` put（256 字节、独立 key）；每对写入后读一个已存在且未到期的 TTL key，最后空闲 2 秒并连续读取该 key 300 次。每次请求使用本机 HTTP 连接，成功状态和 GET 响应体均已校验，两个 namespace 最后经产品 App 删除接口返回 200。分位数取排序后 `floor((n-1)*p)` 位置。

| 轮次 / 操作 | n | p50 | p90 | p95 | p99 | 最大值 |
|---|---:|---:|---:|---:|---:|---:|
| 1 普通 put | 120 | 47.27ms | 61.44ms | 74.93ms | 292.27ms | 462.62ms |
| 1 TTL put | 120 | 50.36ms | 82.22ms | 101.36ms | 495.03ms | 935.46ms |
| 1 写后 GET | 120 | 0.61ms | 21.49ms | 26.58ms | 45.12ms | 46.67ms |
| 2 普通 put | 120 | 45.56ms | 73.41ms | 123.25ms | 236.86ms | 528.15ms |
| 2 TTL put | 120 | 47.47ms | 87.73ms | 104.92ms | 160.93ms | 867.36ms |
| 2 写后 GET | 120 | 0.62ms | 22.21ms | 26.81ms | 58.11ms | 102.76ms |
| 2 空闲连续 GET | 300 | 0.33ms | 0.38ms | 0.40ms | 0.55ms | 57.93ms |

两轮首次 TTL put 分别为 1066.07ms、1477.68ms，首次 GET 分别为 32.47ms、25.33ms，未计入稳态表格。稳态 TTL put 的 p50 比普通 put 高 1.9–3.1ms；约 45–50ms 是当前跨地域 bucket 模式下普通写也存在的共同基线，不是每次都重建 TTL timer 的额外耗时。尾延迟和冷请求仍然显著；本次没有证明它们由哪一个组件造成。空闲热 GET 的 p50/p99 为 0.33/0.55ms，写后 GET 的 p90 约 22ms；两种负载条件不能混成一个 GET 延迟指标。上述结果均非 SG 同区 fleet 或公开入口测量。

对 GET 尖峰追加间隔测试：另一独立测试进程中，连续 300 次热 GET 的 p50/p99 为 0.39/1.56ms；随后每隔 1.15 秒发起一次 GET，8 次分别为 43.75、62.54、71.82、73.97、60.34、73.44、109.20、185.02ms。代码中 binding 缓存默认 1 秒、owner 缓存 1 秒、缺失资源 fence 缓存 1 秒；缓存失效后分别可能读取控制 owner、cell owner 或 COS fence。单次 GET 前后桶 `get` 计数差为 1–7 次，但后台任务也使用同一计数器，不能把差值全部归因于 GET。这组优化前结果支持带失效边界的提前刷新；不能直接延长缓存 TTL，因为它也限定资源删除和 owner 切换的可见时间。

## ADR-283 热读提前刷新复测（2026-10-24）

在同一测试桶及独立 namespace 用修复版重新运行相同的 120 对普通/TTL put + 写后 GET、300 次空闲 GET、8 次间隔 1.15 秒 GET。后台只为最多 64 个近期重复读取的 KV scope 提前刷新原有 binding、owner、fence 缓存；不延长缓存 TTL。每轮所有请求均成功且 GET 内容正确，产品 App 删除返回 200。

| 修复版轮次 / GET | n | p50 | p90 | p95 | p99 | 最大值 |
|---|---:|---:|---:|---:|---:|---:|
| 1 写后 GET | 120 | 0.59ms | 0.77ms | 0.91ms | 20.39ms | 24.06ms |
| 1 空闲连续 GET | 300 | 0.33ms | 0.38ms | 0.39ms | 0.48ms | 0.55ms |
| 2 写后 GET | 120 | 0.60ms | 0.69ms | 0.78ms | 1.30ms | 41.46ms |
| 2 空闲连续 GET | 300 | 0.34ms | 0.39ms | 0.41ms | 37.47ms | 54.34ms |

一个短负载（各 4 次普通/TTL put）后，8 次间隔 GET 均为 0.52–0.69ms，每次请求前后桶 `get` 计数差为 0。完整负载第一轮 8 次间隔 GET 中 5 次低于 0.7ms、3 次为 23.92/43.08/67.38ms；第二轮 6 次低于 0.7ms、2 次为 40.43/290.71ms。第二轮后台刷新 43 次、失败 1 次；桶计数包含大量后台工作，不能把每次尖峰精确归因。两轮首次 GET 分别为 23.42ms 与 141.94ms，首次 TTL put 分别为 942.12ms 与 1021.06ms。这个实现显著降低了混合负载的 GET p90/p95，但仍没有消除所有跨地域 COS 尾延迟、冷启动延迟，也尚未经过 SG 同区 fleet/公开入口复验。

## 本机 owner 判活修复与最终版复测（2026-10-24）

上述尖峰的临时分段日志显示，部分 14–576ms 慢 GET 全部花在 owner 路由判活，SQLite/open 均约 0ms；再细分确认其中的同步 `nodes/<self>.json` 点读耗 24–25ms。原因是本机 owner 也走可能滞后的 node-lease 快照，快照负向时按 ADR-280 做 COS 点读。修复后，本机 owner 且 session 相同的请求以**已成功发布**的本机租约和 self-fence 健康窗口判断；从未发布或已不健康时返回 503，不能在旧 owner 记录上继续读。远端 owner 仍按快照负向点读确认，不改变接管协议。诊断埋点已从最终代码移除。

最终代码另开独立 namespace，在相同本机单 agent、跨地域香港 COS 测试桶、bucket durability、localhost HTTP 的条件下，交替运行各 60 次普通/300 秒 TTL put，每对后读取同一个有效 TTL key；再连续读 300 次，并每隔 1.15 秒读 8 次。全部状态与 GET 内容正确，产品 App 删除返回 HTTP 200。分位数按排序后 `floor((n-1)*p)` 取值：

| 操作 | n | p50 | p90 | p95 | p99 | 最大值 |
|---|---:|---:|---:|---:|---:|---:|
| 普通 put | 60 | 52.17ms | 75.61ms | 82.02ms | 91.14ms | 93.94ms |
| TTL put | 60 | 51.79ms | 74.96ms | 97.55ms | 388.17ms | 603.76ms |
| 写后 GET | 60 | 0.60ms | 0.70ms | 0.76ms | 1.03ms | 29.99ms |
| 空闲连续 GET | 300 | 0.33ms | 0.38ms | 0.40ms | 0.45ms | 0.64ms |

8 次间隔 GET 为 0.58/0.62/0.55/0.55/0.57/0.59/0.62/0.62ms；后台热读刷新 31 次、失败 0 次。首次 GET 30.94ms，首次 TTL put 1080.20ms，单列为冷路径。刷新和其他后台工作共享 bucket 计数，不能把总 `get/list` 差分归因到单次 GET。该轮仍有单个写后 GET 29.99ms；此前加入本机 lease 判活后另一轮 60 对负载的间隔 GET 仍出现 20/38/61ms，分段日志定位到 binding/fence/owner 缓存过期的同步点读。因此本修复显著改善热读常态与大部分长尾，**不能宣称消除了所有尾延迟**。SG 同区 fleet 和公开入口尚未部署验收。

最终版 `make test`、`make vet`、`make build`、owner/lease/server/replica 定向 `-race` 均通过。测试夹具现先发布 node lease，另有回归测试证明未发布的本机 lease 不允许沿缓存 owner 服务请求。

## 真实 HTTP 热 PUT 对照（2026-10-24）

用户追问 PUT 是否与 GET 一样慢，追加本机单 agent 到香港 COS 测试桶的真实 HTTP 对照，仍用 `CELLHIVE_DURABILITY=bucket`。每个独立 namespace 先预热，交替各 20 次普通/300 秒 TTL PUT，每对读一次有效 key，再每隔 1.15 秒做 8 次 GET 和 12 次**普通 PUT**。所有请求成功，所有 namespace 均通过产品 App 删除接口返回 200 清理。不同轮次的 COS 负载不可严格配对，以下只比较量级和请求路径。

| 版本与轮次 | 连续普通 PUT p50 / 最大值 | 12 次间隔普通 PUT | 写后 GET p50 |
|---|---:|---:|---:|
| PUT 未激活提前刷新，定向分段轮 | 49.95 / 88.83ms | 前两次 54/67ms，后十次 110–136ms | 0.57ms |
| 成功 PUT 激活提前刷新，复测 1 | 49.75 / 77.02ms | 43–66ms | 0.61ms |
| 成功 PUT 激活提前刷新，复测 2 | 52.06 / 110.14ms | 11 次 49–66ms，1 次 611.39ms | 0.56ms |
| 成功 PUT 激活提前刷新，定向分段轮 | 49.40 / 68.53ms | 47–56ms | 0.58ms |

定向分段轮在未激活 PUT 的代码上取到后十次间隔 PUT：binding 约 23–31ms、资源 fence 约 12–16ms、gate 约 22–31ms，handler 约 45–67ms，合计约 110–136ms；这解释了 PUT 与 GET 共享的周期性控制面等待。修复只让**成功 PUT**也登记活跃 scope，使用 ADR-283 已有的有界后台刷新，失败仍不续期，写入证明不变。修复版三轮后台刷新分别 45/48/47 次，均无失败；这只说明对应测试期间刷新成功，不能保证所有请求命中缓存。

PUT 另有 GET 没有的固定成本：按 cell 捕获、SQLite 写入和 RPO=0 持久化证明。修复版临时诊断埋点显示多个 80–181ms 的连续 PUT 在 binding/fence/gate 与 owner 路由上约 0ms，额外时间全部处于 `captureWaitTxID` 的证明等待；首次 TTL PUT 仍约 0.8–1.4 秒，属于冷路径。复测 2 的 611.39ms 单次间隔 PUT 没有同请求的阶段日志，不能断言其具体阶段；跨地域 bucket 模式仍有显著尾延迟。本机单 agent 结果不能外推 SG 同区 fleet 或公开入口。临时诊断使用 Go build overlay，仓库代码未保留阶段日志。

## COS Append 免 HEAD 的 PUT 对照（ADR-284，2026-10-24）

代码审查发现，COS 上的 LTX `ConditionalCreate` 是原生 Append，再做 S3 HEAD 读取 ETag。临时 Go build overlay 实测 COS 测试桶：28 次成功 Append 的响应都有 ETag；另一个独立 namespace 对 15 次 Append 同时做 HEAD，对比 15/15 相同。正式代码仅在响应 ETag 非空时省略 HEAD；缺失时回退原流程，原生 Append 成功仍是 RPO=0 持久化证明。独立 namespace 上通过真实 HTTP PUT/GET 和产品 App 删除，清理均返回 200。

与变更前二进制做顺序 A/B：本机单 agent、香港 COS 测试桶、bucket durability、localhost HTTP，每轮交替各 40 次普通/300 秒 TTL PUT，每对写后 GET，再做 12 次间隔 1.15 秒普通 PUT。所有请求状态和 GET 响应正确；分位数按排序后 `floor((n-1)*p)` 取值。

| 版本 | 普通 PUT p50 / p95 / 最大值 | TTL PUT p50 / p95 / 最大值 | 写后 GET p99 |
|---|---:|---:|---:|
| 原路径 Append + HEAD | 49.95 / 67.57 / 295.07ms | 48.78 / 64.71 / 79.98ms | 0.69ms |
| COS 响应 ETag，省略 HEAD | 44.19 / 75.03 / 544.06ms | 46.57 / 103.66 / 392.69ms | 0.67ms |

这轮普通/TTL PUT 的 p50 分别少 5.76/2.21ms，符合省掉一次网络往返，但不同轮次 COS 负载波动明显，**不能把差值全归因于 HEAD**；优化版仍有 544ms 普通 PUT 尖峰，尾延迟没有解决。两轮产品 App 删除均返回 200。下一步需要在 SG 同区 fleet 和公开入口测量，并单独观察慢写是 COS Append、资源 fence 点读还是网络重试。

进一步用临时 Go build overlay 对正式优化代码跑 120 对普通/TTL PUT、写后 GET 和 12 次间隔 PUT（同样的真实 HTTP/香港 COS/bucket 模式）。普通 PUT p50/p99/最大值为 44.10/133.07/335.44ms，TTL PUT 为 45.53/475.18/825.50ms；写后 GET p99 0.74ms，12 次间隔 PUT 中有 451/493/561ms 尖峰。全部请求和内容正确，产品 App 删除返回 200。分段日志把一个 824ms 的普通 PUT 分成资源 fence **权威点读 778ms**、不可变 LTX 写入 42ms；另有一次 474ms PUT 中权威点读 57ms、不可变 LTX 写入 412ms。其他尖峰分别观察到权威点读 618/526/424ms 或 Append 472/315/231ms；`captureWaitTxID` 与这些阶段总耗时相符。刷新失败数为零并不能规避提交前的 fresh fence 检查。这些数据明确表明剩余长尾在跨地域 COS 权威读/写；单节点 bucket 模式不能通过延长请求缓存、跳过 fence 或提前确认来安全消除。需要 SG 同区 fleet follower-fsync 与 bucket 回退路径分别验收。临时计时代码只在 `/tmp` build overlay，不进入仓库。

## 本机双节点 follower 对照（2026-10-24）

用当前工作树构建的 cell-agent，在同一宿主机启动两个独立数据目录/节点，共用香港 COS 测试桶，设置 `CELLHIVE_DURABILITY=fleet`；真实 localhost HTTP 请求发到 A，B 经 loopback 接收并 fsync follower spool。独立 namespace 注册 App/KV，预热至 `cellhive_commit_total{mode="fleet"}>0`，逐次校验 PUT 200 和 GET 内容。分位数按排序后 `floor((n-1)*p)` 取值。

| 样本 | 普通 PUT p50 / p95 / 最大值 | TTL PUT p50 / p95 / 最大值 | 写后 GET p50 / 最大值 | 确认模式 |
|---|---:|---:|---:|---|
| 连续 60 对 | 6.60 / 9.64 / 13.66ms | 7.10 / 9.10 / 9.78ms | 0.57 / 0.80ms | 窗口内 fleet +121、bucket-batch +0 |
| 连续 40 对 | 6.26 / 7.69 / 8.21ms | 7.07 / 9.28 / 11.20ms | 0.53 / 1.67ms | 后续间隔/转发阶段发生回退，不能给整轮标记纯 fleet |
| 连续 20 对 | 6.43 / 7.25 / 8.41ms | 6.69 / 8.49 / 8.80ms | 0.52 / 0.70ms | 连续阶段 fleet +40、bucket-batch +0 |

第三轮随后每隔 1.15 秒做 12 次普通 PUT：p50 7.89ms、最大 56.22ms；从 B 访问同一 KV 再做 12 次转发 PUT/GET：PUT p50 6.10ms、最大 61.55ms，GET p50 0.74ms、最大 0.94ms。间隔阶段 fleet +15、bucket-batch +3；这些提交计数包含后台控制面提交，不能逐一归到某个 KV 请求。第二轮间隔 PUT 有一次 319.94ms，转发 PUT p50 40.64ms，结束时 `cellhive_live_followers=0`、整段 fleet +105/bucket-batch +9；第三轮结束时 follower=1，但日志在若干时刻报告无可用 peer。这表明双节点能把稳定 fleet 写入降至约 6–9ms，但 follower 可用性或租约采样波动仍会触发 bucket 回退与长尾，不能宣称长尾已解决。三轮均通过产品 App 删除返回 200 清理。

这只是**同机 loopback follower + 跨地域 COS 权威**的对照，不代表 SG 两台机器间网络、同区 COS 或公开入口的延迟。SG 仍需测真实双 Pod 的提交模式、follower 健康及请求延迟。

### 同一 KV 的 owner 与 follower 入口配对测试

再用两个独立 namespace 各跑一轮真实 HTTP：A 先持有同一个 KV scope；客户端按 AB/BA 交替顺序分别向 A（owner）和 B（follower 入口）写不同 key，再从两端读取相同的预热 key。每个请求新建 localhost HTTP 连接，请求体 256 字节，B 经内部 HTTP 转发给 A；PUT 200 和 GET 内容逐次校验。预热两端路由后才采样，测试窗口的 A 提交指标均为 fleet，bucket-batch 增量为零；B 没有与请求数量相称的本地提交（两轮增量分别 0、2），与转发路径一致。`cellhive_live_followers` 两端采样时均为 1。

| 配对数 | owner PUT p50 / p95 / p99 | follower 入口 PUT p50 / p95 / p99 | owner GET p50 / p95 / p99 | follower 入口 GET p50 / p95 / p99 |
|---:|---:|---:|---:|---:|
| 100 | 6.78 / 8.79 / 10.56ms | 6.44 / 9.03 / 9.76ms | 0.50 / 0.67 / 0.74ms | 0.78 / 1.11 / 1.37ms |
| 200 | 6.64 / 8.86 / 11.23ms | 6.58 / 9.57 / 28.00ms | 0.50 / 0.73 / 1.00ms | 0.79 / 1.10 / 11.76ms |

同一轮相邻请求的 `follower - owner` 配对差值：PUT 中位数分别为 -0.28ms 和 -0.04ms，均处于实验噪声量级；GET 中位数分别为 +0.26ms 和 +0.28ms。第二轮 follower PUT/GET p99 尾部明显高于 owner（分别 28.00/11.76ms），不能把尾部全部解释为一次 loopback 网络往返，需进一步区分 follower 路由/缓存失效等待与网络开销。本机稳态 PUT 的主要耗时仍是 owner 上的 SQLite 捕获和 follower fsync；以上差值不能外推跨机器网络。两轮均通过产品 App 删除 HTTP 200 清理。

## SG revision 53 部署复验（2026-10-24）

代码提交 `ac5ad43`、SG Helm 操作仓提交 `c3f66ab`，镜像 digest `sha256:255b951d1814e13dc84c910df6041ed4e34137a33c34e67892948f8f614ee381`。SG Helm revision 53 `deployed`；两个 cell-agent、两个 gated DO runtime、两个 user-runtime 的 Pod 均 Ready、实际 `imageID` 为该 digest、restartCount 为 0。

先通过 `kubectl port-forward` 对独立测试 namespace 做 20 对真实 HTTP PUT/GET（普通和 300 秒 TTL 各 10 次），逐次检查状态和内容；全部通过，产品 App 删除 HTTP 200。该轮 owner 指标 `fleet` +17、`bucket-batch` +4；包含控制面提交，不能逐次归因到 KV。隧道测得约 130ms 热 PUT，**含 kubectl 隧道开销，不作为 SG 服务延迟**。

然后直接 `kubectl exec` 到 `cellhive-sg-cell-agent-0`，用 Pod 内 `curl 127.0.0.1:7001` 和另一独立 namespace 做同样 20 对真实 PUT/GET（普通/TTL 各 10 次）；全部 HTTP 200、GET 内容正确。前两次写分别 752/146ms，后 18 次热 PUT p50 9.84ms、p95 11.44ms、最大 13.14ms。窗口内 `cellhive_commit_total{mode="fleet"}` +22、`bucket-batch` +0，`cellhive_live_followers=1`；产品 App 删除 HTTP 200。此结果只覆盖单 Pod、短时间的小值写；公开入口、跨 Pod 入口和长期尾延迟仍需测。

启动后 `cell-agent-1` 有一次恢复旧节点 session 时向旧 follower IP `10.0.16.195:7001` 发 seal 请求超时的 WARN。代码会在不完整 recovery 时拒绝封存，未观察到新写失败；旧 session 能否最终封存尚未确认，需另行追踪。
