# user-runtime 静态托管原型（无 Go router / 无进程管理）与压测（2026-10-20）

目的：验证「目标 = 裸 workerd `/ping` 13,994 RPS 的 70%，即单实例 ~9.8k」能否在**不做副本**的前提下达到。结论：**动态 workerLoader 架构不可达（3.3–3.8k）；把租户版本在部署时渲染成静态 workerd service（无 loader、无每请求 JSRPC）可以达到目标线附近**——Tail 关 **13.0–13.3k（−5%~−7%）**，Tail 开 **8.5–9.7k（−29%~−39%）**，Tail 开是否达标取决于是否保留默认日志采集。

按本轮要求，原型**去掉了 Go host router 和 workerd 进程管理**：只验证 workerd 静态托管本身的请求路径与吞吐；Host 路由交给现有边缘（Traefik），进程启动/编排复用现有 supervisor，均不在本原型范围内。

## 原型结构

固定 stock workerd `1.20260916.1`，一个 workerd 进程静态承载：

| service | 作用 |
|---|---|
| `plat` | 平台能力宿主：加载 `bindings.js` 等平台 JS，导出 `KV` 等 entrypoint，`CELL_URL` 指向本机 cell-agent |
| `v1` / `v2` | 两个版本的租户 worker：`main.js`（直接 `new CellHiveHost(ctx,env).fetch`）+ `wrapper.js`（`queue-wrapper.js` + platformConsts）+ `tenant-v1/v2.js` + facades/quota/invocation |
| `tail` | 静态 Tail service：`tail(events)` 累计计数，另有 socket 供查询 |
| `public-network` / `private-outbound` | 出网 network service |

关键点：
- 租户 binding 用 capnp `bindings = [(name="A", service=(name="plat", entrypoint="KV", props=(json="{\"kind\":\"kv\",\"ns\":\"loadbench\",\"name\":\"A\",\"token\":\"<scopedToken>\"}")))]`。`scopedToken` 与 loader.js 的 HMAC 格式一致（secret 为 HKDF(root,"cellhive/token/scope") 的 base64url **字符串字节**），在渲染时生成。
- Tail 用 `tails = [ (name="tail", props=(json="{...version...}")) ]` 静态挂载。
- 两个版本各一个 socket（19101/19102），Tail 用 19103；**无 host router、无进程编排**。压测直连 workerd socket（与 13,994 的 bare 基线同口径）。
- 版本切换验证：v1 的 in-flight SSE 保持返回的同时，新请求直接发往 v2 socket（生产对应边缘路由切换）。

## 压测方法

`wrk -t4 -c64 -d15s --latency` 直连 workerd socket，每轮前 3s 预热。每轮采样 workerd 进程 CPU（`/proc/<pid>/stat`）计算 cores 与 CPU/请求（cores=1.0 的轮次才用于折算，宿主被外部任务占用时 cores<1 的轮次视为受干扰）。宿主为共享环境，本轮外部 node 构建任务使 load 在 4.5–10.8 之间波动，因此**用 CPU/请求折算干净单核吞吐**，绝对值仅作参考。

## 结果（直连，无 router / 无进程管理）

| 配置 | 有效轮次 RPS | CPU/请求 | 折算单核 RPS | 相对 bare 13,994 |
|---|---:|---:|---:|---:|
| 静态 · Tail 开（真实 ring handler）· 直连 v1 | 7,918 / 8,488 | 125–134µs | ~7.5–8.0k | −39%~−43% |
| 静态 · Tail 关 · 直连 v1 | **13,017 / 13,297** | 77–79µs | **~12.8–13.0k** | **−5%~−7%** |
| 静态 · Tail 关 · 直连 v2（冷启动轮） | 9,060 | — | — | 仅参考 |
| 当前动态 loader 完整路径（同机历史） | 2,714 / 3,456 | 290–360µs | — | −75%~−81% |
| 裸 workerd 静态 `/ping`（干净宿主） | 13,994 | 71µs | — | — |

（另有 Tail 关闭的纯 wrapper 原型在更干净宿主 load≈1.7 时实测 **12,203 RPS**，与上表 77–79µs 的折算一致。）

## Tail 成本拆解（none / 空 handler / 真实 ring）

为回答“Tail 不是本地 ring 吗，为什么还这么贵”，用三个只差 `tails` 配置的变体直连压测（15s、c=64，load 3–4，交替两轮）：

| 变体 | RPS | CPU/请求 | 说明 |
|---|---:|---:|---|
| 不挂 Tail | 12,761 / 11,347 | 78–87µs | 基线 |
| 挂 Tail，handler 只计数（空实现） | 9,068 / 9,648 | 106–111µs | 只剩 workerd 原生 Tail 事件生成 + 跨 isolate 投递 |
| 挂 Tail，真实 `TenantTail`（本地 ring） | 8,488 / 7,918 | 125–134µs | 再加上 ring 入队/记账 |

结论：

- **本地 ring 只消除了“逐调用 HTTP 投递 + 查询时才转换”**（ADR-236 的贡献）；它没有消除 workerd 每个 invocation 的原生 Tail 事件生成与投递，也没有消除 handler 内的 ring 记账。
- 原生 Tail 事件投递约 **+30µs/请求**（即使没有任何 console 输出，workerd 仍为每个请求产生 Tail 事件）；我们的 `storeRecords` 记账（`quietSize` 遍历请求头、分配 record、入 ring、驱逐与字节统计）再 **+15–25µs/请求**。
- 这两项都在 workerd 的同一个事件循环线程上，直接和请求处理争 CPU；同时 p99 从 0.39–0.57s 升到 1.4–1.6s，wrk timeout 从 0 变为 ~100。
- 相对影响变大是因为请求路径已被我们压到 ~80µs：固定 ~45–55µs 的 Tail 成本就从“噪声”变成 **60%+ 的额外 CPU**。

## 正确性验证

- `/version`：v1 返回 `v1`，v2 返回 `v2`。
- `/kv`：租户 `await env.A.get('hot')` 经静态 `plat` service 的 `KV` entrypoint → cell-agent `/v1/kv/get`，返回 `{"v":"value-a"}`（真实 scope token 校验通过，验证于本原型首轮）。
- `/stream`：v1 的 SSE 在新请求切到 v2 后仍完整收到 10 个 chunk（首块 0.21s，末块 1.82s）。
- 静态 Tail：`/ping` 期间 tail 计数器增长到 323,484，证明事件送达静态 Tail service。

## 结论

1. **Tail 关时静态托管达到目标**：13.0–13.3k RPS，仅比 bare 低 5%–7%，远好于“允许下降 30%”的线；单请求 CPU 比 bare 高约 6–8µs（wrapper + env/binding 结构）。
2. **Tail 开是唯一缺口**：原生 Tail 事件投递约 +30µs，本地 ring 记账再 +15–25µs，合计 +45–55µs/请求；静态 Tail 开折算 ~7.5–8.0k，比目标线 9.8k 低约 15%–18%。要达标需要：a) 把 Tail 变成按 worker / 按订阅的可选项（静态渲染时决定是否挂 `tails`，收益最大，直接回到 ~13k）；b) 降低 ring 记账成本（例如去掉逐事件 header 长度估算）；c) 接受 Tail 开时约 −40%。
3. **去掉 Go router / 进程管理不影响该结论**：以上都是 workerd 直连口径，与 bare 13,994 同口径。生产路由仍在边缘（Traefik 按 Host → per-worker socket），进程启动/编排复用现有 supervisor，不在本轮实现；若计入边缘代理成本，应使用端到端口径并同时计入旧架构的同类成本。
4. 未覆盖（生产化前必须验收）：D1/R2/Queue/DO/Workflow 的静态 props、queue/scheduled handler、WebSocket、进程/版本生命周期（drain、回滚、内存上限）、冷启动与容量调度、边缘路由下发。

## 局限

本轮宿主被外部 node 构建任务占用（load 4.5–10.8，8 逻辑核，workerd 单线程），所有绝对 RPS 仅作同会话参考；表中采用 cores≈1 的轮次并用 CPU/请求折算单核吞吐，不能作为容量承诺。
