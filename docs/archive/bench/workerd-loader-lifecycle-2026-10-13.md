# stock workerd 动态 Worker 生命周期调研（2026-10-13）

## 结论

可以不修改 workerd 实现动态 Worker 回收。当前 named `LOADER.get(id, getter)` 的内部强引用缓存是版本累计占用的重要原因。删除 CellHive JS 缓存不能删除这个内部条目。已验证匿名加载和动态 Worker 内部 `abortIsolate()` 均能避免本次最小实验中的实例累计 OOM；生产需结合缓存预算、活动引用跟踪和回收策略，不能直接每请求 abort。

## 固定源码证据

版本为 `1.20260916.1`，仓库记录上游 revision `adda2635656d09e541b0feeea796da9d2a8bc10e`。以下链接均固定 revision，不以 Cloudflare 托管运行时文档推断 standalone 行为。

- [`api/worker-loader.h`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/api/worker-loader.h)：WorkerLoader JS 只注册 `get`、`load`，WorkerStub 只注册 `getEntrypoint`、`getDurableObjectClass`；没有 loader.delete/unload 方法。模块类型是源码/字节/JSON/WASM，没有文件路径模块类型。
- [`server/server.c++:5141-5196`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/server/server.c++#L5141-L5196)：named load 经 `isolates.findOrCreate` 存进 `HashMap<String,Rc<WorkerStubImpl>>`；匿名 load 不进入该 map，仅 startup task 临时额外持有引用。该实现没有 named-map LRU/TTL/总字节预算。`removeIsolate()` 由 abort 路径 erase。
- [`server/server.c++:5247-5269`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/server/server.c++#L5247-L5269)：WorkerStubImpl 析构时将 WorkerService unlink/destruction 延后到事件循环下一轮，避免在其他 isolate 的 GC 内同步销毁。
- [`server/server.c++:5378-5386`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/server/server.c++#L5378-L5386)：动态服务还持有源代码 ownContent，并安装 isolate abort 回调。说明源码 Map 去重不保证 native 动态服务源码也只保留一次。
- [`server/server.c++:4491-4507`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/server/server.c++#L4491-L4507)：动态 Worker 的 abort 回调删除内部 map 条目；非动态 Worker 的 abort 会终止整个进程！实现还留有 outstanding calls abort 的 TODO，不能把它当作完善的 drain/unload 接口。
- [`api/workers-module.c++:71-77`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/api/workers-module.c++#L71-L77) 与 [`cloudflare/workers.ts:211-213`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/cloudflare/workers.ts#L211-L213)：`import { abortIsolate } from "cloudflare:workers"` 操作**当前 isolate**，并终止当前 JS 执行。宿主不能直接对任意 loader id 调它。
- [`api/tests/worker-loader-test.js:1045-1103`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/api/tests/worker-loader-test.js#L1045-L1103)：上游测试通过 module counter/getter count 证明 abort 后同名重新加载。
- [`api/tests/worker-loader-gc-test.js`](https://github.com/cloudflare/workerd/blob/adda2635656d09e541b0feeea796da9d2a8bc10e/src/workerd/api/tests/worker-loader-gc-test.js)：匿名 Worker 在引用释放/GC 后销毁的回归测试。

### DO 边界

`server/server.c++:733-792` 的 `abortFacet` 中止并移除 facet container；`deleteFacet` 还会删除底层数据，不能用于缓存回收。facet 的 ActorClassImpl（5467-5533）持有动态 WorkerStubImpl 引用；abort facet 不会替代 named loader map 的移除。DO 空闲 actor shutdown（1042-1087）和 namespace container cleanup 也是独立生命周期，不能直接等同于 code isolate 卸载。

## pinned 二进制实验

复现入口：`workerd/spikes/memory-lifecycle/{probe.capnp,host.js,run.py}`。最小宿主，不使用生产 bundle/prepared/assets Map，以隔离 workerd 内部生命周期；源码为最小 fetch handler + 1 MiB 注释。Docker 512 MiB，无 swap，2 CPU；host 与 tenant 均没有 experimental compatibility flag，仅 workerd 进程启用 `--experimental` 以使用 workerLoader。

```bash
python3 workerd/spikes/memory-lifecycle/run.py \
  --image cellhive:runtime-baseline \
  --output /tmp/opencode/cellhive-lifecycle-evidence
```

镜像 ID：`sha256:9e138615b853310dd148bc4a16953b349192b675154b4810c7705372ed2dc565`；二进制输出 `workerd 2026-09-16`。临时版本实验两轮 + checked-in runner 一轮均显示相同模式，最终 runner 结果：

| 模式 | 相同 ID 两次请求 | 1,000 次新增加载 | 结束 RSS | idle 15s RSS | OOM |
|---|---|---|---|---|---|
| named get(id) | counter 1→2，getter 1 次 | 未完成，100 次时 RSS 387.7 MiB，随后进程死亡 | — | — | true，exit 137 |
| anonymous get(null) | counter 1→1，getter 2 次 | 全部完成 | 63.3 MiB | 62.9 MiB | false |
| named + tenant 自 abort | counter 1→1，getter 2 次 | 全部完成 | 66.7 MiB | 66.3 MiB | false |

abort 被调用的控制请求预期失败，由宿主捕获；业务 ping 响应先完整读取。没有跨请求缓存 capability，没有强制 GC。最终原始 JSON/log/version 位于 `/tmp/opencode/cellhive-lifecycle-evidence/`。每个测试容器结束后清理。

实验说明卸载机制在实际二进制有效；不说明 RSS 已绝对恒定（仍有少量增长），也未测并发请求、waitUntil、流式响应、RPC 返回对象、Tail、DO、长连接或性能代价。

## 可行路线

### 推荐普通 function：named 热实例 + 受控退休

保留版本化 id；发布后新请求只进入当前版本。旧版本、冷 function 达到 idle/容量阈值后，宿主通过可信 wrapper 的管理 entrypoint 让目标动态 isolate 自调用 abortIsolate，继而清除宿主 plan/source 引用。旧版本仍可根据不可变 artifact 重新加载（回滚、pinned service binding）。不能直接去掉 id 的版本号，否则旧 env/代码可能被复用。

管理能力必须由可信 host 控制；不能给公网 tenant fetch 增加未经隔离的清理 URL。无 binding direct-fetch 路径需要扩展模块/管理 entrypoint，意味着加载预算和性能要重新测试。静态 loader/host 自身不得调用 abortIsolate，否则会使整个 workerd 退出。

必须跟踪响应 body 完成、waitUntil、RPC/WS、Tail 与 DO 引用，先标记 retiring/阻止新派发，再回收；单纯 fetch Promise resolve 不等于所有活动结束。避免“清理请求把已淘汰的 id 又冷加载”或“晚到的清理删掉同名新实例”的竞态，管理入口需 generation 验证。

### 匿名加载：可用于冷/一次性执行的策略候选

`get(null)` / `load(code)` 无内部 named 缓存引用，但每次调用可能重新编译，并且 counter/global 状态不复用。本次内存低不代表性能合格。跨请求缓存 WorkerStub 仍受当前项目 request-context 约束，不能用简单 JS stub LRU 代替 named cache。DO 可通过 facet/class 生命周期持有匿名动态代码，但需独立恢复、驱逐、版本切换和持久性测试。

### 两条路线都需要

- 平台 JS 缓存仍须总字节预算和单项上限；最小 spike 没有它们，不能把 spike 的 63 MiB 直接当作生产修复后内存。
- 限制同时加载/执行的数量及峰值内存；即使旧版本可回收，大量真正并发活跃 Worker 仍能 OOM。
- 进程高水位准入、drain/recycle 作为兜底，不再需要把“无法卸载”作为默认前提。

## 实现前下一组验证

1. 同 function 1,000+ 版本切换，旧版本退休，验证当前 env、生效版本和 RSS 趋稳。
2. 旧版本慢请求/流式 body/waitUntil/RPC/WS 存活时不能提前卸载。
3. 同 SHA vars/bindings-only 发布、回滚与 pinned service 的重新加载正确。
4. 当前版本独立 function LRU 淘汰 + 再激活，内存受总预算约束。
5. DO facet abort 与代码 isolate 释放协调，不删除 storage，gated durability 与接管保持正确。
6. Native Tail、direct fetch 和匿名冷加载的吞吐/延迟对照。
