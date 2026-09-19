# workerd wrapper（平台侧）

平台注入到租户 worker/DO 的包装代码。租户业务代码保持原样，由 wrapper 处理
批处理、输出门等运行时关注点。

## `groupcommit.js` — DO 组提交 + 输出门

**问题**：workerd 只对**一个 DO 事件（请求）内的写**做自动原子合并（一次
commit/fsync）。所以"一次请求一次写"的负载被单 workerd 进程的 **~900 提交/s**
（fsync 上限）封顶；`sql.exec("BEGIN")` 又被 stock workerd 拒绝，应用层无法自己
开事务攒批。

**做法**：`GroupCommit` 把并发的 `submit(op)` 缓冲起来，在**一个事件内**应用整批
（→ 一次提交），并**只对整批取一次输出门证明**，然后统一 resolve。租户只声明
`apply(op)`（如何执行一个 op），不需要关心批处理或门。

```js
import { GroupCommit } from "wrapper/groupcommit";

export class MyDO {
  constructor(state, env) {
    this.gc = new GroupCommit({
      state, env,
      gate: env.GATE,        // 可选：带 /sync 的 service binding（输出门）
      windowMs: 3,           // 攒批窗口
      maxBatch: 512,         // 单批上限
      apply: (op) => {       // 一个 op 的写入（租户业务）
        state.storage.sql.exec("INSERT INTO t ... ");
      },
    });
  }
  async fetch(req) {
    const op = parse(req);
    await this.gc.submit(op);   // 返回时：已提交 + 已被 fleet 证明（RPO=0）
    return new Response("ok");
  }
}
```

**语义**：`submit()` 在"该 op 所在批已应用**且**（设了 gate 时）已被 cell-agent
fleet 证明"之后才 resolve。同一批共享一次提交 → 全有或全无，与 workerd 对单事件
的保证一致。

**实测**（workerd 2026-06-15，双 cell-agent，`cmd/gatebench`，1 op/请求，gated，
`windowMs=3`）：

| c | req/s | p50 | p99 |
|---|---|---|---|
| 1 | 160 | 6.18ms | 7.89ms |
| 8 | 936 | 8.44ms | 15.71ms |
| 32 | 2,501 | 12.07ms | 41.07ms |
| 64 | 3,196 | 18.69ms | 83.12ms |
| 128 | **3,637** | 32.30ms | 106.12ms |

- 同一"1 写/请求"负载不用 wrapper 时是 ~530 req/s → **~6.9×**；`/stats` 显示
  `avg_batch≈23`、`maxObserved=128`、`commits==batches`。
- **数据准确性**：200 次顺序 gated op 后，从 follower 复制链（2500 段）恢复
  actor DB → `t.n` 精确一致、`integrity_check=ok`。**组提交不破坏 RPO=0 门语义**。

**调优/边界**：
- `windowMs` 是延迟/吞吐权衡：c=1 时单个请求也要等 3ms（160 < 不攒批的 531）→
  生产应做**自适应窗口**（无竞争时 0，拥挤时增大）。
- 每批一次门 RTT（~2ms）在关键路径；可与 ADR-047/048 的持久流/流水线结合。
- 上限仍是**单个 workerd 进程**；横向靠多进程（单进程多 actor 无效，已实测）。
- 与"网关 batch 端点"方案相比：wrapper 无需改调用协议，但要求写入经由 `submit`。

## 参考

- ADR-051（workerd DO → WAL → LTX → cell-agent 输出门）
- `workerd/spikes/p0/gate/`（gate spike：`worker.js` / `batch.js` / `gc.js`）
