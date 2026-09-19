# P0 任务拆解

目标：验证两个根本假设 + 跑通最小闭环。任务按依赖顺序；每项含验收。状态：☐ 未开始 · ◐ 进行中 · ☑ 完成。

## P0.1 SQLite cell 存储 ☑

**交付**：`internal/cellstore` —— 每 cell 一个 SQLite（`<DataDir>/cells/<scope>.db`），提供：
- `Open(scope)` 打开/创建；`OpenAt(path)` 打开既有库（恢复用）；
- KV 表（`kv(key,value,meta)`）+ `cell_meta`（txid）；
- `Snapshot(dest)` 通过 `VACUUM INTO` 产出一致快照。

**验收**：☑ 单测覆盖 put/get/delete/scan/txid/snapshot；快照可独立打开且数据一致（`go test ./internal/cellstore` 通过）。

## P0.2 WAL 捕获 spike ☑

**交付**：`internal/wal` —— 只读解析 SQLite `-wal`：
- 解析 WAL 头（magic/salt/page size/checkpoint seq）；
- 按 frame 读取**已提交事务**；
- 维护 `(salt, frame)` 游标；**检测 checkpoint/截断**（salt 变化或文件截断）→ 报告 Checkpoint 事件；
- 段编码见 P0.3（`internal/ltx`）。

**验收**：☑ 用"替身写入者"（普通 SQLite 进程持续写）验证：捕获全部提交、检测到新提交、TRUNCATE checkpoint 被检测（`go test ./internal/wal` 通过）。真实 workerd 复验见 P0.7。

## P0.3 LTX 复制上传 ☑

**交付**：`internal/ltx`（44 字节段头 + payload + CRC32C，`Encode`/`Decode`）☑；`internal/replica`（Append/List/Read/Restore，按 epoch 链排序）☑；`cell-agent` 端点 `POST /v1/internal/append`、`GET /v1/internal/segments`、`GET /v1/internal/segment`（epoch fence + 内部 token）☑。
**验收**：☑ `go test ./internal/replica ./internal/server`：claim→append→list→read 全链路，epoch 不符被 fence，未授权被拒，Restore 顺序（snapshot→link→delta）正确。

## P0.4 双节点 ensemble ☑

**交付**：`internal/peer` —— follower **fsync spool**（`Append` 落盘 + `Sync`）☑；owner `Replicate` 并发发往 follower、**等 ≥1 个 ack（quorum=1）** 才返回 ☑；`/v1/internal/commit` 实现 **fleet 模式**（quorum 成功后 bucket 上传**异步 + 批量**）与 **bucket 降级**（无/全部 follower 失败 → 同步等 bucket）☑；follower **从 `nodes/*` lease 自动选取**（`lease.SampleCached`，排除自己、最多 2）☑。
**验收**：☑ `go test ./internal/peer ./internal/server`：quorum-1 取首个成功、全失败报错、follower spool 落盘、`mode=fleet`/`acked_by`、无 follower/不可达 follower → `mode=bucket`、从 lease 自动选 follower。**遗留**：gRPC 传输（现 HTTP，见 ADR-038）。

## P0.5 node-log + recovery ☑

**交付**：`internal/nodelog`（`node-logs/<node>/<session>.json`，条件创建 open，CAS recovering/sealed，ListNode）☑；`internal/recovery`（recovery gate：absent/sealed 跳过、open/recovering → CAS fence → 从 follower `Held` 收齐段 → 上传 bucket → seal）☑；follower 侧 `GET /v1/peer/held` 与 owner 侧 `POST /v1/internal/commit` 在 fleet ack 前 `node-log.Open` ☑。
**验收**：☑ `go test ./internal/recovery` 模拟"已 fleet ack（follower fsync）但崩溃未上传"：bucket 初始无段 → recovery 从 follower 收齐 → 段落入 bucket 且 payload 一致 → node-log `sealed`，二次 recovery 为 no-op。**待补**：真实进程 `kill -9` 的接管时延测量见 P0.6。

## P0.6 性能门与报告 ☑

**交付**：`cmd/cellbench`（进程内 owner+follower，测 fleet/bucket 写延迟、restore、PUT/ack）+ `internal/upload`（**group commit 批量上传器**，有界队列溢出同步回退）+ `FSBucket` 操作计数 ☑；报告见 [`p0-report.md`](./p0-report.md)。
**验收**：☑ failures=0；**fleet `put_per_ack ≈ 0.09`（≪1）**；热路径 `list` 仅缓存样本刷新；c=1 fleet p50 ~1ms、c=32 ~29ms（fsync 争用）。**已补真实测量**：真实多进程 + 真实 TCP（`cmd/realbench`）；真实 S3（MinIO，`S3Bucket`/`cmd/s3init`）条件写/range/presign ✅ 与 bucket/fleet 延迟。**仍待测**：云 S3 真实延迟、真实跨主机网络。

## P0.7 workerd 集成 spike ☑

**交付**：使用本地可用 workerd `2026-06-15`（`@cloudflare/workerd-linux-64`）；最小 DO 配置 `workerd/spikes/p0/config.capnp`（`durableObjectNamespaces` + `enableSql` + `durableObjectStorage=(localDisk=...)`）与示例 Worker `workerd/spikes/p0/worker.js`（用 `ctx.storage.sql` 自增）；`cmd/walscan` 只读解析 WAL ☑。
**验收（已验证）**：☑ 真实 workerd 跑通原生 DO + SQLite（`curl` 自增 `{"n":0/1/2}`）；actor 多文件布局确认（`metadata.sqlite` + `<hash>.sqlite`(+wal/shm)）；`walscan` 解析真实 `-wal`（**WAL 模式 + 外部只读**）✅；☑ **`workerLoader` 动态加载租户模块**（`modules` 为 record + `mainModule`，`.js` 后缀）并完成真实 **KV put/get 往返**到 cell-agent（`put:200` / `get:hello-world`）✅。
**待补（非阻塞）**：`globalOutbound` 的**公网-only 隔离**强制（本地 loader 放开 private 以连 cell-agent）；真实 workerd 上 checkpoint 过程对齐（持续写入触发）。

---

## 依赖与顺序

```
P0.1 ──▶ P0.2 ──▶ P0.3 ──▶ P0.4 ──▶ P0.5 ──▶ P0.6
                                  └────────▶ P0.7（可并行启动安装/配置）
```

## 退出标准（见 `roadmap.md` P0）

- WAL/checkpoint/文件布局验证结论；
- `kill -9` 接管 RPO=0、≤5s；
- 双节点写 p50/p99 ≤ 30/100ms；单节点降级 ~90ms；
- 跨节点"捕获→cell-agent→证明" p50/p99；
- 热路径 LIST=0；每 ack PUT 数 ≪ 1；
- bucket 条件写/presigned 通过。

_最后更新：2026-09-14_
