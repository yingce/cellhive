# 动态加载内存压测：2026-10-13

## 方法

- 代码：`fb909f8` 加本次 opt-in 压测；生产 JS loader/host 未修改。
- stock workerd：`cellhive:runtime-baseline` 中的 `workerd 2026-09-16`，测试时 Render 使用当前仓库 JS。
- Linux Docker cgroup；user-runtime 2 GiB、DO runtime 4 GiB，swap 禁用，2 CPU。
- 编译在测量容器外完成；测试 driver 和 stub HTTP 控制面与 workerd 在同一 cgroup。
- `/proc/*/status` 记录 workerd RSS；`memory.current` 记录整个 cgroup；结束后 `docker inspect` 验证 OOMKilled。
- 顺序请求，并非并发吞吐测试。生产 host 扫描限流保留，每 75 个不同 host 等待 1 秒。
- Worker bundle 为最小 handler + 指定尺寸注释。实际源码略大于参数字节数；注释避免混入租户业务分配。SHA/version 是隔离 mock 元数据，不测真实控制面和对象存储。
- 公共 loader 关闭 native Tail，使用无 binding fetch fast path；DO 为 resident host、非 gated 路径，无 tenant 内存数组及 SQL 写操作。输出门/supervisor、日志和业务分配可能增加生产占用。
- 以下计数是**该合成负载的观测点**，不能作为通用安全容量。请求 index 从 0 开始，成功数与失败 index 相同。

## 结果

| 场景 | 上限 | 成功数/请求数 | workerd RSS 观测 | 结果 |
|---|---|---|---|---|
| 不同 Worker，1 KiB 注释 | 2 GiB | 1,432 成功，第 1,433 次失败 | 1,425 个时 2,027 MiB | OOMKilled=true |
| 不同 Worker，1 MiB 注释 | 2 GiB | 451 成功，第 452 次失败 | 450 个时 2,058 MiB | OOMKilled=true |
| 同一 Worker，不同源码/版本，1 MiB | 2 GiB | 451 成功，第 452 次失败 | 450 个时 2,059 MiB | OOMKilled=true |
| 同一 Worker，同 SHA/源码，不同版本号，1 MiB | 2 GiB | 575 成功，第 576 次失败 | 575 个时 2,056 MiB；bundle 仅读一次 | OOMKilled=true |
| 不同 Worker，10 MiB 注释 | 2 GiB | 60 成功，第 61 次失败 | 50 个时 1,682 MiB | OOMKilled=true |
| 静态资源，每项 10 MiB | 2 GiB | 189 成功，第 190 次失败 | 175 项时 1,882 MiB | OOMKilled=true |
| 同 Worker/版本，1 MiB，重复请求 | 2 GiB | 3,000 次全部成功 | 96.6 MiB；idle 10s 后 96.9 MiB；bundle 仅读一次 | 未 OOM |
| 不同 Worker，1 MiB，200 个后空闲 | 2 GiB | 200 成功 | 953.0 MiB；idle 10s 后 953.3 MiB | 未见显著回落 |
| 同一 DO Worker，1 KiB，5,000 个 facet | 4 GiB | 5,000 次全部成功 | 1,337.5 MiB；idle 10s 不回落；cgroup 约 1,395 MiB | 未 OOM |
| 不同 DO Worker/sha，每项 1 MiB、一个 facet | 4 GiB | 770 成功，第 771 次失败 | 700 个时 3,702.5 MiB | OOMKilled=true |

通过最终 runner 重复执行 1 MiB 不同 Worker 场景，再次得到 OOMKilled=true：449 个成功、第 450 次失败，与首次 451 个成功的结果接近。不能把单次失败计数当作精确容量保证。

### DO 文件描述符对照

默认 nofile=1024 时，在复用 HTTP client 后，约 302 个 resident facet 就出现 `SQLITE_CANTOPEN`，不是 OOM。提高到 65536 后，同样类型的 5,000 facet 测试通过。最早一轮使用单请求单 client 还触发 `Too many open files`，已排除这项 driver 干扰。生产必须检查实际容器 nofile 与 SQLite/连接 FD 占用。

## 解释与边界

1. **OOM 已实测，不只是静态计算。** 在缓存条目上限 2,000/4,000 生效之前，2 GiB 已不足。
2. 小代码 Worker 仍有明显固定运行时成本；仅限制 bundle 字节不够。
3. 同 SHA 的多版本只读一次 bundle，但 RSS 仍增长至 OOM，说明源码缓存去重无法覆盖版本相关的 workerLoader/执行状态开销。尚未对 V8 heap/编译代码/native allocation 做分项归因，也未证明具体 workerd 内部缓存机制。
4. 重复调用同一个 id 的对照远小于新增 id；不支持“每次 get 都必然创建新 isolate”的说法。
5. DO `BUNDLE_CACHE_MAX=64` 是**每个 Host 实例**的缓存，不是整个 runtime 的总上限；不同 worker/shard 的 host 数量增加会扩大 aggregate 占用。不能拿 `64×bundle_size` 当作进程上界。
6. 只观察了 10 秒空闲，不能据此断言永久泄漏或永不驱逐；也没有进行数小时 churn、并发突发、大量 bindings/Tail 或真实云桶测试。
7. RSS 包括共享/文件映射页，不能与 cgroup 独占计费直接等同。整个 cgroup 包括 driver/stub；workerd RSS 增长与 OOM 状态同时记录。
8. 合成 bundle 使用注释，而非真实大框架；真实代码编译及 tenant heap 可能更重。

## 重现

需要 Docker、Python3 与已构建的 pinned CellHive 镜像（例如 `make docker-build` 的 `cellhive:dev`）。编译器默认 `golang:1.27-bookworm`，在外部容器构建测试二进制。

```bash
# 预计触发 OOM；runner 返回非零，仍保存 .log/.json 证据。
python3 scripts/runtime-memory-stress.py \
  --runtime-image cellhive:dev --mode workers --count 1000 \
  --bytes 1048576 --memory 2g --output /tmp/cellhive-memory-workers

# 相同源码只变版本号
python3 scripts/runtime-memory-stress.py \
  --runtime-image cellhive:dev --mode same-sha --count 2000 \
  --bytes 1048576 --memory 2g --output /tmp/cellhive-memory-versions

# DO facet 对照
python3 scripts/runtime-memory-stress.py \
  --runtime-image cellhive:dev --mode do-facets --count 5000 \
  --bytes 1024 --memory 4g --output /tmp/cellhive-memory-do
```

其他模式：`hot`、`assets`、`versions`、`do-workers`。测试默认 skip，不纳入常规 CI；runner 保存 image ID、runtime version、内存/FD 限制、环境、退出状态和日志，然后清理自己创建的容器。

本机原始证据：`/tmp/opencode/cellhive-memory-evidence/`（11 组日志和 Docker inspect JSON）；runner smoke 验证：`/tmp/opencode/cellhive-memory-runner-check/`。
重复 OOM 证据：`/tmp/opencode/cellhive-memory-repeat/`。

## 待修复方向

后续已完成 pinned 源码与实际二进制生命周期验证：named loader 的内部强引用 Map 不随 JS LRU 淘汰；匿名加载和动态 isolate 内的 `abortIsolate()` 可释放实例并使同名重新加载。参见 [`workerd-loader-lifecycle-2026-10-13.md`](workerd-loader-lifecycle-2026-10-13.md)。生产释放/并发协调仍未实现。

2026-10-13 后续实现了公共 user-runtime 的首版受控退休与字节预算：版本切换后旧版本停止接收新请求；handler、Response body、`waitUntil` 和错误路径都参与活动引用；达到容量时拒绝新的冷加载；bundle/prepared/env/asset/meta cache 改为有总字节上限，大 asset 不进内存缓存。使用同一 2 GiB/2 CPU/1 MiB×1000 合成 Worker 压测时不再 OOMKilled，最终观测 workerd RSS 约 840 MiB、cgroup 约 809 MiB，idle 10s 约 840/809 MiB；workerd 会为主动退休打印内部 abort 错误日志。该结果是回归证据，不是生产容量承诺；DO、内部 dispatch、WS/RPC 长连接、FD 和长时 churn 仍需独立验证。

- 总字节预算 + 单项限额的 bundle/prepared/assets 缓存；大资源流式路径。
- 动态加载版本/isolate 的容量管理、内存高水位拒绝新加载与可验证的回收策略；仅 JS LRU 不足以证明 compiled worker 被释放。
- DO host/facet residency、idle 生命周期以及进程级别 aggregate 限制。
- 每个 runtime RSS/cgroup、加载/缓存数量与 FD 指标；保护性 drain/restart 必须验证持久性与在途请求行为。
- 修复后用相同负载重跑，要求内存趋稳或受控拒绝，而非 OOMKilled。
