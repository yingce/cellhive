# 跨模块审查修复验收（2026-10-25）

基线 `213a012`，工作分支 `fix/review-fixes-20261025`，工作区 `/opt/cellhive/.worktrees/review-fixes-20261025`。本批次未提交、未发布，不改变本地现有部署或 SG/USW。历史审查见 [原始报告](project-review-2026-10-25.md)，契约见 ADR-292，[执行计划](../superpowers/plans/2026-10-25-review-fixes.md)。

## 修复和确定性回归

| 问题 | 修复 | 覆盖 |
| --- | --- | --- |
| F1 Queue 无界分配 | 配置上限 100，runner 防御存量值 | 原生/CF 配置 101 拒绝、100 接受；MaxInt runner 安全返回 |
| F2 adaptive router fatal unlock | flight 等待后重新进入锁与缓存判定 | 子进程 fatal 回归、结果过期重查、取消、32 调用单飞 |
| F3 冷恢复隔离失效 | 内部 timeout 前分类；HTTP/admin 显式交互标记 | 默认 timeout 后台池不侵占交互池；真实无 deadline HTTP 请求不被后台恢复饿死 |
| F4 R2 旧 metadata | 旧 sidecar 按存储 ETag 条件清理；读时校验对象版本，size/ETag 权威值 | 不同/相同内容覆盖、失败 onlyIf、sidecar 失败、迟到发布版本错配、range 版本改变；两个 Store 延迟清理不删除新 sidecar；OSS 不支持时写前拒绝 |
| F5 D1 二次方预算统计 | 线性累计，BLOB 分配前计入展开载荷，逐行取消 | BLOB/text/numeric 上限及边界、行数、预取消/中途取消、SQLite benchmark |
| F6 同 cell 等待不可取消 | context.AfterFunc 唤醒 cond；生产调用迁移 BeginRequestContext | cold open/eviction 等待取消，保留生产者和请求保护 |
| F7 随机 kid 刷新放大 | 全局 min(30s,TTL) 刷新间隔和失败退避 | 顺序 unknown kid、并发刷新、初始/后续失败退避、短 TTL、轮换 key 恢复 |
| F8 busy 计数 map 增长 | 归零删除 | 请求完成后无残留条目 |
| F9 CI 缺 SQLite tags | workflow 调用 make test/vet | 原无 tags 命令触发编译 guard；统一 Makefile 命令通过 |

新增回归均先对旧实现观察失败再修复；未在生产执行危险 Queue 大内存分配。集成中另外发现 HTTP 入站通常无 deadline，因此补显式交互标记；最终复审发现无条件 sidecar 清理竞态，因此改成条件删除并覆盖 provider 能力边界。

## 测试命令与证据

Go tags 统一使用：`sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook`。实际 workerd 为 `/opt/cellhive/deploy/workerd-fork/workerd`，无 fork 改动。

- `make test`：全量 Go 通过，首次集成证据 `/tmp/cellhive-review-fixes-make-test.log`。
- `make vet build`：通过，`/tmp/cellhive-review-fixes-build.log`。
- `make js-test`：通过，`/tmp/cellhive-review-fixes-js.log`。
- 修改模块 race：auth、queue/control/CF、userruntimepool、D1、cellstore/server、bucket/R2 通过。cellstore/server 完整包证据 `/tmp/cellstore-server-final-race.log`；R2 最终 provider/条件删除证据摘要 `/tmp/r2-provider-review.txt`；其它 red/green 证据 `/tmp/queue-auth-*.log`、`/tmp/cellstore-review-*.log`、`/tmp/http-hydration-*.log`、`/tmp/cellhive-fix-20261025-d1-*.log`。
- `scripts/e2e/queue.sh`：真实 agent/workerd Queue 重试→DLQ、R2 metadata 等通过，`/tmp/cellhive-review-fixes-queue-e2e.log`。
- `scripts/e2e/queue-multinode.sh`：两个真实 agent，杀死 n1 后接管并消费 8 条通过，`/tmp/cellhive-review-fixes-queue-multinode.log`。
- 新增 `scripts/e2e/review-fixes.sh`：真实 agent + workerd + 公共 HTTP，Queue 101 拒绝/100 接受、R2 两种覆盖和 HEAD/GET/range、D1 20k 行正确性通过，`/tmp/cellhive-review-fixes-e2e.log`。
- 最终 R2 修改后的全量检查与重跑 E2E：见下方最终验收状态。

真实 E2E 复跑命令（临时目录、独立端口、文件桶）：

```bash
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY NO_PROXY='*' \
  CELLHIVE_WORKERD=/opt/cellhive/deploy/workerd-fork/workerd \
  E2E_PORT_BASE=24100 bash scripts/e2e/review-fixes.sh
```

## 性能结果

D1 `runOne` 的真实 SQLite benchmark，多轮中位数：

| 行数 | 修复前 | 修复后 |
| --- | ---: | ---: |
| 1,000 | 5.33 ms | 2.71 ms |
| 5,000 | 69.03 ms | 5.51 ms |
| 20,000 | 1,072.58 ms | 19.26 ms |

20k 行约 56 倍改善来自去掉对已累计结果的反复遍历，不是改变 SQLite 查询/持久性语义。真实 workerd 公共 HTTP 五次（服务端完整读取 20k 行并检查末行，返回结果摘要）：47.661、43.246、43.906、47.157、44.055 ms。HTTP 时间包含路由、binding、SQLite、序列化和 Worker 处理；**不代表 20k 行直接传输到外部客户端的时间，也不是生产 RPS**。同机文件桶、短时单请求样本，未验证生产 COS/跨机 fleet。

## 边界与下一阶段

- R2 数据和 sidecar 分开存储，仍非原子：迟到 metadata 发布可能覆盖新 metadata；不同内容读取会过滤不匹配版本，但相同内容 ETag/相同 sidecar 字节 ABA 不能完全区分。错误响应可能已写入数据，不自动重放。
- ProviderBucket 的 authority/point-fence 删除走已有权威协议；raw COS 条件删除受支持，OSS/未知 append provider raw 路径拒绝。存在旧 sidecar 的覆盖会在写数据前拒绝不支持的 provider。generic S3 依赖 Bucket 契约和启动 Diagnose 的条件删除测试，本轮未访问外部 provider 做验收。
- 每次 R2 PUT 多一次旧 sidecar Get，存在旧 sidecar 则增加条件删除，带 metadata 另需 Put；正确性修复不宣称降低 R2 写入延迟。
- D1 32 MiB 为保留载荷预算，不含全部 Go 对象开销或 JSON 编码大小；JWKS 新 key 可受最长 min(30s,TTL) 的窗口延迟；Queue 上限是每队列批次并发，不是节点总准入。
- 下一阶段顺序：Queue ack/retry 批量事务与慢任务隔离 → DO 连接自愈和续租容量 → SQLite RSS/FD 准入。Cron 补跑需独立 misfire 契约，当前不补跑行为保持。
- 本次不是全仓逐行审计，不宣称消除所有灾难恢复/性能风险；未修改 owner/epoch/持久性输出门，也没有新增热路径 List。

## 最终验收状态

最终条件删除/provider 补丁后，`make test vet build` 全部通过（退出码 0，`/tmp/cellhive-review-fixes-final-checks.log`），真实 HTTP E2E 再次通过（退出码 0，`/tmp/cellhive-review-fixes-final-e2e.log`）。最终五次 20k 行 HTTP 耗时为 51.987、45.350、46.060、45.341、47.589 ms。独立最终复审通过，无新增阻塞问题；既有 R2 非原子边界如上。

ProviderBucket 拒绝不安全的 raw 条件删除也影响 OSS 的 best-effort R2 用量 marker 清理：可能报错并留下统计偏差，不影响对象删除结果。要消除此限制，应实现并验证 provider 的安全条件操作，不可恢复无条件删除。

计划任务 1–8 验收完成；工作区代码保持未提交，未部署。主工作区 `/opt/cellhive` 仍干净。

## 追加真实 HK COS 验收

后续按用户要求运行本机进程连接真实香港 COS，详见 [HK 验收](hk-review-2026-10-25.md)。R2/D1/Queue 及条件删除通过；KV 重启首写已复现秒级延迟，不得将此前本地文件桶通过解读为真实 COS 的完整性能通过。
