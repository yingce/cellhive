# DO 局部恢复与版本切换验收

状态：源码修复与下列隔离验收通过；未部署到当前 clai 恢复实例，未提交。CellHive 改动位于 celld-ltx-migration worktree，原生改动位于独立 cellhive-workerd-fork 仓库。

## 问题与修复

- 版本切换遇到旧 WebSocket 时原逻辑持续返回 do_runtime_capacity。现在按 facet 合并构建/切换，等待正在执行的旧请求，关闭旧连接（1012），通过 native detach/retire 后加载新版本。等待切换的请求不算旧版本执行中的请求；旧 socket 的迟到回调不能影响新连接。
- WebSocket-only facet 可能在首次 HTTP gate 刷新映射前退出。退出捕获现在先刷新本地 facet 映射，避免写入 relpath scope 后按 object scope 恢复失败。
- 瞬时网络/超时/408/429/5xx 续租失败只在最后确认的有效期内重试，不延长本地 expiry；明确失权立即停止。JS 保留尚有效的 transient claim，当前 guard 请求仍失败关闭。
- 真正失权后，新 owner 恢复先关闭旧 Host 的 admission。只 abort 对应 Host/facet，等待全部 native SQLite 句柄 detach，以及捕获/读取/写入退出；之后丢弃旧本地缓存，按新 claim 恢复。不会用新 owner 身份上传旧缓存；重复的同 claim 恢复是幂等的。
- 元数据普通 SQLite 连接也跟踪 attach/detach。每个 Host 独立锁，其他 shard 的 open/capture 不等待目标 Host 的清理锁。错误/未确认退出保持关闭，不强制删除活跃文件。
- 原生分页连接禁止关闭时自动 checkpoint，保留 WAL 至 supervisor 捕获或丢弃。准备 checkpoint 在任何证书/修改前明确失权返回 412；只 fence 该激活的 I/O，并允许关闭/detach。回调结果不确定、主文件修改失败仍保留既有保护，不将其当作明确失权。
- 删除本地文件不改变 COS LTX 格式，不新增远端 marker。恢复能力自动由 native build-info 的 do-paging-local-close/1 协商；旧二进制不会启用新的局部清理。

## 验收

真实计算进程在本机，COS 桶 cellhive-e2e-hk-1376795072，测试 namespace 独立，结束后清理成功。

- 真实 fork + 本地存储：16 并发版本切换、旧 WebSocket 1012、新激活计数保留、无关 WebSocket 存活，同一 workerd 进程。
- 同一版本切换用例使用真实 HK COS：通过。
- 本地及真实 HK COS：连续三次 owner 丢失后在同一 workerd 接回，epoch 2→3→4→5；每次 16 路读回、BLOB 校验通过，无关 shard 持续请求成功，native PID 不变。
- 真实 HK COS：B→C→B，epoch 5→6→7。C 写入计数 1，B 再接管后的 16 路读均为 1，BLOB 校验不变；B native PID 不变，无关 shard 持续可用。
- 随后五轮写入、idle 退出、16 路重开，值 2..6 均通过。
- supervisor owner/recovery/checkpoint 定向 race 测试通过；DO JS 30 项通过；fork SQLite 48 项通过。
- Go supervisor、DO runtime 与 supervisor 入口回归通过（含启用的原生 paging 测试）；最终校验结果以以下日志为准。

## 证据与复跑

日志（本地，不含凭据）：
- /tmp/cellhive-precommit-review/ws-version-cos.log
- /tmp/cellhive-owner-recovery/e2e.log
- /tmp/cellhive-owner-recovery/cos-cross-node.log
- /tmp/cellhive-owner-recovery/cos-final.log
- /tmp/cellhive-owner-recovery/go-final.log
- /tmp/cellhive-owner-recovery/race-final.log
- /tmp/cellhive-owner-recovery/js-final.log
- /tmp/cellhive-owner-recovery/native-tests.log

新 native 验收二进制：/tmp/cellhive-owner-recovery/workerd，4 jobs 增量构建。未覆盖原有部署二进制。

复跑 owner 故障验收：设置 E2E_OWNER_RECOVERY=1，运行 scripts/e2e/do-idle-retirement.sh；使用 HK COS 时通过 scripts/e2e/hk-review.py --suite do-idle-retirement，并指定支持 do-paging-local-close/1 的 --workerd 和匹配 E2E_BIN。

## 边界

上述验收不是当前线上镜像已更新的证明。当前服务部署仍需更新 supervisor、Host JS 和 native workerd；仅更新其中之一不能宣称局部 owner 恢复生效。原有业务数据卷和备份未改动。
