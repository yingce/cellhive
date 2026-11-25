# 平台 JS 内存与 trace 修复（2026-11-25）

范围：本地工作区修复 Tail、R2 平台 binding、并发 trace、Assets binding 四项问题；未提交、未部署、未重启线上服务。

## 实现

- Tail 淘汰立即清空全局数组槽位，并同步从所属 worker ring 移除记录。所有 ring 的记录均为全局保留集合的子集，8 MiB 预算覆盖记录并集；保留 10000 条、200 worker、每 worker 1 MiB/1000 条限制。计数是序列化记录大小，不是精确 JS heap/RSS。
- R2 平台 entrypoint 通过 RPC 只传递上游 stream 与元数据，接收端恢复 text/json/arrayBuffer/blob；这些方法与 body 共用一次消费状态。旧 facade 同样不预读正文，不 tee，不持有第二份完整 buffer。独立初始化模块先于租户模块求值，覆盖模块顶层保存 imported env binding 的用法。
- 请求 trace/request ID 使用 AsyncLocalStorage，binding RPC 显式传递上下文；原生 fetch 用请求头传递，保留 WebSocket upgrade。Queue、Cron、Workflow、Service handler 的异步调用互不覆盖；KV 微批次按 invocation 分组。同一请求内继续批量合并，保留子请求配额对本地操作的豁免。
- Assets 支持 Request 和 RequestInit 的 HEAD，转发方法与 Content-Length，GET 直接返回流。agent 内部 asset 接口 HEAD 调用 StatAsset，GET 调用 OpenAsset，并关闭 reader；保留 Objects 的前缀属主与路径验证，不使用 List 或整对象 Get fallback。

## 回归证据

- 修复前，400 条约 250 KB 记录的序列化保留量为 100010800 字节，但全局计数仅 8250891 字节；新增单 worker、100 worker、220 worker 引用预算回归先失败后通过。
- 阻塞上游 EOF 的测试验证 R2 get 先返回、首块可读；显式消费后 bodyUsed 为 true，重复消费拒绝。涵盖 platform RPC 返回形态与旧 facade。
- 真实 workerd 测试 TestUserRuntimeStreamingAndConcurrentTrace：agent 模拟上游等待客户端读到首块才发送尾块；客户端成功读取 first/last。A 请求在 KV 等待，B 完成后 A 恢复，后续 KV 请求 trace 分别为 A/B。
- Workflow 交错执行、后台 Promise、凭据签发等待交错、跨请求 KV 微批次、内部方法拒绝均有 Node 回归；原有 DO WebSocket 跨 RPC/公开 loader 回归通过。
- agent 单元测试验证 HEAD 只调用 Stat，GET 只调用 OpenReader，结束后关闭 reader；JS 验证 Assets GET 取消向上游传播。

## 验证状态

- 全量 `make test` 通过；最后新增 binding 初始化前置后，对受影响的 `internal/userruntime`、`internal/doruntime`、`internal/plangen` 使用 `-count=1 -p 1` 整包复验通过（34.619s / 34.857s / 0.075s）。
- Node 平台/user-runtime/do-runtime 全部 `*test.mjs`：200/200 通过，无跳过。
- `make build`、`make vet`、`git diff --check` 通过。
- 最后补测：启用 subrequest limit 的真实流式/并发 trace/模块顶层 R2 capture 测试通过；独立静态生成 R2 模块顶层 capture 测试通过。
- 原始命令输出保留在本机 `/tmp/cellhive-fixes-go-test-final.log`、`/tmp/cellhive-fixes-runtime-final.log`、`/tmp/cellhive-js-fixes-tests-final.log`、`/tmp/cellhive-fixes-build-final.log`、`/tmp/cellhive-fixes-vet-final.log`。

## 边界

- 上述 workerd 验证为本机独立临时目录与 loopback 测试服务；没有在线上制造 OOM，没有测量生产 RSS 或并发容量。
- R2 本轮修复的是平台 JS 的全量缓冲；agent 的 R2 Snapshot.Get/GetRange 既有聚合路径未改，不宣称对象存储到客户端全链路零缓冲。
- Assets 的 COS Stat 会读取固定 16 字节 frame 头；FSBucket 为计算内容 ETag 仍以有界内存扫描本地文件。这不是 HEAD 下载完整云端资产，也不宣称本地磁盘零读。
- nodejs_als 是平台 wrapper 的运行依赖，动态、静态及 DO 配置同步开启；没有修改 workerd fork 或放开完整 nodejs_compat。
