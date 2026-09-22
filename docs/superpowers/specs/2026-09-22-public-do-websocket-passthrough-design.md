# 公开入口 DO WebSocket 透传修复

日期：2026-09-22

## 背景

真实 Docker 验收发现：DO 自身的 WebSocket echo、1012 abort、跨节点转发，以及租户 Worker 在内部消费 DO WebSocket 均可工作；但外部客户端经 `user-runtime :8081` 连接一个直接返回 DO WebSocket 101 的 Worker 时，会得到 500：

```text
DataCloneError: Could not serialize object of type "WebSocket".
```

根因是公开 loader 通过 `CellHiveHost.handleFetch(request)` 普通 RPC 方法取回租户响应。普通 RPC 会结构化序列化返回值，不能携带 `Response.webSocket`。ADR-184 已记录 stock workerd 的约束：只有名为 `fetch`、首参为 `Request` 的 fetch-shaped RPC 保留 101/WebSocket 透传语义，但公开入口最后一跳未遵守该约束。

## 决策

公开 fetch 路径改为调用 loaded Worker entrypoint 的 `fetch(request)`，使整条链路每个需要携带 WebSocket 的 RPC 边界都保持 fetch 语义：

```text
external client
  -> user-runtime public loader
  -> CellHiveHost.fetch(Request)
  -> tenant Worker fetch
  -> DurableObjectNamespace.fetch(Request)
  -> cell-agent connect lookup
  -> owning do-runtime /v1/do/connect
  -> tenant Durable Object
```

`handleFetch` 可继续用于不需要返回 WebSocket 的内部派发兼容路径；本修复不扩大到 queue、scheduled、service RPC 或其他无关调用。

## 安全边界

- 不恢复 `CH_DO_CONNECT`、`CELLHIVE_CAP_WS` 或任何 tenant-visible 平台键。
- `CH_*`、`CELL_*`、`__cellhive*` 以及历史平台名称继续是用户自定义空间。
- owner lookup、短期 shard ticket、cell-agent URL 和内部 token 全部留在可信平台 Worker。
- 不修改或 fork workerd，不新增第二套 JS 引擎、gateway 或旁路网络代理。
- 普通 HTTP fetch 的状态、header、body、异常处理、trace flush 与现有行为保持一致。

## 测试设计

按 TDD 实施，先加入在旧实现上稳定失败的公开入口回归：

1. 使用固定版本 stock workerd 启动真实 user-runtime loader；
2. tenant Worker 将公开 Upgrade 请求直接转发到 DO binding；
3. 外部 RFC 6455 客户端连接公开端口，必须取得 101；
4. 验证双向 text frame 往返，而非只断言握手状态；
5. 断言 tenant env 中 `CH_DO_CONNECT`、`PLATFORM`、`CELL_URL` 等平台键仍不存在；
6. 保留并重跑内部 DO WebSocket、1012 abort、跨节点转发及完整 DO compatibility suite。

Docker 验收使用 `examples/do-websocket` 走真实 `cellhive deploy`，链路为公开 `user-runtime :8081` 到 gated do-runtime。至少验证：非升级请求 426、WebSocket 101、连续消息计数、权威 LTX 产生、do-runtime 重启行为和资源清理。任何 skip 均不算通过。

## 文档修正

实现与验收同时修正 ADR-184、`known-issues` 和 `testing` 的中英文说明。文档必须区分：

- DO 内部 WebSocket transport 已工作；
- 外部公开 101 还要求 user-runtime 最后一跳保持 fetch-shaped RPC；
- 只有自动回归和真实 Docker 请求均通过后，才能标记公开入口问题已解决。

## 非目标

- 不改变 DO ownership、epoch、lease、迁移和存储模型；
- 不设计跨进程 WebSocket 会话恢复；do-runtime abort/restart 仍以 1012 通知客户端重连；
- 不引入新租户配置、环境变量或绑定类型；
- 不顺带重构 service binding 或内部 dispatch。

## 验收标准

- 新公开入口回归在修复前因 WebSocket 序列化失败而红，在修复后通过；
- pinned workerd 的完整 DO compatibility suite 19/19 PASS、零 skip；
- 真实 Docker 公开 WebSocket 请求完成握手和双向消息；
- tenant env 平台键仍为零；
- `make js-test`、相关 Go 测试及 `REQUIRE_ALL=1 bash scripts/ci.sh` 通过；
- 测试容器、网络、volume 和生成文件均清理，Git 仅包含预期改动。
