# 链路与协议

本文件集中定义 CellHive 各组件之间"走什么协议、为什么"，是 [`cell-protocol.md`](./cell-protocol.md) §12 与 **ADR-026 / ADR-027** 的展开；**gRPC 平面已取消（ADR-136），内部统一 HTTP**。目标是：**热路径最小开销、workerd(JS)/对外保持 REST/JSON、Go↔Go 用 REST + 二进制帧流**。

## 0. 原则

1. **区分南北向与东西向**：南北向经运维边缘代理（TLS + host 分流）；东西向走私网直连，不经边缘代理。
2. **按"参与者"选协议，不按性能**：
   - **Go↔Go** → HTTP/REST（LTX 热路径为长度前缀二进制帧 + HTTP 101 持久流，ADR-042）；
   - **凡 workerd(JS) 参与** → HTTP/REST/JSRPC；
   - **对外/租户** → REST/JSON。
3. **二进制热路径用帧流，绝不用 JSON**（避免 base64 膨胀与文本解析）。
4. **热路径最小化**：跳数、序列化、分配、往返次数（连接复用、group commit）。

## 1. 推荐总表

| # | 链路 | 两端 | 频率 | 推荐协议 | 理由 / 备注 |
|---|---|---|---|---|---|
| 1 | 公网 → Traefik → `user-runtime:8081` | 外部↔workerd | 高 | **HTTPS（REST）** | 南北向；TLS 在 Traefik |
| 2 | 公网/CLI → Traefik → `cell-agent:8082` | 外部↔Go | 低 | **HTTPS（REST/JSON）** | admin/控制面；token 鉴权 |
| 3 | `user-runtime`(JS) → `cell-agent:7001`（bindings） | JS↔Go | **高（热）** | **REST/JSON**（长连接 keep-alive） | JS 侧不适合 gRPC；用连接复用 |
| 4 | `user-runtime`(host adapter) → owner `do-runtime:8788`（DO/WS） | JS↔JS | **高（热）** | **HTTP / JSRPC** | workerd 间调用；WS 走 upgrade |
| 5 | `cell-agent` → `user-runtime:8088`（scheduled/queue/workflow 派发） | Go↔JS | 中 | **REST/JSON** | 目标是 workerd |
| 6 | `cell-agent` ↔ `cell-agent`（control：acquire/release/seal/tail/recovery/claim） | Go↔Go | 低–中 | **REST/JSON（HTTP/1.1）** | 连接池复用；无 gRPC（ADR-136） |
| 7 | `cell-agent` ↔ `cell-agent`（**LTX append/tail 热路径**） | Go↔Go | **高（热）** | **长度前缀二进制帧 + HTTP 101 持久流**（失败回退 `/append_batch`；ADR-042） | 避免 JSON 膨胀；多 lane + 在途多批 |
| 8 | do-runtime supervisor(Go) → `cell-agent:7001`（WAL 上报 / claim / restore） | Go↔Go | 高（WAL）/ 低 | **REST/JSON**（WAL 段为二进制帧） | 与 peer 同一 HTTP 平面 |
| 9 | do-runtime 内部：supervisor ↔ workerd | 同 Pod | 高 | **本地**（loopback HTTP / JSRPC） | 非网络协议；WAL 捕获是**文件读取** |
| 10 | `cell-agent` → 对象存储 | Go↔S3 | 中 | **S3 API（HTTPS）** | 唯一桶访问者；fleet 模式下后台合批、不在 ack 路径 |
| 11 | 控制面 → `user-runtime`（路由投影下发） | Go↔JS | 低 | **REST/JSON**（纯拉取 + 5–10s TTL，无 push） | 见 ADR-031 |
| 12 | `cell-agent` ↔ bucket（owner 记录 / node lease / 条件写） | Go↔S3 | 低–中 | **S3 API（HTTPS）** | 点查，禁止 LIST；非 peer |
| 13 | 南北向内部（Traefik → 后端） | Traefik↔后端 | 高 | **HTTP(S)**（ClusterIP / service 名） | 由 service name/DNS 解析 |

**规则**：内部**全部 HTTP**（`#3`–`#8`、`#11`；其中 `#7` 是二进制帧流）；`#1/#2` 南北向 HTTPS。

### 端口划分（M-02 定稿）

| 端口 | 协议 | 用途 |
|---|---|---|
| `:7001` | **REST/JSON + 二进制帧** | 内部全部：Go↔Go（peer LTX、DO WAL/claim/restore）与 workerd(JS) bindings 共用 |
| `:8082` | REST/JSON | 对外 admin/控制面（经边缘代理） |
| `:8081` | HTTP | user-runtime 公开 loader（经边缘代理） |
| `:8088` | HTTP | user-runtime 内部特权派发（仅私网） |
| `:8788` | HTTP | do-runtime 私网（owner 调用/WS） |

## 2. 热路径专项：DO 写

```
workerd 提交（同步 SQL）
  → supervisor 捕获 WAL（文件读取，本地）
  → HTTP 原始字节帧上报 cell-agent（可跨节点）
  → cell-agent 写 e<epoch> 前缀 + 持久化证明（fleet ensemble）
  → host adapter 输出门放行
  → 返回
```

热路径优化项：

- 本地捕获、**连接复用**、**group commit**（多笔合一次 append/fsync）；
- owner 与 follower **同 AZ 亲和**；
- **原始字节帧**而非 JSON/protobuf；
- bucket 上传**后台异步**（fleet 模式不在 ack 路径）。

## 3. 备选与演进

| 备选 | 说明 |
|---|---|
| **Connect(Buf)** | 同一 protobuf 服务统一定义；可统一内外，但增工具链，默认不引入 |
| **HTTP/2** | 连接多路复用；当前用 HTTP/1.1 + 连接池 + 多 lane，未证明是瓶颈 |
| **原生 gRPC** | 已明确**不做**（ADR-136）：Go↔Go 也无 gRPC 依赖 |
| **原始 TCP 帧流 / QUIC** | LTX 热路径的更快形态；接口保持不变，按基准决定是否下沉 |

## 4. 待定

- LTX 热路径是否从 HTTP 101 持久流下沉为原始 TCP 帧流（`#7`，接口不变）——按基准决定。
- 路由投影的下发已是**纯拉取 + ETag/revision**（ADR-115/116）；推送不在计划内。

## 5. P0 验证

- `#7/#8`：peer append 往返 p50/p99；group commit 合批比；hedge 命中/无效比例（自适应 hedge 已实现，默认关，ADR-164）。
- `#3`：binding 调用 p50/p99（连接复用是否足够）。
- `#4`：DO 调用/WS 代理延迟。
- `#10`：bucket 上传是否确实不在 ack 路径。

_最后更新：2026-09-14_
