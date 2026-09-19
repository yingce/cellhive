# workerd / Cloudflare 的 sandbox 能力 —— 分层的答案

结论先给：**支持，但必须分成三层说**，混在一起会得到错误结论。

| 层 | 沙箱原语 | 状态 | 适合做什么 |
|---|---|---|---|
| OSS workerd（自托管，CellHive 用） | **isolate 级**：`workerLoader` binding 动态加载 Worker 代码 | 已实测跑通；**必须 `--experimental`** | 跑不受信任的 JS/WASM：无文件系统、无宿主对象、出网由 `globalOutbound` 决定 |
| OSS workerd（自托管） | **容器级**：DO `container` + worker `containerEngine = localDocker` | 已实测跑通（真起 Docker 容器）；schema 注释明确写 "Only used for local development and testing purposes" | 本地开发/测试；**不能当多租户生产隔离** |
| Cloudflare 托管平台 | **Dynamic Workers**（isolate）/ **Containers + Sandbox SDK**（容器） | 平台产品，均需 Workers Paid | 生产多租户沙箱 |

---

## 1. 平台侧：官方把这件事产品化了

### Dynamic Workers（isolate 路线，官方定位 = 轻量沙箱）

`https://developers.cloudflare.com/dynamic-workers/`（Last updated Apr 21, 2026）原文：

> Dynamic Workers let you spin up an unlimited number of Workers to execute arbitrary code specified at runtime. **Dynamic Workers can be used as a lightweight alternative to containers for securely sandboxing code you don't trust.**

> Dynamic Workers are the lowest-level primitive for spinning up a Worker, giving you full control over defining how the Worker is composed, which bindings it receives, whether it can reach the network, and more.

出网控制（`/dynamic-workers/usage/egress-control/`）：

> The `globalOutbound` option in the `WorkerCode` object returned by `get()` or passed to `load()` controls all of this. It intercepts every `fetch()` and `connect()` call the dynamic Worker makes.

> In this mode, you can still give the Dynamic Worker direct access to specific resources and services using bindings. **This is the cleanest and most secure way to design your sandbox: block the Internet, then constructively offer specific capabilities via bindings.**

API 参考（`/dynamic-workers/api-reference/`）：

> If `globalOutbound` is not specified, the default is to inherit the parent's network access, which usually means the dynamic Worker will have full access to the public Internet.

> If `globalOutbound` is `null`, then the dynamic Worker will be totally cut off from the network. Both `fetch()` and `connect()` will throw exceptions.

> As a convenience, the loader implements caching of isolates. … But there is no guarantee: a later call with the same ID may instead start a new isolate from scratch.

> It is never guaranteed that two requests will go to the same isolate. Even if you use the same `WorkerStub` to make multiple requests, they could execute in different isolates.

并发上限（`/dynamic-workers/platform/limits/`，Last updated Aug 27, 2026）：单个 Worker 请求 4 个并发 Dynamic Worker；Durable Object 内 10 个（"previously 4"）。

计费（`/dynamic-workers/pricing/`）：**"Dynamic Workers are currently only available on the Workers Paid plan."** 每月含 1,000 个 unique Dynamic Worker，超出 +$0.002/个/天；请求与 CPU 按 Workers 标准费率另计。计费口径：unique = **Worker ID + code**，同一 ID 同代码多次调用只算 1 个。

### Containers / Sandbox SDK（容器路线）

- `/containers/`（Last updated Aug 28, 2026）：**"Available on Workers Paid plan"** —— 用 Docker 镜像跑完整 Linux 环境，由 Worker 里的代码控制生命周期。
- `/sandbox/`（Last updated Aug 13, 2026）：**"Sandbox SDK 1.0 preview"**，**"Built on Containers, Sandbox SDK provides a simple API for executing commands, managing files, running background processes, and exposing services"**。
- `/sandbox/platform/limits/`：沙箱继承 Containers 的实例/内存/磁盘限制；从 Worker 调用时受 subrequest 限制（Free 50 / Paid 1000），可用 `SANDBOX_TRANSPORT=rpc` 规避。
- 本地开发（`/containers/guides/local-dev/`）：`wrangler dev` / `vite dev` 需要本机 Docker；"When the dev session ends, all associated container instances should be stopped"。

**要点：Sandbox SDK 不是 workerd 的功能，是 Cloudflare 平台 + Containers 的功能。** 自托管 workerd 拿不到它。

---

## 2. OSS workerd 侧：schema 里有什么

来源：`https://raw.githubusercontent.com/cloudflare/workerd/main/src/workerd/server/workerd.capnp`

- `workerLoader` binding（`Binding.workerLoader`，字段 `id`）：
  > A binding representing the ability to dynamically load Workers from code presented at runtime. … each Worker must have a name, and if a Worker with that name already exists, it'll be reused.
- DO namespace 容器选项（`Worker.durableObjectNamespaces[].container`）：
  > If present, Durable Objects in this namespace have attached containers. workerd will talk to the configured container engine to start containers for each Durable Object … The Durable Object can access the container via the ctx.container API.
- 容器后端（**`Worker.containerEngine`，不是 Config 顶层字段**）：
  > localDocker :DockerConfiguration — Use local Docker daemon for container operations. **Only used for local development and testing purposes.**

---

## 3. 本机实测记录（workerd 2026-09-14，本机自托管）

### 3.1 isolate 沙箱（`workerLoader` 动态加载租户代码）

不带 flag 直接启动的报错（原文）：

> service main: Worker loader bindings are an experimental feature which may change or go away in the future. **You must run workerd with `--experimental` to use this feature.**

`workerd serve --experimental config.capnp`，宿主用 `env.LOADER.get(id, () => ({ compatibilityDate, mainModule, modules, env, globalOutbound: env.OUTBOUND }))` 加载一段"租户代码"，实测结果：

```json
{
  "tenant": {
    "typeof_process": "undefined",
    "typeof_require": "undefined",
    "host_env": "undefined",
    "fetch_private": "DENIED: ... connect() blocked by restrictPeers()",
    "fetch_public": "ok:200",
    "node_fs": "err: No such module \"node:fs\".",
    "tenant_module_state": 1        // 第二次同 id 请求 → 2（复用同一 isolate）；不同 id → 重新从 1 开始
  }
}
```

即：租户代码能跑，但看不到宿主 `process`/`require`/env；出网由 `globalOutbound` 指向的 service 决定（配 `network = (allow = ["public"])` 时私有地址被 `restrictPeers()` 拒绝、公网放行）；无 `nodejs_compat` 时 `node:*` 内置不可用。

### 3.2 容器沙箱（DO + `ctx.container` + localDocker）

跑通后的返回：

```json
{
  "has_ctx_container": true,
  "ctx_typeof_container": "object",
  "running_before_start": false,
  "running_after_start": true,
  "exec_ready": true,
  "stdout": "MARKER-INSIDE-CONTAINER\nNAME=\"Alpine Linux\"\nID=alpine\nuid=0(root) gid=0(root) ...\nbin dev etc home lib media mnt opt proc root run sbin srv sys tmp usr var\n0::/",
  "exit_code": 0
}
```

容器确实被 workerd 起起来（`docker ps`）：

```
workerd-sandbox-cbox-probe-<hash>        wtest-sleeper:latest
workerd-sandbox-cbox-probe-<hash>-proxy  cloudflare/proxy-everything:main
```

### 3.3 踩到的坑（复现时会撞上）

1. `containerEngine` 是 **Worker 的字段**，写在 Config 顶层会报 `Struct has no field named 'containerEngine'`。
2. `containerEgressInterceptorImage` **必填**，否则起容器时报 `containerEgressInterceptorImage must be configured for containers`；官方镜像 tag 是 commit 风格（用 `cloudflare/proxy-everything:main`，`:latest` 不存在）。
3. `socketPath` 要带 `unix:` 前缀（`unix:/var/run/docker.sock`），否则被当 host:port 解析 → `DNS lookup failed`。
4. `globalOutbound` 传的是 **Fetcher/ServiceStub 对象**（如 `env.OUTBOUND`），传字符串报 `not of type 'Fetcher'`。
5. `ephemeralLocal` 命名空间要 `--experimental`，且**没有 `idFromName()`**（`get()` 直接收字符串）。
6. 容器镜像需要**长驻 CMD**：`alpine:latest` 默认 `/bin/sh` 无 tty 立即退出（Exited (0)），`exec` 永远 ready 不了；用 `CMD ["sleep","infinity"]` 的镜像。
7. 本机 2026-09-14 版本 workerd 退出时**不回收容器**（残留需手动 `docker rm -f`）；main 分支有 PR #7244 "Clean up containers on graceful shutdown"。

### 3.4 复现文件

- isolate 探针：`/tmp/wtest/{config.capnp,host.js,run.sh}`
- 容器探针：`/tmp/wtest/{cbox.capnp,cbox.js,Dockerfile.sleeper}`

---

## 4. 对 CellHive 的落点

- CellHive 走的是 **isolate 沙箱** 这条线（`workerLoader`，见 `internal/userruntime/userruntime.go`、`internal/doruntime/doruntime.go`、`workerd/user-runtime/loader.js`），与官方 Dynamic Workers 是同一个原语：**隔离边界 = 你显式给的 bindings + `globalOutbound`**，不是进程/文件系统隔离。租户代码拿不到宿主对象，但也别指望它能被 CPU/内存硬限（OSS 侧无平台级 enforcer）。
- loader 缓存语义要在设计里当成"**可能复用、也可能不复用**"：同 id 通常复用同一 isolate（实测复用），官方文档明确不保证。
- OSS workerd 的容器后端定位是本地开发/测试（Docker-in-the-loop），**不能当作生产多租户隔离**；要真容器沙箱只能用 Cloudflare 托管的 Containers / Sandbox SDK（Workers Paid，SDK 1.0 preview）。
