# 致谢与设计参考

CellHive 的状态模型、运行时接入方式与部分工程手法，参考了同领域几个开源项目的**设计与契约**。借的是设计，不是实现：除明确属于第三方且按其许可证复用的部分外，本仓库代码为自研。

## celld（`denoland/celld`）

一个自托管的分布式 Durable Objects 平台（自带 V8 的一体化 daemon，长期状态放在你自己的 bucket）。

- **借鉴的设计**：cell = 一份 SQLite 的状态单元；**bucket 条件写决定 owner**；**epoch 围栏**；提交的写捕获为 **LTX**；**RPO=0**（单节点等桶、多节点由 peer 先持有）；持久寻址与节点可替换；节点 lease 作为伸缩信号；**优雅 handoff**；以及"**索引一律非权威、可修复**"的纪律（CellHive 的 `wake/` 索引与修复循环即由此而来）。
- **未借鉴**：celld **自带 V8 执行引擎**——CellHive 坚持用**原版（stock）workerd**，只经 `workerLoader`/bindings/capnp 接入；也**不 fork、不改造** celld 的二进制。
- 对照参考见 [`decisions.md`](./decisions.md)（cell 协议、LTX、capture、持久性姿态、handoff 等相关 ADR）。

## WDL（`wdl-dev/wdl`）

一个基于 stock workerd 的自建多租户 Workers 平台（独立服务族 + 外部状态存储）。

- **借鉴的模式与工程手法**：`workerLoader` **多租户动态加载**不可变版本；**binding host adapter**（平台 worker 导出入口类，`ctx.exports.X({props})` 生成 props 绑定的 RPC stub）；版本/回滚；**双 socket 特权分离**；DO 的 **host actor + facets** 组织方式与 supervisor 生命周期；**alarm shim** 思路（stock workerd 对 SQLite facet 不实现原生 alarm）；以及**按模块组织文档 + 给出阅读路径**的文档方式（本仓 `docs/modules/` 与 `docs/contributing.md`）。
- **未借鉴**：WDL 的 **Redis/Valkey 状态模型**（CellHive 用 bucket + SQLite + 条件写）、**独立 gateway 组件**（CellHive 用 Traefik + user-runtime loader）、以及把 **scheduler / workflows 拆成独立 Rust 服务**（CellHive 在 `cell-agent` 内统一计时与派发）。

## LiteFS / `superfly/ltx`

Go 侧 SQLite 复制与 LTX 格式的**实现参照**：`internal/ltx`、`internal/replica`、`internal/compaction` 等遵循 LTX 的页映射/快照/压实思路（我们不直接搬 Rust 实现）。

## 关于许可与署名

- 复用任何 **Apache-2.0** 许可的第三方代码或设计时，须保留对应的 `LICENSE`/`NOTICE` 与署名（见 [`decisions.md`](./decisions.md) 的复制/许可相关 ADR）。
- 本文档仅说明**设计来源与致谢**，不构成与上述项目的隶属、赞助或背书关系。感谢这些项目的作者与社区公开他们的设计与经验。

_最后更新：2026-09-19_
