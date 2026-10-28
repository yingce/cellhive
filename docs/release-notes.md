# 发布说明

_最后更新：2026-10-28_

当前线上版本与最近两次发布的摘要。完整发布记录（镜像、Helm revision、验收证据、残余告警）在 `docs/archive/release-*.md`。

## 2026-10-28 — KV pinned resource-id 修复

- 源码 `49133b1`；镜像 `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:c65a26b74296ecd78a127bc0a12fc8c03938dd7485da677f559716786d5175eb`（tag `49133b1`）。
  - 精确 digest 与部署记录见 [release-2026-10-28](archive/release-2026-10-28.md)。
- SG Helm revision **79**，USW **63**；所有业务 Pod 使用新 imageID、Ready、`restartCount=0`。
- 修复：loader 对 pinned KV binding 签 `b.id` 而不是绑定键（ADR-302）；`scopedtoken` 拒绝 delegated/non-KV claims 携带 `Resource`。
- 验收：每区 20 组 direct KV PUT/GET + 10 组 `name=KV,id=main` 的 Worker binding PUT/GET；临时 app 删除 200。

## 2026-10-27 — COS 恢复、绑定与队列加固

- 源码 `48216e7`；镜像 digest `sha256:c2865514fcee9f12f8491038fd69791dab9ec157da60d57385fd59b86263bd0d`。
- SG Helm revision **71**，USW **56**。
- 包含：COS 空读观察、sealed follower 恢复合批、COS 后台上传合批、固定版本授权后台续期、Queue 响应确认与调度隔离、Workflow 事件/事务栅栏修复等。
- 证据与残余告警见 [release-2026-10-27](archive/release-2026-10-27.md)。

## 更早

- 2026-09 的“未发布（main）”功能清单已归档到 [release-notes-2026-09-main](archive/release-notes-2026-09-main.md)，仅作历史参考；其中功能均已后续发布。
- 其他单点修复记录见 `docs/archive/` 下对应日期文件。
