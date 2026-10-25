# 冷恢复首写性能修复

工作区：`fix/review-fixes-20261025`，保留既有未提交 F1–F9 修复。不部署生产、不放宽 RPO=0/owner/session/epoch 校验、不调高验收门槛。

- [x] 用临时 Go overlay 分段记录真实 HK COS 的请求路径，确定约 9s 主耗时。
- [x] 参考 celld 恢复源选择/日志链校验，编写失败回归；先同步设计决策再实现。
- [x] 运行相关单元/race 和全量 Go/vet/build；复审故障恢复、缺段、旧 epoch、请求取消的边界。
- [x] 同配置重跑真实 HK cold E2E，逐阶段记录延迟、已确认键 SHA256、SQLite integrity；保留失败与限制。
- [x] 补验收文档，代码保留待提交，不部署。

前测：SIGKILL 同节点重启首写 8.7s，清空本地 cells 后首写 9.2s，接管 TTFS 18.7s；目标 cell 约 3 MiB。数据无缺失但 1s 首写门槛失败，见 `docs/archive/hk-review-2026-10-25.md`。

- [ ] 完全冷重建首写达到原 1s 门槛：未达标，最终无插桩 2318ms；补充诊断复测 1319ms。仍需 owner 元数据往返与后台基线快照解耦的后续设计和验证。

- [x] ADR-294：后台快照使用独立 SQLite backup 文件上传，增量 capture 继续运行；补慢上传、失败/取消、确认水位、退出等待、大小上限、遗留清理回归。最终真实 HK 验收见 `docs/archive/detached-snapshot-2026-10-25.md`。
