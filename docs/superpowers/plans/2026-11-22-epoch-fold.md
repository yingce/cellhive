# Epoch Fold Implementation Plan

Goal: 接管后异步收敛恢复链，同代仅 L0→L1。
Architecture: 后台内存 owned 扫描与 scope+epoch 重试状态；复用 Fold 发布内核；新增完整 L9 优先策略，不修改输入或 GC。

- [x] 更新设计、decisions、known-issues，明确替代旧触发。
- [x] 新增 FoldEpoch：接链检查、固定切点、完整快照预算、失败回退。
- [x] 新增 agent 换代维护队列与测试；移除生产 idle/pre-release/shed Fold 入口。
- [x] 回归并发尾部、未接链、幂等、预算与多代恢复。
- [x] 运行相关测试、race、build/vet；记录未验证项，不提交/部署。

验收边界：全量测试/vet 有既有 WAL 测试导入环；完整 compaction race 的 GiB 用例超时，见验证报告。
