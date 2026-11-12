# Authority 分段物理回收实验

状态：代码未提交；USW 已通过两阶段升级启用正常换代物理回收，默认仍保留占位对象。运行时模式、发布证据与回退约束见 [USW 发布记录](2026-11-11-usw-authority-reclamation.md)。下文早期实验记录的“未部署/无开关”描述仅适用于当时状态。

## 范围与前提

通过 `AuthorityOptions.ReclaimRetired` 显式启用，不改变 LTX 或现有 CHA3 字节布局。根必须永久保留，不得删除/重建；代数单调增长，key 不复用。所有访问同一记录的读写参与者必须使用新验证路径。旧版本 locator cache 和旧写确认不安全，禁止新 GC 与旧参与者混跑，不能直接滚动启用生产。实现仍依赖 provider 对根点查及按位置追加的原子/一致性契约。

根发布后删除前任段。冷读执行 root -> segment -> root，校验根 instance/generation/length。热读使用缓存作为段地址，执行 segment -> root；只有末尾根验证 instance/generation 匹配才能返回值或 tombstone。根校验不缓存，根读取失败时失败关闭；旧段重建或换代会触发重新发现。永久根和严格递增代数保证末尾验证不能误认可退休代。普通追加与初次创建返回成功前校验根仍指向对应代。换段帮助者发现根已推进时不返回猜测的旧 token，读取重试或写入失败关闭；上层处理不确定结果时仍需精确确认。

`CollectRetiredPage(ctx,key,after,limit)` 是显式运维方法：用 `ListPage` 扫描精确分段前缀，limit 为 1..1000，单页候选与删除数量均受限制；S3/COS 每轮发出一个分页 List 请求。无分页能力时返回不支持，不退回全量 List（FS 实现仍可能遍历本地目录）。游标按对象 key 字典序推进，不按 gN 数字排序；完成一轮后必须从空游标开启后续扫描，以清理被延迟请求重新创建在游标后方的垃圾。只删除规范命名且早于扫描前根代数的对象，g00 等别名不删除。扫描后一次根复查即可覆盖整批；根永久保留且代数不能回退，所以后续推进不能把候选变回当前代。中途删除失败返回已删除数和输入游标，允许幂等重试。当前/未来分段、根和相似名称不删除。该方法尚无生产调度器或管理 API 接入。

## 实验结果

- 修复前复现两类失败：旧段重建后缓存读取返回旧权限；暂停的写入恢复后对旧段追加，错误报告成功。修复后两项通过。
- 连续 105 次换代，正常同步回收路径始终只保留根＋当前段两个对象；请求路径没有 List。
- 覆盖读取两次根之间换段并复活旧段、延迟初次 genesis、延迟旧 seal、分批 GC、保护未来代/相似 key、默认模式禁止 GC。
- 并发读取、换段、GC race 测试重复 10 轮通过；有限重试用尽可返回冲突，不能据此授予权限。
- 真实专用 HK COS `cellhive-e2e-hk-1376795072`：原生追加确实能够重建已删除旧段；实验模式拒绝旧代写成功确认、读取返回当前记录、再次 GC 清除旧段；tombstone、重建当前记录、冷读取、旧 token 拒绝均通过。`TestCOSAuthorityCHA3` 全套 7 个子测试通过，共约 10.94 秒，其中新回收用例约 4.70 秒。只使用唯一测试 key，结束后精确清理。并非生产故障注入或完整运行时 E2E。

## 已知成本与未完成边界

小记录 warm Get 已从初版实验的三次降到两次（segment/root），cold Get 仍为三次；默认未启用回收的生产模式仍为一次。单元与真实 COS 均断言操作次数。大记录可能需要额外范围读取；换代时也有额外发现开销。写入仍增加根确认，尚无生产延迟/吞吐验收，不应宣称相对当前生产性能无回退。

延迟 genesis、GC 失败、后台回收槽位繁忙或帮助者先完成发布都可能留下旧段；显式 GC 可再次清理。因此不是任何时刻严格两个对象。根仍按换代追加描述符，16MiB 容量上限未解决；也没有解决删除整个 cell 后永久根的回收。当前是针对历史 gN 的实验，不是“存储完全有界”的最终协议，不具备上线验收结论。

证据：`/tmp/cellhive-reclaim-cos.log`、`/tmp/cellhive-reclaim-stress.log`、`/tmp/cellhive-reclaim-race.log`、`/tmp/cellhive-reclaim-regression.log`。

## 第二轮优化验证

- 热读操作数测试先复现 3 次而非目标 2 次，再修改读取路径通过；新增热读期间换代并重建旧段、根缺失/损坏时拒绝返回缓存值。
- 游标分页覆盖多页（含 g10 与 g2 的字典序）、当前/未来/非规范名称保护、延迟重建后的下一轮回收、删除部分失败后的幂等重试、无分页能力拒绝执行。
- 本机到专用 HK COS，同一稳定记录 10 组交替 cold/warm 测量：首轮 3 GET p50 61.14ms、2 GET p50 41.11ms；仅作此环境小样本，不等价于业务 KV 或生产内网压测。
- 默认行为未启用本实验；未部署、未执行生产对象删除。

第二轮证据：`/tmp/cellhive-reclaim-v2-race.log`、`/tmp/cellhive-reclaim-v2-stress.log`、`/tmp/cellhive-reclaim-v2-cos.log`、`/tmp/cellhive-reclaim-v2-regression.log`。

最终复测：HK COS 全部 7 项通过（8.46s），新增真实分页续扫覆盖 COS 最后一页恰好满页仍标记 truncated 的情况；该轮 3 GET p50=61.29ms、2 GET p50=36.86ms。bucket/lease 全包 race、回收用例 20 轮 race、go vet 通过。

## 本轮 E2E 复测

- 专用 HK COS 存储契约套件 `scripts/e2e/cos-contract.py`：16/16 PASS，无跳过。覆盖原生追加/owner 条件、gN 回收、分页列表、资源 fence 生命周期、64MiB 分页冷恢复、原生 level/epoch 续链恢复、follower 恢复、流式 multipart 快照、wake 旧删除不覆盖重新发布。
- 真实 COS `TestCOSRenewalDelayedCommit`：初次发布/续租 × 是否换代，4 个场景全部通过；该测试使用默认未启用回收的 provider。
- `scripts/e2e/session-recovery.sh`：双进程 E2E PASS（11.69s），80 条已确认值恢复、地址变化、中断恢复重试、空替换盘拒绝、旧 session 封闭；暂停 owner 超过 TTL 后 peer 接管并推进 epoch，旧 owner 恢复后退出，新 session 读取已确认值。
- 运行时双进程测试是默认模式回归，不等价于开启物理回收的完整运行时验收。开启回收的验证来自真实 COS `TestCOSAuthorityCHA3/reclaim-retired`；未新增生产开关、未部署。

本轮证据：`/tmp/cellhive-reclaim-e2e-contract-rerun.log`（16/16）、`/tmp/cellhive-reclaim-e2e-cos-rerun.log`（authority 与续租故障）、`/tmp/cellhive-reclaim-e2e-session-rerun.log`（进程恢复）。
