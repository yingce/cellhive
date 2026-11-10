# Worktree 全量审查与验收（2026-11-10）

状态：三种对象存储的 10 万条强杀/冷恢复长测已通过；COS 并发 immutable create 契约失败，正在复现排查，尚未达到发布验收条件。未提交、未部署。所有 CellHive 改动位于 `/opt/cellhive-worktrees/celld-ltx-migration`。

## 范围与版本

对当前整个未提交批次进行 storage、authority/provider、runtime 三方面独立审查，补充回归后复审。并非只检查本轮新增函数，也不将审查无发现等同于不存在任何缺陷。

证据目录：`/tmp/cellhive-final-acceptance-20261110`。基础验收二进制 `bin/` 包含本轮六项审查修复；`bin-v3/` 另外包含真实 OSS E2E 发现的 R2 metadata 修复与容量预检；`bin-final/` 再包含 R2 正文 CAS 的 provider 边界保护。原生 workerd 固定 `/tmp/cellhive-workerd-lifecycle-20261110`，未重编译 C++。SHA-256 见证据目录 `binary-sha256-final.txt`。Go 使用 Makefile 五项 SQLite tags、GOMAXPROCS=4；Go 包测试串行。

## 本轮修复

1. 普通 cell 超过 1GiB 的全库快照误包含 SQLite locking page，导致 native encoder 拒绝；三种 snapshot mode 共用输入均过滤该页。
2. continuation 替换与 native DO retirement 遗留 WAL cursor；明确关闭旧/失败 cursor，避免依赖 GC 回收已删除 WAL。
3. 分页索引反复缩库/扩库累计收费；删除 block 时释放对应预算。65 次交替提交回归，保留 3 blocks，费用由 1,120,198 降为 63,430 字节。
4. 原生 epoch GC 后，资源 purge 把既有 retired.json 当未知 payload；保留单调退休下界，跨页继续处理；max=1、删除/重建、拒绝迟到旧 epoch 上传回归。
5. worker purge 删除 owner-gen 造成 epoch 重用（此前已有问题）；与 namespace purge 一致保留 own.json/owner-gen，重建 epoch 严格递增。
6. 同 scope transfer 等待不可取消且占据两个全局槽之一；改为可取消 scope gate，取得后才申请全局执行配额。
7. 真实 OSS R2 覆盖写被不支持条件 DELETE 阻断；metadata 使用既有 native append authority 条件逻辑删除。sidecar 名称改为 sha256(key)，避免用户 key 与 .gN 代际对象碰撞；原生 provider 容量在正文变更前预检。COS 同路径，S3 继续使用条件 PUT tombstone。

8. COS/OSS 的 R2 正文 CAS 不再透传到未经验证的普通 S3 条件 PUT，明确返回不支持且不修改正文；普通覆盖写和 metadata CAS 正常。S3/MinIO 条件替换保留。

没有新增 LTX 标记或格式。R2 旧 sidecar 不在线兼容，需空目标或离线迁移；正文与 metadata 的跨对象非原子性仍是既有边界，不能宣称新增完整对象事务语义。

## 修正的测试环境/旧断言

- 安装 CLI 锁定依赖后，18 项 Bun 测试通过；初次缺少依赖失败保留在日志中。
- session 进程测试仍读取旧 owner.json，改为 owner.Key 的 own.json。
- owner lease 冷恢复断言更新为实际原生两 epoch 路径：epoch 目录一次，两层各一次 level 目录与 L0，共 5 次，非宽泛 payload 扫描。
- 64MiB COS 测试的计数 wrapper 遮蔽 streaming 接口且仍直接使用 raw S3；改为生产 COS provider 并透传 streaming。
- Docker 网络恢复测试支持挂载指定当前二进制；测试容器 root 用于读取 worktree 中的受限文件，不修改生产服务。
- RPO 脚本支持指定 E2E_BIN，避免误用旧构建。

## 已取得证据

- 完整 Go：77 包通过，12 个无测试包；2,871 个测试/子测试通过。49 个 opt-in 测试跳过，独立真实云端/进程验收另列，不把 skip 算通过。
- 本地 14 组应用/恢复 E2E 全通过；包含 bindings、D1 id、workflow 与恢复、queue/多节点/并发、service headers、review fixes、SQLite transfer、native DO transfer、fleet 分页接管、KV routing、batch recovery。
- deploy/rollback、MySQL+ProxySQL、owner lease/restart、owner takeover、session recovery 通过。
- RPO=0：SIGKILL + 删除本地库后 1,200 条已确认 KV 全部存在且内容一致，integrity_check=ok。
- DO DNS/网络恢复：旧连接挂起时切换地址约 2 秒恢复，容器 restart=0，使用当前 Go 与既有原生 workerd。
- 10GiB COS 部分读取：逻辑 SQLite 10,751,451,136 字节；快照编码 787,262,031 字节，上传 26.869 秒；L1 晋升 91.204 秒；L9 按默认逻辑大小预算跳过。恢复页索引 0.848 秒，首条完整验证数据 1.332 秒；三条抽样 BLOB 与最新 overlay marker 正确，本地 allocated=3,182,592 字节。索引读取约 26.1MB；这不是 10GiB 所有行的全库哈希验收，也不是 workerd HTTP 延迟指标。
- 单机 SQLite gate：PutTx 20,273 ops/s，batch100 83,877 keys/s。与云存储测试并行、共享主机，不作为前后性能对比。

- 原生 workerd 分页 4 项专项全部通过：fail-stop 与独立恢复、冷读再捕获、多轮 retirement/readmission、跨 lease 读取；256 个 BLOB 全量 SHA256 验证；checkpoint 新增快照 bytes=0，retirement 额外 hydrate bytes=0。
- MinIO 100,000 条 KV、3 次强杀与 owner 切换：在线与清空本地缓存后的冷恢复均 100000/100000，PASS。
- MinIO S3 contracts、R2 metadata 条件删除、immutable wake hint integration 均通过。

- 8 个存储/协议重点包的完整 race 已通过；先前测试重命名过程中并行编译的中间失败已用冻结代码重跑。

## 待汇总

COS、OSS、MinIO 均通过 100,000 KV、3 次强杀/owner 切换、在线与冷恢复全量核验。COS 总耗时 960.70 秒，OSS 1332.76 秒（包含写入、故障、核验与清理）。8 包 race 与最终 R2 边界 race 均通过。

COS 的 16 项扩展契约中 `TestCOSAssetImmutableCreateLive` 失败：8 个并发不同内容创建得到 4 个成功，预期唯一成功；其他 15 项通过。复跑再次失败：8 个不同内容创建中 7 个返回成功且 ETag 各异（cos-contract-recheck.log）。未将该失败归为环境问题或豁免。该路径共用 native immutable create，须排查对 assets 和 LTX 不可变写入契约的影响。

OSS 扩展契约退出 0。最终构建的 COS/OSS review-fixes 已通过，后续 queue/multinode suite 也已通过。完整退出码和复跑对应关系待汇总；当前不能声称全量通过。

### COS 并发创建 HTTP 取证

`cos-create-wire-repeat.log` 连续 10 次测试中 8 次失败、2 次通过，失败轮次 2–6 个成功者。签名传输层确认实际 PUT 携带 `x-cos-forbid-overwrite: true`，不同正文分别收到真实 HTTP 200 和不同 ETag；409 也正确映射为 ErrPrecondition，排除 SDK 把已观察到的 409 当成功。顺序重复创建被拒绝，但该桶并发唯一创建不成立。COS 内部实现原因仍未知，不能把“内部先查后写”作为已证实结论。当前适配器将防覆盖等同原子唯一创建的假设不成立，生产代码尚未修复。
