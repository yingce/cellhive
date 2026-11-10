# COS assets 并发创建修复（2026-11-10）

仅修改 `/opt/cellhive-worktrees/celld-ltx-migration`，未提交、未部署。证据目录 `/tmp/cellhive-assets-fix-20261110`。

## 原因与修复

原路径把普通 PUT 的 `x-cos-forbid-overwrite: true` 当作原子 create-if-absent。HTTP 证据确认 header 已发送，但 COS 对多个不同正文返回 200；修复前 10 轮有 7 轮失败，最多 8 个写入者全部成功。SDK 没有把这些 200 映射成错误，不能用顺序重复创建成功阻止覆盖来推断并发安全，也不能通过写后读取补足原子性。

COS assets 现在在原 key 上使用 native `AppendObject(position=0)`，数据前置 16 字节（`CHASSET1` + 8 字节大端正文长度）。空文件同样占用非零长度，竞争失败映射为 `ErrPrecondition`。不增加协调对象；LTX/owner、OSS/S3 格式和写路径不变。

读取返回逻辑正文：Get/Stat/range/stream/tail 校验头部与物理长度，拒绝截断或旧 raw 文件；跨 HEAD/range 请求校验 ETag，避免删除重建时混读。列表保留所有 key 以便 purge 清理中断对象，仅扣除 framing 开销，不引入逐对象网络扫描。普通 Put/CAS 不允许绕过不可变创建。

COS assets 不能直接预签名返回带头部的物理对象，所以返回 `ErrNotSupported`，runtime 使用既有 authenticated agent 点读回退。agent 返回显式 Content-Length，保留 runtime 既有小文件缓存；相关回归先复现缺失长度再通过。这增加了 assets 首次读取的 agent 依赖；本轮验证功能正确，没有声称此路径性能提升。旧 raw COS assets 必须迁移到新空桶/新 namespace 重传，或停服清理旧 assets 后重传；仅重复发布同内容版本会复用旧 manifest token，并不能转换格式。不能与新读取器混跑。该变更不涉及 SQLite 数据迁移，也不改变 celld LTX 格式。

## 验证

- 修复前：真实 COS 10 轮竞争，7 轮失败，见 `before.log`；单元协议断言也在修复前失败，见 `unit-before.log`。
- 修复后：真实 COS 连续 10 轮通过，每轮包含非空和空文件各 8 写入者竞争、唯一赢家内容检查、Stat/range/stream/tail/list-size、禁止 PUT/CAS/裸 presign；见 `after.log`。
- 真实 COS 64MiB 上传上限、完整读回和逻辑读接口通过，见 `large-asset.log`。
- 真实 COS CLI 部署当前 assets 示例，公开 HTML/二进制/空文件逐字节一致；相同 token/path/content 重试成功，不同内容拒绝；应用清理 HTTP 200，见 `assets-e2e-cache-final.log`。
- 截断/旧文件拒绝、列表保留中断对象供 purge 清理、64MiB 编解码边界覆盖于单元测试。

最终全量 Go 77 包通过、vet 通过（退出码 0），见 `go-all-final.log`、`vet-final.log`；缓存长度回归 race 通过，见 `cache-race-final.log`。最终真实 COS 发布与应用清理通过，见 `assets-e2e-cache-final.log`。shell/Python 语法及 `git diff --check` 通过。未重编译 workerd，使用 `/tmp/cellhive-workerd-lifecycle-20261110`。

最后列表清理调整后的真实 COS 竞争复验 3/3 通过，见 `native-final.log`；最终 bucket/artifacts race 通过，见 `race-final.log`。

## 文档清理后再次复验

用户要求重跑此前失败项，本轮未改生产代码。证据目录 `/tmp/cellhive-assets-recheck-20261110`：

- `TestCOSAssetImmutableCreateLive` 非缓存 10/10 轮通过（每轮非空、空文件各 8 并发写入者）；见 `after.log`、`concurrency-summary.log`。
- `TestCOSAssetUploadLimitLive` 64MiB 上传及读回通过，3.31 秒；见 `large-asset.log`。
- 真实 COS assets 部署/公开 HTML、二进制、空文件逐字节校验、同内容重试及不同内容冲突全部通过；测试 namespace 清理 HTTP 200，runner 退出 0。见 `assets-e2e.log`。

本轮为原失败项及相关发布路径复验，不表示再次执行了全部可选 E2E 组合。未提交、未部署。
