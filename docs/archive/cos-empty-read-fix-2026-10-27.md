# COS append首次创建空读修复（2026-10-27，本地未发布）

## 根因证据

HTTP transport诊断只记录方法、position、响应码、Content-Length、对象类型、request-id及相对时间，不记录请求签名、凭据或正文。修复前100轮真实HK COS并发创建，97通过/3失败；三次均捕获200/length=0/type=appendable。

代表失败时序：

- 第一个GET在11.299ms返回404。
- 随后发起POST Append(position=0)，40.340ms返回成功，next-position=14。
- 与它并发的第二个GET在37.558ms返回200、length=0、type=appendable。
- 后续GET返回14字节完整记录。

这确认COS在首次Append仍在途时可暴露零长度appendable对象；旧AppendAuthority把该成功空读交给decodeFrames，立即返回empty object，而不是竞争创建应有的ErrPrecondition。这不是SQLite、恢复合批或MinIO问题。

## 修复边界

AppendAuthority的Get/ConditionalCreate共用readRecord：仅对成功的零字节读取最多观察5次，退避5/10/20/40ms。CAS与ConditionalDelete通过Get复用。非空读取与其他错误仍立即进入原逻辑；非空截断、CRC错误、非法格式不重试。持续为空保留明确错误；context取消终止等待。75ms是额外退避上限，不含网络耗时。

不重试任何Append，不把空对象当作NotFound，不生成空对象token，不改变位置条件写、删除栅栏、epoch或确认条件。正常非空/404读不增加网络调用。OSS共用AppendAuthority，也只有非法空记录会触发此观察；本次真实云验收使用COS。

## 验证

- 新回归修复前失败：瞬时空后已有winner、Get空后有效记录、取消时应返回context错误。
- 修复后覆盖：持续空Get/Create/CAS/Delete失败且零写入；stale token经空读后仍拒绝；当前token只写一次；空读后CRC损坏立即拒绝；非空损坏不重试。
- 真实COS修复后300轮，**300通过/0失败**。日志仍捕获**10次200空响应**，均重新观察后正确处理，证明并非本轮未碰到竞态。每轮验证唯一winner、winner值、CAS、拒绝旧token删除、允许当前删除、删除后再创建。
- 完整COS存储契约**6/6通过，无skip**。
- make build/test/vet全部退出0；bucket/owner/lease/recovery race三轮通过。
- 真实HK COS双agent：强杀时16项spool，接管后100/100正确；survivor drain后删除SQLite缓存、重启，再次100/100正确，首个冷GET1288.762ms，app清理HTTP200。测试二进制SHA256 `3989de6bb3c047ed29d2e00e0ae17cd63074575ab40c38f55ba7ec57f11ad53d`。

重复命令（新入口正确核对每个指定用例的通过次数）：

```bash
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3 --native-repeat 300 --append-diag
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3
```

本轮300次运行使用临时包装脚本调用相同Go用例和配置；after300.log的包装末行显示1/1是按测试名汇总，实际300个PASS与10次空响应以逐轮日志为准。仓库入口已改为核对重复次数。native append测试保留唯一itest测试键作为诊断证据，不删除共享命名空间。未提交、未推送、未部署；不声称COS冷延迟门槛已达成。
