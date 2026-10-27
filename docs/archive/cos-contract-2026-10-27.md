# HK COS 存储契约补验（2026-10-27）

用户要求使用COS、不依赖MinIO。此次在专用 `vwork-e2e-hk-1376795072` 桶验证，无生产桶操作、无Docker/MinIO。测试凭据从指定文件读取，未写入代码或日志。新增可复用入口：

```bash
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3
```

运行器要求6个指定用例逐一PASS；退出0但缺失/skip用例也判失败。与其他HK harness串行运行。没有重新执行完整41项E2E。

## 生产COS路径

- `TestNativeAppendCloud`：原生定点追加及冲突行为。
- `TestProviderCloudDiagnose`：生产authority create/reject-create/CAS/reject-stale/range/条件删除，以及不可变LTX拒绝覆盖（含legacy对象）。
- `TestCOSConditionalDelete`：authority及普通数据对象拒绝旧etag删除，允许当前etag删除。
- `TestCOSDataContract`：普通GET、范围读取、真实预签名URL HTTP GET、每页2项的游标列表及size/etag。
- `TestS3ReplicationRestoreChain`：通过生产COS provider执行snapshot→replica→完整恢复（500行）→compaction→按页Materialize（500行）。测试原先硬编码MinIO凭据/桶，已支持外部配置、唯一scope和清理。
- `TestCOSLargePagedRestore`：64MiB SQLite、按需查询1MiB blob；约1.01MiB范围传输/5次range，验证数据面分页恢复。

第一轮6/6通过；最终入口复测5/6通过、1失败，入口正确返回非0，详见同名目录final.log。失败为并发创建中的 `append authority: empty object`，并非MinIO环境问题。随后单独20次并发创建用例均通过；追加100轮又复现1次同样错误（99通过/1失败），见native-repeat100.log。因此这是已重复观察到的异常，不能覆盖或抹掉失败。

测试仍检查恰好一个winner、读取值匹配、CAS、拒绝旧token删除、当前token删除及重新创建；本次未报告双winner或确认数据丢失。代码在读到空对象时拒绝继续，影响表现为创建错误。是否是COS首次Append期间可见空对象，需要进一步定向证据；目前只是解释假设，不作为已确认底层原因。

bucket/replica普通回归结果见regression.log。只修改测试入口与测试，不改变生产存储协议，尚未提交/部署。

## 原生S3能力边界（保留失败）

直接把严格S3条件写测试连接到COS时，`TestS3BucketIntegration` 在重复ConditionalCreate处失败：`reject-create err = <nil>, want ErrPrecondition`。证明本次COS原生S3接口没有满足该条件写契约，不能声称严格S3套件通过。失败原始日志 `native-s3-boundary.log` 保留。

生产 `cos://` 使用既有ProviderBucket/native append承担authority和不可变对象，已在上述测试验证；这不是本次为了让测试通过而放宽条件写。原生S3测试断言保留，其MinIO环境验证仍未执行，但不再作为此次COS部署的外部镜像阻塞。

发布判断修正：COS存储契约已实际补测，但不是全部稳定通过。并发创建偶发空对象错误仍需排查，不能仅凭后续重跑成功宣布解除发布顾虑。此前“仅剩冷恢复性能”的判断已被本轮新证据修正；生产协议未修改，未部署。


后续修复：HTTP诊断已确认首次Append在途时的200空对象可见窗口；现有界重读修复已通过300轮真实COS（其中10次空响应），完整COS契约6/6及双节点故障恢复通过。此前发布阻塞判断对应修复前状态，最新见 [空读修复验收](cos-empty-read-fix-2026-10-27.md)。原始失败证据保留。
