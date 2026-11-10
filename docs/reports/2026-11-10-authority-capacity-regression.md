# 2026-11-10 权威存储容量与全量回归

工作目录 `/opt/cellhive-worktrees/celld-ltx-migration`。共享权威存储默认单段由8MiB降为1MiB，根由128MiB降为16MiB。未提交、未部署。真实COS桶为 `cellhive-e2e-hk-1376795072`；使用现成 native workerd，CPU限制0–3。

本轮发现并修复：S3 ConditionalDelete 直接透传 COS NoSuchKey，使并发清理唤醒对象时 Workflow 返回500。现在映射为 bucket.ErrNotFound；412仍为条件冲突，403等错误不吞掉。新增404/412/403回归，确认修改前失败；真实COS再次删除已不存在对象的用例通过。另同步5个失效JS测试夹具，生产JS未改。

修复版：全量带SQLite tags Go测试77包、2830测试/子测试通过；39条件跳过另记，部分已由独立COS/native测试补跑。全量vet、全部命令构建、bucket/wake/workflow及server Workflow/Wake race通过。JS148/148、临时CLI副本锁定依赖测试18/18通过。

修复版真实COS已通过Workflow丢响应强杀+空目录恢复、16并发事件消费、路由/鉴权/删除重建、导入导出/旧SQLite迁移、review/queue/multinode、100条批量强杀和冷恢复（100/100，首次冷读2.102272秒）。原生DO fleet最终复验通过：A→B→A，epoch1→2→3，确认值10→11保留，接管31/33秒含租约等待。

容量调整版（独立404修复前）的10万写入压力：3次SIGKILL，在线100000/100000、冷恢复100000/100000一致，首次冷读2.624452秒。写入含接管间隙814.6秒。双节点fleet性能36组热请求加冷探测153995次、0失败；这不是与旧版本交替对照，不宣称性能提升。原生DO fleet A→B→A、epoch1→2→3已通过，接管33/34秒含租约等待。

按用户当前范围未重跑10GiB和100 epoch专项；外部Wrangler、其他provider等条件测试不宣称通过。旧对象超过新上限会失败关闭，测试结果不替代生产对象容量核对。

完整日志、跳过清单、原始失败、修复前后源码/二进制SHA256和矩阵位于 `/tmp/cellhive-authority-small-acceptance`，汇总见该目录 `report.md`，修复版矩阵见 `fixed-e2e-results.json`。

最终DO fleet首次修复版运行因HTTP ready先于节点发现，短工作负载全部回退COS，未满足fleet模式断言。已在测试脚本增加有界fleet ACK就绪探测，然后重新设置DO计数基线；重跑DO工作负载增加12次fleet提交且完整恢复通过。未放宽断言或修改生产逻辑。
