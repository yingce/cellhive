# 真实 HK COS 双 agent 热写长尾 trace（2026-10-27）

## 结论

已复现835ms热PUT，主要阻塞在非owner入口的资源fence对象读取。不是该次业务写等待COS落桶：同trace owner HTTP仅5.515ms，durability proof 5.065ms；正式窗口fleet提交842批、bucket-batch=0。不能据此解释此前没有trace的686ms样本，也不能声称所有长尾已定位。

## 运行与证据

代码3e448de，工作区 `/opt/cellhive-worktrees/data-path-optimization`。本机双agent/真实HTTP keepalive/1KiB/32keys/单KV cell，HK专用COS生产provider；两轮c1/c8/c32，owner、follower、Worker GET/PUT。命令：

```sh
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance-dual --port-base 29500 --trace-endpoint http://127.0.0.1:28480
```

153960正式请求、0失败、exit0，测试应用DELETE200；Wrangler CLI附加测试未配置而跳过。完整OTLP从测试启动前开始采集，结束后等待导出并停止collector。原始trace约587MiB，保存在 `/tmp/cellhive-data-path-trace-20261027.jsonl`，不加入git；精选trace、完整运行输出和摘要在同名目录。运行目录 `/opt/.hermes/cache/scratch/cellhive-e2e.TG6FxH`。

二进制SHA256：cell-agent `5a352dc20099394ecf04cd164602e8ef5b1df826c6c8f97389a2ec96f2ca4880`，user-runtime `e4b51e2656da3f29f85872ba0d8335530681224e76e30602e009ffdc0b4a50a5`，workerd fork `63009b9b507a7b4fefb77de9ecb4ef87252476783da96054502965fbb984760c`。

## 明确定位的转发PUT

| trace | HTTP总ms | binding刷新ms | fence读取ms | owner HTTP ms |
|---|---:|---:|---:|---:|
|12f7e529c4eded2e9e423d25b35368b8|834.702|33.502|793.540|5.515|
|98dfe27818a75992ec3b141bc1edf892|582.502|33.371|540.846|5.891|
|6138252b153a54057e2f58b4c70cc00a|286.252|41.304|229.487|8.781|
|835aea69c610b09c44c63f9a3352e972|277.299|42.079|220.860|7.561|
|e64469338bbb9be41807200e82cafdbb|201.509|37.990|140.645|16.979|

这些是同一客户端trace中的入口与owner spans；转发保留原traceparent，两个HTTP server span是兄弟关系，不假称严格父子span。资源prepare/fence与binding刷新则是入口HTTP的子树。最大样本约95%在fence读取。

## 源码与并发证据

- `internal/server/scope.go` 的scopeAuth在业务handler前调用beginBindingAccess。
- `internal/server/scope_access.go:21` 先prepareResourceAccessSnapshot，因此follower也在转发前等待资源fence。
- `internal/replica/resource_fence.go:41` 缓存1秒；readResourceFence在释放锁后直接readResourceFenceFresh，没有同key singleflight。后者调用objectstore.GetVersion（最终Bucket.Get）。本轮是已初始化资源，不是首次legacy迁移。
- 第1轮c8转发PUT的8个请求在06:19:54.290Z约0.3ms内同时进入各自fence读取；6次耗时13–21ms，另两次541/794ms，证实缓存过期并发查询放大。span包含provider与网络等待，未进一步区分TCP/TLS、COS服务时间及SDK重试，不把794ms全归为COS服务端处理。
- `internal/server/kv_access_warm.go` 普通KV入口2秒空闲即移出热集合，第二次近期访问才激活，首轮预热再等500ms；后台每次成功后500ms刷新，任务超时1秒。分批入口切换期间某个入口数秒无请求会退出预热，下一批前台必须同步校验。资源数据虽仍驻留，入口元数据缓存可能已冷。

## 其他长尾与测量限制

>=50ms正式请求1376次：forwarded PUT82、Worker PUT41、Worker GET1253；direct PUT/GET及forwarded GET未记录>=50ms。全量trace产生大量span，且中途离线解析与压测短暂重叠，不能把本轮RPS/p99当无观测性能基线。

Worker GET最大159.55ms（trace 1dabf945336bc5d56249379d7f9d9ba5），只有135ms的JS报告HTTP span，未关联到完整agent子请求。现有并发trace传播/报告仍存在关联缺口，不能把Worker剩余延迟套用fence结论。正式请求都成功不等于trace完整率100%。

冷启动743ms、peer发现阶段159ms独立于上述正式热样本，不混入根因表。collector已停止，未变更生产配置、未部署。

## 建议修复顺序

1. 先为同scope的普通fence缓存未命中/后台刷新合并查询，保留删除rev与失败不续期规则，独立覆盖取消、删除重建、并发旧结果回写。它能减少请求放大，但不能消除共同等待的一次慢COS读取。
2. 评估非owner只做token/路由校验后由owner执行资源generation准入的路径，避免follower重复fence读与本地SQLite gate。必须先证明旧token、删除同名重建、owner切换的拒绝行为等价，不能直接跳过鉴权或放宽fence TTL。
3. 对间歇访问调整有界预热策略，作为缓存命中优化，不承诺解决COS慢于有效期的安全等待。
4. 修补Worker并发trace关联，再单独定位其GET/PUT长尾。新路径用真实COS删除/重建、接管和双节点性能回归验收。

本次仅诊断与证据归档，没有实现上述修复。
