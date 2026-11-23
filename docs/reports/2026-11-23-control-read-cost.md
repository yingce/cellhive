# Queue 发现与控制面读取请求优化

## 问题与改动

SG 两台 agent 的 172 秒采样共记录 15099 次 GET 类操作（包含 Range GET），同期恢复文件和按需缺页计数没有增长。发现 Queue 每秒重新获取资源列表、本地控制面查询重复前后 authority 校验；没有逐调用来源指标，不能将全部线上请求归因于这两处。

- Queue 轮询使用独立的资源发现缓存，TTL 最多 5 秒，从刷新开始计时；空列表也是有效结果。并发刷新合并，过期失败不回退旧视图，调用者取消不返回缓存。返回切片独立，调用者不能修改缓存。
- 资源发现响应携带该次成功查询绑定的 authority identity；观察到其他控制读取报告换主或失败后，下次缓存读取失效。未知的远端配置变化最多等待剩余 TTL，不把缓存当作资源授权依据。
- 本地控制读取复用外层刚完成的 owner 点查作为起始样本；内层保留 ReadFence、ACK、node/session/epoch 校验及结束点查。远端调用仍保留调用端与接收端的完整前后校验。
- Queue 的 storage ID 映射、资源删除门禁、owner gate、dispatch 校验、capture 持久化路径未改变；严格配置读取和清理 authority 校验不走发现缓存。

## 验证

针对性测试及 race 检查通过：缓存过期/读取失败/恢复、并发刷新、调用取消、删除后空映射、同名重建使用新 storage ID、已观察的 authority 变化、慢请求 TTL、结果身份绑定、切片隔离，以及本地和跨节点换主拒绝。

计数测试：30 次间隔一秒的轮询调用资源发现源 6 次；无缓存时需 30 次。本地成功读取需要两个 owner GET（开始和结束），原调用链需要四个；跨节点仍为调用者两个、接收者两个。这些是逻辑调用计数，不是线上 COS 账单或最终总降幅。

全仓 make test、make build、make vet 均退出 0（未变更包可能复用 Go 测试缓存）；针对性 race 检查退出 0，git diff --check 通过。初次直接 go test 因缺少项目要求的 SQLite tags 被编译门禁拒绝，补齐 Makefile tags 与 FILESTAT CFLAGS 后针对性检查通过。

## 发布边界

本批仅修改工作区，未提交、未部署；SG/USW 镜像与线上请求率没有因本批修改而变化。后续部署需验证真实 Queue 收发/ACK/重试/DLQ，并在相同业务负载下采样 COS 请求。证据日志位于 /var/tmp/cellhive-control-read-*.log；线上基线位于 /var/tmp/cellhive-sg-cos-read-20261123/。
