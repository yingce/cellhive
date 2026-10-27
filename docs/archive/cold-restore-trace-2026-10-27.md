# 冷恢复 trace（2026-10-27，本地未发布）

## 方法与验收

本机双agent、HK真实COS，`hk-review.py --suite batch-recovery --trace-endpoint http://127.0.0.1:28480 --port-base 28600`。100个不同key经fleet确认后强杀owner，survivor接管逐个校验；survivor优雅退出并移除整个测试cells目录，再启动，ready后发送带唯一traceparent的冷GET。两轮接管/冷恢复均100/100正确，测试app删除HTTP200。第1轮强杀前11项spool，第2轮7项。

只补充观测，没有改变恢复算法、并发度或持久化语义。全量采样有额外开销，两轮COS延迟波动明显，不能以跨轮耗时判断回归或优化效果。此次同时清空了控制cell与KV，不等价于仅一个KV被淘汰且控制cell仍热的场景。

验证：带项目SQLite tags的replica/cellstore/server/cell-agent测试及vet通过；replica/cellstore race通过。冷GET trace id打印在用例日志；selected-spans.jsonl保留请求、目标KV恢复及第2轮控制cell恢复的span。临时collector未正确保留bool属性，选取记录中已删除其零占位，分析只使用scope/时序/整数计数。

## 第2轮：2948.081ms（HTTP span 2947.661ms）

请求trace `4a2b5dfb98dfdda8a4a3a230c9305978`。

| 请求阶段 | 耗时 | 证据 |
|---|---:|---|
| binding_lookup/refresh | 1964.010ms | 内含1603.109ms等待 `__platform__/__control__/main` 已在进行的打开 |
| 资源fence prepare | 16.503ms | fence点读16.407ms |
| 等待KV冷open | 685.161ms | `cell.wait_existing_open`，对应目标scope |
| 其他请求耗时 | 约282ms | owner查询/claim等，部分与KV后台hydrate重叠，未细分所有步骤 |

控制cell恢复由后台 `cell.register_pending_timers` 启动，trace `b209443f4c585c5f93b081b38e42e3be`：cold open1649.487ms，manifest读取772.285ms、链对象列表230.783ms、4对象下载430.461ms、SQLite应用3.781ms。该控制cell是共用HK测试桶的历史控制状态，不能外推为生产每个namespace都有同等控制恢复成本。

KV恢复同样由后台timer注册先启动，trace `064b6216c8dbe86ed2fb73a0a86de331`。请求在它开始后约27ms进入等待，等待终点与open完成对应。两条trace是独立后台/前台上下文，以scope、等待span和时间区间关联，不伪造父子关系。

| KV恢复阶段 | 耗时 |
|---|---:|
| 分页恢复探测（最终回退完整恢复） | 36.201ms |
| 完整恢复再次发现最新epoch | 17.130ms |
| List恢复链（121个key） | 23.428ms |
| resource fence | 0.001ms（缓存命中） |
| manifest | 16.578ms |
| 下载119个对象，默认4并发 | **582.686ms** |
| 解码/过滤/去重 | 30.198ms |
| 链校验 | 0.042ms |
| SQLite文件应用 | **4.701ms** |
| 打开SQLite | **0.596ms** |
| 完整cold open | **711.815ms** |

解码206个逻辑段，**93段重复**，最终113个逻辑段。对应写阶段owner用6批上传93段；接管恢复又逐段上传，产生重复逻辑内容。恢复使用Header.ID去重，正确性通过，但去重发生在下载之后，无法省掉这些GET。

## 第1轮：2778.209ms

请求trace `648671463133846dfa36296662340eab`；binding查询650.331ms。KV恢复trace `a077670145c5447d777d07257c669e73`，cold open1663.201ms；下载120对象共264203字节耗1532.812ms，单对象p50 15.865ms、最大1008.936ms；SQLite应用6.164ms、打开0.563ms。第1轮尚未补前台open等待span/重复计数；第2轮补全这些观测。第1轮冷重启二进制已含list_chain观测，但此前takeover过程使用该观测补全前的版本，takeover trace不作为列表计时证据。

## 根因与优化顺序

1. **接管补传没有沿用合批**：`internal/recovery/recovery.go` 的恢复路径对fragment逐个调用Replica.Append；owner已上传batch的内容会以单段对象再次出现。`restoreOnceKeys`下载后才按Header.ID去重。应优先设计有界、同scope/epoch的恢复合批，确认整批持久化后才推进recovery progress；不可直接跳过未证明已持久化的数据。
2. **冷鉴权还依赖冷控制cell**：清空全部SQLite后首个binding查询会等待控制cell恢复；第2轮这个等待比目标KV恢复更长。应单独测控制cell预热/ready策略及元数据点查，不能把全部2.95s算成KV下载。
3. **分页探测与完整恢复重复查epoch**：第2轮两次分别18.423/17.130ms；第1轮takeover阶段曾543/219ms。可评估同次恢复共享已验证元数据，保持epoch/fence正确性。
4. **COS尾延迟仍存在**：第1轮单对象GET约1s，第2轮控制manifest约772ms；增加下载并发不能消除串行元数据查询和冷控制依赖。

本次没有实施上述性能修复；不把trace完成视为冷恢复SLO达标。未提交、未部署。
