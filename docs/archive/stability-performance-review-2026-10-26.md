# 稳定性与性能复查（2026-10-26）

范围：CellHive `cdaa6fd` 的 peer transport、提交/上传、DO readiness、Helm入口；对照 `/opt/celld` `90b4301`（v0.6.2）。这是重点路径review，不代表全仓无其他问题。初始review未修改生产逻辑；后续经用户确认实施，见文末修复记录。未部署。

## P1：peer握手及写入不能被context取消

`internal/peer/stream.go:172–177` 仅TCP Dial使用context，之后req.Write/http.ReadResponse未设置deadline，也未通过取消关闭连接。`submit:246–250`的互斥锁等待和Write/Flush同样不受context约束。

隔离故障测试：TCP接受但不返回HTTP101，context为50ms，250ms仍未退出；net.Pipe对端不读取，Write同样超过context仍阻塞，直到主动关闭测试连接才退出。测试命令 `go test ./internal/peer -run TestReview -v -count=1` 退出0，表示成功断言缺陷存在，不表示缺陷已修复。

影响：已有上层超时不能保证同步dispatch退出；复制延迟、关闭流程可能被半失效peer拖住。建议连接建立、握手、写入均有明确取消边界，写入结果不确定时保留RPO=0桶证明，不得盲目重放写操作。

## P1：单个peer握手持有全局连接锁

`stream.go:375–387` 在`t.mu`内执行网络握手；`Close`也获取同一把锁。隔离测试先阻塞一个目标的握手，再访问无关目标，无关目标在250ms内也不能返回，直到释放第一个连接。即使后续补超时，握手期间仍会阻塞其他lane/peer，故需单独解决锁粒度。建议全局锁只维护map，每key独立协调建立连接，IO在全局锁外执行。

celld的`main/peer_tunnel.rs:340–374`只在checkout/park时持池锁，establish在外部async进行；可参考这一锁边界，但它是请求转发隧道，不等同于日志复制协议。

## P1：异步ACK取消后仍占用流水线槽位

`stream.go:288–300`等待ACK/closed时没有ctx.Done；对端读完frame但不ACK时，请求取消不会终止后台等待，也不会使stream失效。隔离测试50ms context，250ms后future仍未返回、slots=1，关闭stream才释放。一个lane累积到pipeline容量后，后续发送只能等待或超时；当前stream仍会被缓存选中。

证据 `/tmp/cellhive-review-20261026/peer-async-test.go` 和 `peer-async.log`。建议将请求取消和连接健康超时分开：流水线共享连接不能因任意一次正常取消就随意打断其他成功请求；但必须有ACK停滞上限、退役与所有pending的清算，防止永久占槽。

celld `logic/log_evict.rs`默认1500ms backstop；`node_log.rs:2837`普通append使用backstop+100ms请求上限，流模式跟踪outstanding并退役连接、失败所有未完成帧。不能简单照搬1500ms到CellHive，也不能据此宣称celld所有握手都有总时限：其open_stream只配置5s connect_timeout，lane的dial处也需结合外层生命周期判断。

## P2：fleet成功路径仍可能同步等待COS（已知性能边界）

`server.go:3513–3517`拿到peer ack后同步调用`Uploader.Enqueue(context.Background())`。`upload.go:447–484`先写本地spool；内存队列满则直接调用对象存储AppendBatch。因此后台上传积压时，fleet成功请求仍可等待COS且脱离原HTTP取消。这不是本轮证明的USW DO故障原因，也不证明前次SG正常热测已有队列满。

建议先计量已有slow upload enqueue日志中的synchronous/elapsed与spool、队列状态，再考虑以耐久spool为接受边界、后台有界排空；spool失败/磁盘满必须继续背压或失败，不能改成无界goroutine或无持久依据的提前成功。

## USW DO未自动恢复：根因仍未确认

发布证据是：DO /ready持续503 cell_unreachable，同容器curl访问cell-agent为200；agent就绪后重建DO恢复。host.js:103–106把HTTP非2xx与DNS、连接、超时异常全部归为同一reason，并未保留具体异常，现有证据不足以确定是哪一类。

隔离测试使用线上同一镜像（digest 9e981187…），复制ready中的800ms AbortController模式：后端普通重启、换到不同IP（192.168.80.2→192.168.80.20）、DNS先失败后恢复，均能恢复200。此前同IP重启只作为连接测试，不作为DNS换址证据。不能声称这些实验复现了USW故障，也不能据此排除实际host actor负载、事件循环或集群网络差异。

源码中实际使用NetworkService→KJ NetworkHttpClient；不能拿仅供external service使用的PromisedNetworkAddress TODO推断所有fetch都永久缓存DNS。当前依赖实现会在连接池drained或初次解析失败后清理host entry。

Helm另有确认的可用性风险：cell-agent Service同时承担StatefulSet发现和runtime业务入口，clusterIP=None且publishNotReadyAddresses=true；业务DNS可包含尚未就绪的agent。建议保留现有headless用于成员身份，另加只包含ready endpoints的ClusterIP业务入口。这能减少滚动窗口错误，但不是本次持续故障的已证实根治。

下一步：给readiness失败增加限频、分类诊断（HTTP状态、abort、底层异常及耗时，不含凭据），在隔离环境运行实际DO host并覆盖双agent同时换地址/短暂DNS故障/连接黑洞/DO活动负载，要求无需重启DO即恢复。

## 验证与清理

四项peer缺陷断言测试均按预期观察到缺陷；没有修复后通过的声明。原始日志位于`/tmp/cellhive-review-20261026/`。临时测试Go文件已从源码移除；隔离Docker容器及network已清理，未操作线上业务进程。本轮仅留下review文档及known-issues入口，未提交。


## 用户确认后的实现与验收（ADR-296）

- `internal/peer/stream.go`：握手整个阶段绑定5秒context/deadline及取消关闭；写入gate可取消，已进入写入后取消/失败退役连接。pending顺序在写入gate内登记，与wire顺序一致，防止并发请求错配ACK。
- 全局map锁不做网络I/O，同target/lane合并建连。并发等待者共享一次建连失败，避免N个等待者串行经历N次握手超时；Close取消在途握手并拒绝新stream。
- 每帧进入发送阶段起5秒无ACK则退役lane，失败全部pending并释放槽位；正常调用取消仅让该调用返回，pending继续等真实ACK或健康上限，避免误伤其他调用；空闲连接不超时。失败连接下一次可重建；ACK不确定的写入不盲目重放。
- `workerd/do-runtime/host.js`：保留原reason，新增HTTP/timeout/network、耗时、上游状态、受限错误名称/内部引用；失败日志每30秒限频、恢复记录一次。明确取消响应体；不记录后端body、URL、token或原始错误文本。
- 尚未取消上传队列满的同步安全背压，也未变更headless业务入口或patch workerd fork。5秒是故障上限，不是正常请求固定等待；极慢但最终成功的peer也可能被退役，调用继续需要既有桶证明。

回归先红：旧代码4项测试分别确认握手、写入、其他目标/Close、异步等待取消失效；新增并发建连共享失败测试在初版也失败，补全后转绿。DO诊断测试在旧代码因缺少failure_kind失败，新增字段后通过。

验证命令/证据：

- `go test -race ./internal/peer -run TestStream -count=10`：9项测试各10轮通过（最终5.970s）；涵盖ACK停滞退役、取消不伤其他ACK、空闲不误关、故障重连、100并发ACK正确对应、共享握手失败。日志 `/tmp/peer-recovery-final-race.log`。
- `TestPeerStreamStallBucketProofAndRecoveryHTTP`：真实HTTP owner→故障peer，握手不响应约5秒后返回bucket-batch，先核验桶中含完整原始LTX段，再恢复peer，下一写为fleet并从follower活动session读取已持久化帧；与既有同步/异步stream测试一并通过race（6.118s）。日志 `/tmp/peer-recovery-http.log`。
- 固定本地workerd产物启动实际DO host：HTTP503→超时→恢复、连接拒绝→同地址重新监听→恢复，均不重启workerd；检查分类字段及HTTP状态，既有ready/drain测试通过。日志 `/tmp/do-ready-green.log`。这里的后端是HTTP故障stub，不把它称为USW真实集群故障复现。
- 完整 `make test`、`make vet`，及真实双进程 `scripts/e2e/session-recovery.sh` 的最终结果保存在 `/tmp/stability-fix-{test,vet,session-e2e}-final.log`。session场景包括两地址变化、恢复中断重启、空替换盘拒绝、原follower盘恢复、80个确认值、旧控制注册、旧session封口，以及SIGSTOP跨TTL后旧owner被fence。

上述为本地验证，不包含线上发布或SG/USW吞吐复测；不宣称已根治USW历史DO未自动恢复问题。源码、测试和文档均保留未提交，待下一步发布指令。


## 后续DO定位进展

ADR-297已用真正DO host复现：无超时的后台日志配置拉取挂在旧地址，使共享HTTP客户端不能drain，DNS改到新后端后ready仍503；仅readiness诊断改动不能解决。给该拉取增加完整2秒预算后，不重启DO约2秒恢复，user-runtime同类路径也修复。详见 [对照实验及边界](do-address-recovery-2026-10-26.md)。不把该机制推断为缺少历史请求证据的USW事件唯一根因。
