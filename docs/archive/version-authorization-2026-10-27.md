# 固定版本授权后台续期（2026-10-27）

## 变更

固定 KV token 改为独立检查指定版本 binding 的名称/资源匹配、app/worker 存活以及资源撤销状态。SQL 单条快照读取版本 JSON 和撤销状态，兼容没有 normalized bindings 的旧版本。当前 active 映射不参与该版本的授权；旧版本在新版本不再声明该 binding 时仍可使用，直到资源/worker/app 被撤销。普通 HTTP 旧 token 语义不变。

授权缓存以 namespace/worker/version/name/resource/token 身份区分，复用原 singleflight 和 revision 失效保护。有效期仍为 BindingCacheTTL（默认1s），从查询开始而非完成时计时；过期/失效结果不可用于放行。后台续期失败不能延长旧结果。固定版本在第一次成功请求后即登记预热，空闲60s后移除，已有共享集合最多64项、最多8个刷新任务；满额回退到同步校验，不无限建 goroutine。刷新间隔为 TTL/3、最多500ms，调度 tick100ms。容量/延迟可能造成过期，因此不是无条件无等待保证。

资源 fence、owner/liveness、持久化不变；没有引入撤销推送或新桶前缀。后台仍会查询 control owner 与 COS。非固定 token 仍走旧 registered-binding 路径。是正常请求的延迟优化，不是把所有授权查询删掉。新状态类在旧 agent 不支持，仍须全部 agent 先升级再升级运行时。

## 验证

server/control/scopedtoken/user-runtime 全量通过；授权与 binding 专项 race 三次通过，vet 和 diff check 通过。测试覆盖 app/worker/resource 撤销、失败续期不延长有效期、过期 fail closed、空闲超过2s仍后台续期、60s退出预热、版本/worker/resource 不匹配拒绝、active版本变化不重定向旧版本，以及此前固定映射与 fence 测试。

HK COS 同机双 agent、真实 HTTP，6轮c1/c8交替，1KiB、32keys，每轮 direct PUT64，空闲2.2s，forwarded PUT64/GET64，Worker PUT64/GET64。共1920正式请求，失败0，正式 bucket-batch 增量0。Worker PUT 每轮最大耗时13.622/19.273/11.374/17.996/11.792/18.467ms；c1中位8.133/8.636/8.277ms。Worker GET 最大3.715–15.698ms。短测不作为持续吞吐或长尾完全消失的保证；未声称相对旧批次的严格改善比例。

采集到79个 version_authorization_refresh span：78个无parent的后台续期，1个请求内刷新26.875ms，位于首次Worker加载预热（正式轮次之前）；本轮未观察到正式请求内的该类刷新。trace collector 最初后台启动未存活，前段 agent 启动/peer 发现 trace 缺失；已在正式测试前以前台保活工具会话启动 collector，故不宣称完整覆盖启动期。所有正式测量完成后等待导出，再收集和停止collector。

app DELETE HTTP200；owner handoff released18、peer released0，均 stopped，脚本 exit0。未提交或部署。没有额外故障注入生产服务。

证据：同名目录 requests.jsonl、authorization-spans.jsonl；完整trace /tmp/cellhive-version-auth-spans.jsonl，完整日志 /opt/.hermes/cache/scratch/hk-review-1793036237-ac7914-3ojz5pwq/0/result.log；保留数据 /opt/.hermes/cache/scratch/cellhive-e2e.Ujqq1l。
