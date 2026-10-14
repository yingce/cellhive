# 进程池系数控制实验：2026-10-14

## 目的

验证 user-runtime Go supervisor 是否可以只暴露少量配置：`memoryUtilization` + `maxSlots`，由控制器根据 cgroup 内存上限、保守单槽估计和当前 RSS 动态决定槽数；不把启动内存、冷启动余量、冷却时间、排队等全部变成用户参数。

这是非生产实验，不改 CellHive 生产路由或进程管理。

## 实验实现

- `workerd/spikes/process-supervision/adaptive.go`：最小自适应池；监控 `/sys/fs/cgroup/memory.max`、`memory.current`、每个 workerd RSS；进程由 Go 启动和回收。
- `workerd/spikes/process-supervision/main.go`：复用 pinned workerd 启动、socket-fd、证据输出。
- Docker：`--memory=512m --memory-swap=512m --cpus=1`，swap 禁用，真实 `workerd 2026-09-16`。
- 内置策略（不暴露为配置）：Go reserve=64 MiB；初始单槽估计=96 MiB；采样高水位后估计为 RSS×1.25；预算=memory.max×系数；最终槽数还受 `maxSlots=8` 限制。
- handler 支持 32 MiB / 96 MiB retained allocation、800ms busy slot、普通请求；测试没有并发请求驱动吞吐，重点是容量控制状态。
- “预算减半”只模拟控制器预算变化，实际 cgroup `memory.max` 不变，**不等于 VPA 原地缩容验收**。

运行：

```bash
PATH=/usr/local/go/bin:$PATH go run ./workerd/spikes/process-supervision \
  -adaptive -memory-utilization 0.5 -max-slots 8 \
  -workerd /opt/cellhive/cli/node_modules/@cloudflare/workerd-linux-64/bin/workerd \
  -out /tmp/opencode
```

原始证据：

- `/tmp/opencode/adaptive-4018748207/`（系数 0.5）
- `/tmp/opencode/adaptive-4186233207/`（系数 0.7）

## 结果

| 系数 | 启动槽数 | 32 MiB/槽后 | 96 MiB/槽后 | 96 MiB后处理 | OOM事件 |
|---:|---:|---:|---:|---|---:|
| 0.5 | 2 | 2 | 估算 171 MiB/槽，target=1 | 回收至 1 槽，忙槽完成后可回收至 0 | 0 |
| 0.7 | 3 | 3 | 估算 171 MiB/槽，target=1 | 回收至 1 槽，忙槽完成后可回收至 0 | 0 |

### 系数 0.5 关键采样

```text
baseline                 slots=0  cgroup=2.3 MiB
idle_demand              slots=2  RSS=78.9 MiB   target=2
allocated32_before      slots=2  RSS=144.2 MiB  target=2
allocated96_before      slots=2  RSS=273.9 MiB  target=1
allocated96_reconciled  slots=1  RSS=136.9 MiB  target=1
budget_halved_busy      slots=1  new_admission=false, busy preserved
busy_completed          slots=0
budget_restored         slots=1  RSS=39.0 MiB
memory.events           max=0, oom=0, oom_kill=0
```

### 系数 0.7 关键采样

```text
baseline                 slots=0  cgroup=2.3 MiB
idle_demand              slots=3  RSS=117.9 MiB target=3
allocated32_before      slots=3  RSS=215.6 MiB target=3
allocated96_before      slots=3  RSS=410.0 MiB target=1
allocated96_reconciled  slots=1  RSS=136.4 MiB target=1
budget_halved_busy      slots=1  new_admission=false, busy preserved
busy_completed          slots=0
budget_restored         slots=1  RSS=39.8 MiB
memory.events           max=0, oom=0, oom_kill=0
```

## 判断

1. **系数+绝对槽上限足以简化操作配置**：在同一 512 MiB cgroup，0.5/0.7 直接改变初始槽数（2/3），不需要用户配置十多个阈值。
2. **不能只按平均启动占用算槽**：代码从 32 MiB 增到 96 MiB 后，保守估计升到约 171 MiB，target 从 2/3 降到 1；控制器必须观察高水位并停止新槽/回收空闲槽。
3. **缩容不能杀忙槽**：预算下调时停止准入，忙槽继续到请求结束，再回收；这是安全的缩容顺序。
4. **系数不是 per-isolate hard limit**：一个槽里的 function 仍可把槽进程撑到上限；系数只控制池扩张和槽准入。cgroup 才是最后硬边界。
5. **资源公式建议保持内部策略**：`target = min(maxSlots, floor((memory.max*coefficient-supervisorReserve)/conservativeSlotEstimate))`；还要在启动前检查 `memory.current + estimate + reserve`。

## 建议的最小配置

```yaml
pool:
  mode: adaptive
  memoryUtilization: 0.70
  maxSlots: 16
```

`maxSlots` 是绝对护栏；`memoryUtilization` 留出 Go、冷启动、请求峰值和系统余量。初始默认值仍需用真实 Worker/框架压测，不把本实验的 0.5/0.7 宣称为生产默认。

自动热点状态机模型另由 `workerd/spikes/process-supervision/hotspot.go` 实现，并由 `hotspot_test.go` 验证 promotion/demotion 滞回：连续 3 个 hot sample 才提升，连续 5 个 cold sample 才降级。该模型实验不启动生产路由，实际进程迁移仍需在独立调度器原型中验证。

内置而不暴露的策略：高水位采样、RSS×安全倍率、扩缩冷却、空闲回收、启动失败退避、最大排队、WS/长流 pin、忙槽 drain。HPA 管 Pod 数，Go 管 Pod 内槽数；VPA 改变资源后新 Pod 重新计算，原地 cgroup 变化需单独监听和验证。

## 仍需测试

- 独立负载发生器下的 RPS/p99 和扩缩稳定性；
- 真实 dynamic loader/框架及不同 bundle 大小；
- 高并发启动峰值、Queue/Cron/Workflow 长任务；
- WS/RPC 与槽 pin、Pod drain；
- TKE VPA 实际替换/原地 resize、cgroup v2 和 nofile；
- 多 Pod HPA 与 Go 槽控制器的防抖/协调。

## adaptive user-runtime 真机 Docker/k3d 结果

- `CELLHIVE_POOL_MODE=adaptive CELLHIVE_POOL_MAX_SLOTS=4 bash scripts/runtime-baseline-e2e.sh`：`RUNTIME-BASELINE-E2E: PASS`；镜像内 workerd 2026-09-16 / esbuild 0.28.2，Tenant env/KV/assets/Queue/Cron/Workflow/DLQ/replay/gated DO restart/rollback/delete/budget/compat probe 均完成。
- `bash scripts/k3d-runtime-e2e.sh`：`K3D-RUNTIME-E2E: PASS`；两个 gated Pod、owner Pod 删除冷接管、滚动更新期间 120 个逻辑写，最终 `1,2,123`。该脚本使用当前默认 single-workerd manifests，证明 DO 基线，不证明 Go pool 的 TKE 多节点接管。
- `CELLHIVE_K3D_AGENTS=2 bash scripts/k3d-runtime-e2e.sh` 已尝试创建 1 server + 2 agent 的本地多节点集群；本机 30 分钟执行窗口内在 k3d agent 启动阶段超时/被 harness 终止，集群清理完成，没有把它记为通过。
- Go pool 专用真机测试：`internal/userruntimepool TestManagerProxyRealPinnedWorkerAndProcessReplacement` 和 `TestHotspotCreatesDedicatedChildAndColdPeerStaysShared` 通过；前者覆盖固定版本、loopback 子进程、进程替换，后者覆盖持续请求后的 dedicated promotion、冷 peer 不误用 dedicated 槽和冷却回收。

真实 TKE 尚未执行：当前 context `.kube/config_tke` 没有 cluster server，kubectl 回退 `http://localhost:8080` 并被拒绝；不能把 k3d 结果当作 TKE 验收。
