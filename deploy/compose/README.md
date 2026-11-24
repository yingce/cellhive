# 本地 Redis 协调栈

`docker-compose.yml` 默认保持对象存储协调；叠加 `docker-compose.local.yml`
后启动独立 Redis，并只给 cell-agent 设置
`CELLHIVE_COORDINATION_BACKEND=redis://redis:6379/0`，同时启用共享调度。
Redis 不映射宿主端口，使用持久卷、AOF `appendfsync always` 和 `noeviction`。
SQLite、LTX 等业务耐久数据仍使用原来的对象存储。

首次初始化（包括从已有对象存储协调切换）必须停止所有旧写入者：

```bash
export CELLHIVE_ROOT_KEY=... # 使用已有值，不要重新生成
dc() { docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.local.yml "$@"; }
# 先构建新镜像，并备份现有配置、数据卷及桶；保留原来的项目名和卷映射。
dc build
dc stop user-runtime do-runtime do-runtime-gated cell-agent
dc up -d redis
dc run --rm --no-deps cell-agent cell-agent coordination-init --offline
dc up -d cell-agent user-runtime do-runtime
```

已有栈若使用其他 Compose 文件、MinIO 配置或外部卷，必须在其有效配置上合并
Redis 服务和 agent 环境变量，不能用默认卷覆盖原映射。
初始化只执行一次；已经 active 时工具会拒绝重新初始化。
后续正常启停使用 `up -d` / `stop`，不要执行 `down -v`。
Redis 卷丢失需离线恢复；不得直接改回 COS 或启动旧版本绕过 binding。
`rpo0` profile 的部署应启动 `do-runtime-gated`，保留原来的 gated 路由。

已有本机实例的合并配置保存为被 Git 忽略的
`deploy/compose/docker-compose.active.local.json`（包含本机凭据，权限 0600）。
对该实例使用其原项目名：

```bash
docker compose -p cellhive-migration-test -f deploy/compose/docker-compose.active.local.json ps
docker compose -p cellhive-migration-test -f deploy/compose/docker-compose.active.local.json up -d
```

该文件保留原 MinIO、卷名、端口及 gated do-runtime 配置，并固定本次本地镜像。
后续源码变更需要重新构建镜像并更新此本机配置；不会自动部署工作区改动。

## 双 agent fleet

叠加 `docker-compose.fleet.yml` 可启动第二个独立卷的 `cell-agent-b`，
两个 agent 都使用 `CELLHIVE_DURABILITY=fleet`，通过 Redis 注册发现对方。
必须配置同一个远程 `CELLHIVE_BUCKET` 和 endpoint/凭据；不能用两个独立的本地桶目录。
peer URL 使用各自的容器 DNS 名称，不能用容器内的 localhost。

```bash
docker compose -f deploy/compose/docker-compose.yml \
  -f deploy/compose/docker-compose.local.yml \
  -f deploy/compose/docker-compose.fleet.yml up -d
```

前述本机 active 配置现已合入双 agent fleet，日常仍使用 active 文件启停。
有活 peer 时以 follower fsync 确认；无 peer 时仍按现有协议退为本地 spool，
不等于双副本可用。两个容器在同一宿主机，不能代表跨机器容灾。

## 本机恢复权威 v3

当前本机 active 配置已更新为 `cellhive:local-recovery-v3-20261124`，binding v3，两个 agent 保持 fleet，epoch GC 周期 `1h`、首次 `1m`。Session 正文与 DO 恢复指针在 Redis；旧桶副本不再更新。不能直接回退旧镜像或 Redis 备份。见 [部署验收](../../docs/reports/2026-11-24-local-recovery-v3.md)。云上版本未随此次本机部署改变。
