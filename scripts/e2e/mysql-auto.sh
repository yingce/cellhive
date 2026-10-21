#!/usr/bin/env bash
# E2E: resource create -> CellHive auto-configures ProxySQL -> worker mysql2 query.
# Requires Docker, a built bin/cell-agent, and a mysql2 worker bundle.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${E2E_BIN:-$ROOT/bin}"
PORT="${E2E_MYSQL_PORT_BASE:-19700}"
ADMIN_PORT=$((PORT + 1)); RT_PORT=$((PORT + 2)); INT_PORT=$((PORT + 3))
BASE="$(mktemp -d "${TMPDIR:-/tmp}/cellhive-mysql-e2e.XXXXXX")"
MYSQL_NAME=ch-mysql-e2e; PROXY_NAME=ch-proxysql-e2e
BUNDLE="${MYSQL_E2E_BUNDLE:-$ROOT/scripts/e2e/mysql/worker.bundle.js}"
if [[ ! -f "$BUNDLE" ]]; then
  echo "missing mysql2 worker bundle: $BUNDLE" >&2
  echo "run scripts/e2e/mysql/build-bundle.sh first, or set MYSQL_E2E_BUNDLE" >&2
  exit 2
fi
for c in docker curl jq node openssl; do command -v "$c" >/dev/null || { echo "missing $c" >&2; exit 2; }; done
[[ -x "$BIN/cell-agent" && -x "$BIN/user-runtime" && -x "$BIN/cellhive" ]] || { echo "run make build first" >&2; exit 2; }
export CELLHIVE_ROOT_KEY="${CELLHIVE_ROOT_KEY:-$(openssl rand -hex 32)}"
ADMIN_TOKEN="$(node --input-type=module -e 'import {hkdfSync} from "node:crypto";const k=Buffer.from(process.env.CELLHIVE_ROOT_KEY,"hex");process.stdout.write(Buffer.from(hkdfSync("sha256",k,Buffer.alloc(0),"cellhive/token/admin",32)).toString("base64url"))')"
cleanup() {
  kill -9 "$(cat "$BASE/agent.pid" 2>/dev/null)" "$(cat "$BASE/runtime.pid" 2>/dev/null)" 2>/dev/null || true
  pkill -9 -f "workerd serve --experimental $BASE" 2>/dev/null || true
  docker rm -f "$MYSQL_NAME" "$PROXY_NAME" >/dev/null 2>&1 || true
  [[ "${E2E_KEEP:-0}" == 1 ]] && echo "kept $BASE" || rm -rf "$BASE"
}
trap cleanup EXIT
mkdir -p "$BASE/agent" "$BASE/bucket"

docker rm -f "$MYSQL_NAME" "$PROXY_NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$MYSQL_NAME" -p 13306:3306 \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=app -e MYSQL_USER=app -e MYSQL_PASSWORD=secret \
  mysql:8.0 --default-authentication-plugin=mysql_native_password >/dev/null
for i in $(seq 1 60); do docker exec "$MYSQL_NAME" mysql -uapp -psecret -h127.0.0.1 -N -e 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
docker run -d --name "$PROXY_NAME" --network host proxysql/proxysql:3.0.11 >/dev/null
for i in $(seq 1 40); do (echo >/dev/tcp/127.0.0.1/6032) >/dev/null 2>&1 && break; sleep .5; done

CELLHIVE_NODE_ID=mysql-auto CELLHIVE_ADVERTISE="127.0.0.1:$PORT" CELLHIVE_PEER_URL="http://127.0.0.1:$PORT" \
  CELLHIVE_REST_ADDR="127.0.0.1:$PORT" CELLHIVE_ADMIN_ADDR="127.0.0.1:$ADMIN_PORT" \
  CELLHIVE_DATA_DIR="$BASE/agent" CELLHIVE_BUCKET_DIR="$BASE/bucket" CELLHIVE_DISPATCH_URL="http://127.0.0.1:$INT_PORT" \
  CELLHIVE_HYPERDRIVE_MODE=proxy \
  CELLHIVE_HYPERDRIVE_PROXY='postgres://127.0.0.1:6133,mysql://127.0.0.1:6033' \
  CELLHIVE_PROXYSQL_ADMIN='admin:admin@tcp(127.0.0.1:6032)/' \
  CELLHIVE_HYPERDRIVE_MANAGED_USERS=1 \
  setsid nohup "$BIN/cell-agent" >"$BASE/agent.log" 2>&1 </dev/null &
echo $! >"$BASE/agent.pid"
for i in $(seq 1 80); do [[ "$(curl -s -o /dev/null -m1 -w '%{http_code}' "http://127.0.0.1:$PORT/readyz" || true)" == 200 ]] && break; sleep .3; done
env CELLHIVE_CELL_URL="http://127.0.0.1:$PORT" CELLHIVE_USER_RUNTIME_PORT="$RT_PORT" CELLHIVE_USER_RUNTIME_INTERNAL_PORT="$INT_PORT" \
  CELLHIVE_RUNTIME_DIR="$BASE/runtime" CELLHIVE_USER_RUNTIME_JS="$ROOT/workerd/user-runtime" \
  CELLHIVE_FACADES_JS="$ROOT/workerd/platform/facades.js" CELLHIVE_WORKERD="${CELLHIVE_WORKERD:-$ROOT/cli/node_modules/.bin/workerd}" \
  CELLHIVE_TENANT_OUTBOUND=public,private,local \
  setsid nohup "$BIN/user-runtime" >"$BASE/runtime.log" 2>&1 </dev/null &
echo $! >"$BASE/runtime.pid"
for i in $(seq 1 120); do [[ "$(curl -s -o /dev/null -m1 -w '%{http_code}' "http://127.0.0.1:$RT_PORT/ready" || true)" == 200 ]] && break; sleep .25; done

export CELLHIVE_ADMIN_URL="http://127.0.0.1:$ADMIN_PORT" CELLHIVE_ADMIN_TOKEN="$ADMIN_TOKEN"
"$BIN/cellhive" app create mysqlauto >/dev/null
"$BIN/cellhive" domain add mysqlauto mysqlauto.test >/dev/null
"$BIN/cellhive" resource create mysqlauto hyperdrive hd1 \
  --connection-string 'mysql://root:root@127.0.0.1:13306/app?ssl-mode=DISABLED' || { echo "resource create failed" >&2; exit 1; }
sleep 1

servers=$(docker exec "$PROXY_NAME" mysql -h127.0.0.1 -P6032 -uadmin -padmin -N -e 'SELECT hostgroup_id,hostname,port FROM runtime_mysql_servers' 2>/dev/null || true)
[[ -n "$servers" ]] || { echo "CellHive did not create mysql_servers" >&2; exit 1; }
users=$(docker exec "$PROXY_NAME" mysql -h127.0.0.1 -P6032 -uadmin -padmin -N -e "SELECT username FROM runtime_mysql_users WHERE username LIKE 'hd_%'" 2>/dev/null || true)
[[ -n "$users" ]] || { echo "CellHive did not create generated mysql user" >&2; exit 1; }
roles=$(docker exec "$MYSQL_NAME" mysql -uroot -proot -N -e "SELECT user FROM mysql.user WHERE user LIKE 'hd_%'" 2>/dev/null || true)
[[ -n "$roles" ]] || { echo "generated role missing on MySQL origin" >&2; exit 1; }
echo "proxysql servers=$servers users=$users origin_roles=$roles"

SHA=$(curl -s -X POST -H "x-cellhive-admin-token: $ADMIN_TOKEN" --data-binary @"$BUNDLE" "http://127.0.0.1:$ADMIN_PORT/v1/control/bundle" | jq -r .sha)
[[ -n "$SHA" && "$SHA" != null ]] || { echo "bundle upload failed" >&2; exit 1; }
cat >"$BASE/deploy.json" <<JSON
{"namespace":"mysqlauto","worker":"mysqlworker","bundle_sha":"$SHA","bindings":[{"type":"hyperdrive","name":"HYPERDRIVE","id":"hd1"}],"compatibility_flags":["nodejs_compat"]}
JSON
curl -s -X POST -H "x-cellhive-admin-token: $ADMIN_TOKEN" -H 'content-type: application/json' \
  --data-binary @"$BASE/deploy.json" "http://127.0.0.1:$ADMIN_PORT/v1/control/deploy" >/dev/null
"$BIN/cellhive" route add mysqlauto mysqlauto.test mysqlworker >/dev/null
out=""
for i in $(seq 1 40); do out=$(curl -s -m 20 -H 'Host: mysqlauto.test' "http://127.0.0.1:$RT_PORT/"); echo "$out" | grep -q '"ok"' && break; sleep .5; done
echo "worker=$out"
[[ "$out" == *'"ok":1'* ]] || { echo "worker query failed" >&2; exit 1; }

# Health gate: while ProxySQL is down the cell-agent must stop advertising
# readiness and report the unavailable proxy; it must recover after restart.
docker stop "$PROXY_NAME" >/dev/null
code=""
for i in $(seq 1 30); do
  code=$(curl -s -o "$BASE/ready-down.json" -m 2 -w '%{http_code}' "http://127.0.0.1:$PORT/readyz" || true)
  [[ "$code" == 503 ]] && break
  sleep .5
done
[[ "$code" == 503 ]] || { echo "readyz did not fail closed with ProxySQL down: $code" >&2; exit 1; }
grep -q hyperdrive_proxy_unavailable "$BASE/ready-down.json" || { echo "missing proxy readiness reason: $(cat "$BASE/ready-down.json")" >&2; exit 1; }
echo "health down=503 hyperdrive_proxy_unavailable"
docker start "$PROXY_NAME" >/dev/null
code=""
for i in $(seq 1 40); do
  code=$(curl -s -o /dev/null -m 2 -w '%{http_code}' "http://127.0.0.1:$PORT/readyz" || true)
  [[ "$code" == 200 ]] && break
  sleep .5
done
[[ "$code" == 200 ]] || { echo "readyz did not recover after ProxySQL restart: $code" >&2; exit 1; }
echo "health up=200"

"$BIN/cellhive" resource delete mysqlauto hyperdrive hd1 --force >/dev/null 2>&1 || true
sleep 1
servers=$(docker exec "$PROXY_NAME" mysql -h127.0.0.1 -P6032 -uadmin -padmin -N -e 'SELECT hostgroup_id FROM runtime_mysql_servers' 2>/dev/null || true)
[[ -z "$servers" ]] || { echo "resource delete did not remove mysql_servers: $servers" >&2; exit 1; }
roles_after=$(docker exec "$MYSQL_NAME" mysql -uroot -proot -N -e "SELECT user FROM mysql.user WHERE user LIKE 'hd_%'" 2>/dev/null || true)
[[ -z "$roles_after" ]] || { echo "resource delete did not drop generated role: $roles_after" >&2; exit 1; }
echo "proxysql cleanup=ok role cleanup=ok"
echo "E2E-MYSQL: PASS"
