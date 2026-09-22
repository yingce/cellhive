#!/usr/bin/env bash
# ADR-186 acceptance: pinned image tools, source deployment, tenant env
# isolation, KV, gated Durable Objects, restart durability, and exact boundary
# probes. Every run owns its Compose project, volumes, and temporary files.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

IMAGE=${CELLHIVE_RUNTIME_BASELINE_IMAGE:-cellhive:runtime-baseline}
PROJECT="cellhive-runtime-baseline-$PPID-$$"
PROJECT=${PROJECT//[^a-zA-Z0-9_-]/-}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-runtime-baseline.XXXXXX")
COMPOSE_BASE="$ROOT/deploy/compose/docker-compose.yml"
COMPOSE_OVERRIDE="$TMP/compose.override.yml"
TEST_LOG="$TMP/test-output.log"
HOST_URL_CANARY="runtime-baseline-host-url-canary.invalid"
# This value is never printed. Runtime role credentials are derived from it;
# both the root canary and the derived internal token are scanned below.
export CELLHIVE_ROOT_KEY="runtime-baseline-host-token-canary-20260922-7f6e"

compose() {
  docker compose --project-name "$PROJECT" -f "$COMPOSE_BASE" -f "$COMPOSE_OVERRIDE" "$@"
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ -f "$COMPOSE_OVERRIDE" ]]; then
    compose --profile rpo0 down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT INT TERM

fail() {
  echo "RUNTIME-BASELINE-E2E: FAIL: $*" >&2
  exit 1
}

assert_eq() {
  local want=$1 got=$2 label=$3
  [[ "$got" == "$want" ]] || fail "$label: got '$got', want '$want'"
}

assert_not_contains() {
  local text=$1 needle=$2 label=$3
  [[ "$text" != *"$needle"* ]] || fail "$label contains a host-only canary"
}

retry_capture() {
  local __var=$1; shift
  local out="" i
  for i in $(seq 1 60); do
    if out=$("$@" 2>/dev/null); then
      printf -v "$__var" '%s' "$out"
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "== build pinned runtime image =="
docker build -f deploy/Dockerfile -t "$IMAGE" .
WORKERD_VERSION=$(docker run --rm --entrypoint workerd "$IMAGE" --version)
ESBUILD_VERSION=$(docker run --rm --entrypoint esbuild "$IMAGE" --version)
assert_eq "workerd 2026-09-16" "$WORKERD_VERSION" "image workerd version"
assert_eq "0.28.2" "$ESBUILD_VERSION" "image esbuild version"
echo "image tools: $WORKERD_VERSION; esbuild $ESBUILD_VERSION"

cat >"$COMPOSE_OVERRIDE" <<YAML
services:
  cell-agent:
    image: $IMAGE
    environment:
      CELLHIVE_BASE_DOMAIN: cell.test
      CELLHIVE_DO_RUNTIMES: do-runtime-gated:8788
    networks:
      default:
        aliases:
          - $HOST_URL_CANARY
  user-runtime:
    image: $IMAGE
    environment:
      CELLHIVE_CELL_URL: http://$HOST_URL_CANARY:7001
    ports:
      - "127.0.0.1::8081"
  do-runtime-gated:
    image: $IMAGE
    environment:
      CELLHIVE_CELL_URL: http://$HOST_URL_CANARY:7001
YAML

mkdir -p "$TMP/app/src"
cat >"$TMP/app/wrangler.jsonc" <<'JSON'
{
  "name": "api",
  "main": "src/index.ts",
  "compatibility_date": "2026-09-22",
  "vars": {
    "CELL_URL": "user-cell-url",
    "CELL_TOKEN": "user-cell-token"
  },
  "kv_namespaces": [{ "binding": "KV", "id": "e2e/__kv__/main" }],
  "durable_objects": { "bindings": [{ "name": "COUNTER", "class_name": "Counter" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
JSON
cat >"$TMP/app/src/index.ts" <<'TS'
import { DurableObject } from "cloudflare:workers";

// TypeScript syntax is deliberate: a raw upload cannot execute this file, so
// a successful live request proves the in-image CLI invoked pinned esbuild.
const builtInImage: string = "bundled-by-image-esbuild";

export class Counter extends DurableObject {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    });
  }

  increment(): number {
    const sql = this.ctx.storage.sql;
    sql.exec("INSERT INTO counter(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=n+1");
    const rows = [...sql.exec("SELECT n FROM counter WHERE id=1")];
    return Number(rows[0]?.n || 0);
  }

  async fetch(request: Request): Promise<Response> {
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }
    return Response.json({ count: this.increment() });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    ws.send(JSON.stringify({ echo: String(message), count: this.increment() }));
  }
}

export default {
  async fetch(req: Request, env: Record<string, any>): Promise<Response> {
    const path = new URL(req.url).pathname;
    if (path === "/env") {
      const strings = Object.fromEntries(Object.entries(env).filter(([, value]) => typeof value === "string"));
      return Response.json({ builtInImage, strings, keys: Object.keys(env).sort() });
    }
    if (path === "/kv") {
      await env.KV.put("runtime-baseline", "kv-round-trip");
      return new Response(await env.KV.get("runtime-baseline"));
    }
    if (path === "/do") {
      return env.COUNTER.getByName("counter-1").fetch("https://counter.invalid/increment");
    }
    if (path === "/ws") {
      return env.COUNTER.getByName("counter-1").fetch(req);
    }
    return new Response("fetch-ok:" + builtInImage);
  }
};
TS

echo "== start isolated gated stack =="
compose --profile rpo0 up -d --no-build cell-agent user-runtime do-runtime-gated
READY=""
retry_capture READY compose exec -T cell-agent curl -fsS http://user-runtime:8081/ready \
  || fail "user-runtime did not become ready"
retry_capture READY compose exec -T cell-agent curl -fsS http://do-runtime-gated:8788/ready \
  || fail "gated do-runtime did not become ready"

echo "== deploy TypeScript source with in-image CLI/esbuild =="
compose exec -T cell-agent sh -c 'rm -rf /tmp/runtime-baseline-app && mkdir -p /tmp/runtime-baseline-app'
compose cp "$TMP/app/." cell-agent:/tmp/runtime-baseline-app
compose exec -T -w /tmp/runtime-baseline-app cell-agent cellhive app create e2e >/dev/null
compose exec -T -w /tmp/runtime-baseline-app cell-agent \
  cellhive kv namespace create e2e KV --scope e2e/__kv__/main >/dev/null
set +e
DEPLOY_OUTPUT=$(compose exec -T -w /tmp/runtime-baseline-app cell-agent cellhive deploy e2e - --config wrangler.jsonc 2>&1)
DEPLOY_RC=$?
set -e
if [[ $DEPLOY_RC -ne 0 ]]; then
  printf '%s\n' "$DEPLOY_OUTPUT" >&2
  fail "in-image source deploy failed"
fi
[[ "$DEPLOY_OUTPUT" == *"uploaded bundle"* ]] || fail "source deploy did not upload an esbuild bundle"

fetch_path() {
  local path=$1
  compose exec -T cell-agent curl -fsS -H 'Host: e2e-api.cell.test' "http://user-runtime:8081$path"
}

PUBLIC_ADDR=$(compose port user-runtime 8081 | tail -n 1)
PUBLIC_PORT=${PUBLIC_ADDR##*:}
[[ "$PUBLIC_PORT" =~ ^[0-9]+$ ]] || fail "could not resolve the public user-runtime port"

websocket_roundtrip() {
  local message=$1 expected_count=$2
  bun -e '
const [url, host, message, expectedText] = process.argv.slice(-4);
const expected = Number(expectedText);
const timer = setTimeout(() => { console.error("websocket timeout"); process.exit(2); }, 8000);
const ws = new WebSocket(url, { headers: { Host: host } });
ws.onopen = () => ws.send(message);
ws.onmessage = (event) => {
  let value;
  try { value = JSON.parse(String(event.data)); }
  catch (error) { console.error("invalid websocket JSON", error); process.exit(3); }
  if (value.echo !== message || value.count !== expected) {
    console.error("unexpected websocket response", JSON.stringify(value));
    process.exit(4);
  }
  console.log(JSON.stringify(value));
  clearTimeout(timer);
  ws.close(1000, "done");
};
ws.onclose = (event) => process.exit(event.code === 1000 ? 0 : 5);
ws.onerror = (event) => { console.error("websocket error", event && event.message ? event.message : event); process.exit(6); };
' "ws://127.0.0.1:$PUBLIC_PORT/ws" "e2e-api.cell.test" "$message" "$expected_count"
}

LIVE=""
if ! retry_capture LIVE fetch_path /; then
  echo "-- routing projection --" >&2
  compose exec -T cell-agent cellhive routes >&2 || true
  echo "-- public response --" >&2
  compose exec -T cell-agent curl -sS -i -H 'Host: e2e-api.cell.test' http://user-runtime:8081/ >&2 || true
  echo "-- runtime logs --" >&2
  compose logs --tail=80 user-runtime do-runtime-gated >&2 || true
  fail "deployed worker route did not become live"
fi
assert_eq "fetch-ok:bundled-by-image-esbuild" "$LIVE" "live bundled fetch"

echo "== tenant env isolation and real KV =="
ENV_JSON=$(fetch_path /env)
[[ "$ENV_JSON" == *'"CELL_URL":"user-cell-url"'* ]] || fail "user CELL_URL is not visible"
[[ "$ENV_JSON" == *'"CELL_TOKEN":"user-cell-token"'* ]] || fail "user CELL_TOKEN is not visible"
assert_not_contains "$ENV_JSON" "$HOST_URL_CANARY" "tenant env"
assert_not_contains "$ENV_JSON" "$CELLHIVE_ROOT_KEY" "tenant env"
KV_RESULT=$(fetch_path /kv)
assert_eq "kv-round-trip" "$KV_RESULT" "KV put/get"

INTERNAL_TOKEN=$(compose exec -T cell-agent cellhive creds internal | tr -d '\r\n')
[[ -n "$INTERNAL_TOKEN" ]] || fail "could not derive host internal token"
assert_not_contains "$ENV_JSON" "$INTERNAL_TOKEN" "tenant env"

echo "== gated DO durability and restart =="
DO1=$(fetch_path /do)
assert_eq '{"count":1}' "$DO1" "first gated DO increment"
WS1=""
retry_capture WS1 websocket_roundtrip before-restart 2 \
  || fail "public DO WebSocket failed before runtime restart"
assert_eq '{"echo":"before-restart","count":2}' "$WS1" "public DO WebSocket before restart"
LTX_PROOF=$(compose exec -T cell-agent sh -c "find /data/bucket/cells -type f -name '*.ltx' -print -quit" 2>/dev/null || true)
[[ -n "$LTX_PROOF" ]] || fail "gated DO produced no authoritative bucket LTX object"
compose restart do-runtime-gated >/dev/null
retry_capture READY compose exec -T cell-agent curl -fsS http://do-runtime-gated:8788/ready \
  || fail "gated do-runtime did not recover after restart"
WS2=""
retry_capture WS2 websocket_roundtrip after-restart 3 \
  || fail "public DO WebSocket failed after runtime restart"
assert_eq '{"echo":"after-restart","count":3}' "$WS2" "public DO WebSocket after restart"
DO2=""
retry_capture DO2 fetch_path /do || fail "DO invoke failed after runtime restart"
assert_eq '{"count":4}' "$DO2" "gated DO count after restart"

echo "== rendered config and live boundary scans =="
if compose exec -T user-runtime grep -R -F -e "$HOST_URL_CANARY" -e "$CELLHIVE_ROOT_KEY" -e "$INTERNAL_TOKEN" /data/runtime; then
  fail "rendered runtime files contain a host-only canary"
fi
if compose exec -T do-runtime-gated grep -R -F -e "$HOST_URL_CANARY" -e "$CELLHIVE_ROOT_KEY" -e "$INTERNAL_TOKEN" /data/runtime; then
  fail "rendered gated runtime files contain a host-only canary"
fi

echo "== captured WorkerCode, near-limit, and compatibility probes =="
SQLITE_TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"
go test -tags "$SQLITE_TAGS" ./internal/workerbudget ./internal/server \
  -run 'TestCheckCodeBoundary|TestEstimateEnvChargesJSONAndTwoByteStrings|TestDeployBudgetExactBoundaries' \
  -count=1 -p 1 -v | tee -a "$TEST_LOG"
go test -tags "$SQLITE_TAGS" ./internal/userruntime ./internal/doruntime \
  -run 'TestDynamicWorkerCodeContainsNoPlatformCredential|TestRuntimeRejects(User|DO)EnvOverBudget|Test(Tenant|User|DoRuntime)Env' \
  -count=1 -p 1 -v | tee -a "$TEST_LOG"
node --test workerd/platform/budget.test.mjs | tee -a "$TEST_LOG"

PROBE_CONTAINER=$(docker create "$IMAGE")
docker cp "$PROBE_CONTAINER:/usr/local/bin/workerd" "$TMP/workerd"
docker rm "$PROBE_CONTAINER" >/dev/null
chmod 0755 "$TMP/workerd"
CELLHIVE_WORKERD="$TMP/workerd" bash scripts/workerd-compat-probe.sh | tee -a "$TEST_LOG"

if grep -Ei '(^|[[:space:]])(SKIP|SKIPPED)([[:space:]:]|$)' "$TEST_LOG" \
    | grep -Eiv 'skipped[[:space:]]+0([[:space:]]|$)' >/dev/null; then
  fail "a required runtime-baseline probe was skipped"
fi

echo "RUNTIME-BASELINE-E2E: PASS"
