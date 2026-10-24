#!/usr/bin/env bash
# ADR-186 acceptance: pinned image tools, source deployment, tenant env
# isolation, KV, gated Durable Objects, restart durability, and exact boundary
# probes. Every run owns its Compose project, volumes, and temporary files.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

# Match the full CI entrypoint: bare automation shells may not include the
# installed Go/Bun directories even though the toolchains are present.
for d in /usr/local/go/bin /root/.bun/bin; do
  [[ -d "$d" && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"
done
export PATH

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
  if [[ $rc -ne 0 && ${CELLHIVE_RUNTIME_BASELINE_KEEP_ON_FAIL:-0} == 1 ]]; then
    echo "RUNTIME-BASELINE-E2E: retained project=$PROJECT tmp=$TMP" >&2
    exit "$rc"
  fi
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
docker build --build-context workerd-fork=deploy/workerd-fork -f deploy/Dockerfile -t "$IMAGE" .
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
      CELLHIVE_CRON_INTERVAL: 1s
      CELLHIVE_TIMER_INTERVAL: 200ms
      CELLHIVE_QUEUE_INTERVAL: 200ms
      CELLHIVE_QUEUE_LEASE: 30s
      CELLHIVE_QUEUE_RETRY_DELAY: 1s
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

mkdir -p "$TMP/app/src" "$TMP/app/public"
cat >"$TMP/app/public/asset.txt" <<'ASSET'
asset-from-versioned-object-storage
ASSET
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
  "queues": {
    "producers": [{ "binding": "JOBS", "queue": "JOBS" }],
    "consumers": [{ "queue": "JOBS", "max_batch_size": 10, "max_batch_timeout": 2, "max_retries": 2, "retry_delay": 1, "dead_letter_queue": "DLQ" }]
  },
  "workflows": [{ "binding": "REPORTS", "name": "REPORTS", "class_name": "ReportBuilder" }],
  "triggers": { "crons": ["* * * * *", "*/1 * * * *"] },
  "assets": { "directory": "./public", "run_worker_first": ["/env", "/kv", "/do", "/ws", "/queue", "/receipt", "/workflow"] },
  "durable_objects": { "bindings": [{ "name": "COUNTER", "class_name": "Counter" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
JSON
cat >"$TMP/app/src/index.ts" <<'TS'
import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";

// TypeScript syntax is deliberate: a raw upload cannot execute this file, so
// a successful live request proves the in-image CLI invoked pinned esbuild.
const builtInImage: string = "bundled-by-image-esbuild";

export class Counter extends DurableObject {
  private readonly managedSecret: string;
  constructor(ctx: DurableObjectState, env: Record<string, any>) {
    super(ctx, env);
    this.managedSecret = String(env.MANAGED_SECRET || "missing");
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
    const path = new URL(request.url).pathname;
    if (path === "/alarm/arm") {
      await this.ctx.storage.setAlarm(Date.now() + 1500);
      return new Response("armed");
    }
    if (path === "/alarm/state") return Response.json({ count: (await this.ctx.storage.get("alarm-count")) || 0, due: await this.ctx.storage.getAlarm() });
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }
    return Response.json({ count: this.increment(), secret: this.managedSecret });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    ws.send(JSON.stringify({ echo: String(message), count: this.increment() }));
  }
  async alarm(): Promise<void> {
    const n = ((await this.ctx.storage.get("alarm-count")) as number || 0) + 1;
    await this.ctx.storage.put("alarm-count", n);
    // Prove retries retain the original alarm and successful rearm survives
    // consumption of that original delivery.
    if (n === 1) throw new Error("alarm retry probe");
    if (n === 2) await this.ctx.storage.setAlarm(Date.now() + 1000);
  }
}

export class ReportBuilder extends WorkflowEntrypoint {
  async run(event: any, step: any): Promise<any> {
    if (event.payload?.wait) {
      const ready = await step.waitForEvent("ready", { type: "ready" });
      return { ...ready, releaseTag: this.env.RELEASE_TAG || "original", secret: this.env.MANAGED_SECRET };
    }
    const first = await step.do("first", async () => Number(event.payload?.seed || 0) + 1);
    await step.sleep("pause", "1 second");
    return { value: first * 2, secret: this.env.MANAGED_SECRET };
  }
}

export default {
  async fetch(req: Request, env: Record<string, any>): Promise<Response> {
    const path = new URL(req.url).pathname;
    if (path === "/env") {
      const strings = Object.fromEntries(Object.entries(env).filter(([, value]) => typeof value === "string"));
      return Response.json({ builtInImage, strings, managedSecret: env.MANAGED_SECRET, keys: Object.keys(env).sort() });
    }
    if (path === "/kv") {
      await env.KV.put("runtime-baseline", "kv-round-trip");
      return new Response(await env.KV.get("runtime-baseline"));
    }
    if (path === "/native/kv-short") {
      await env.KV.put("native-short", "short", { expirationTtl: 1 });
      return new Response("armed");
    }
    if (path === "/native/kv-short-state") {
      return new Response(String(await env.KV.get("native-short")));
    }
    if (path === "/native/queue-idempotency") {
      const messages = [{ body: { id: "native-idempotent" }, idempotencyKey: "native-key", metadata: { trace: "native-trace", n: "9007199254740993" } }];
      const first = await env.JOBS.sendBatch(messages);
      const second = await env.JOBS.sendBatch(messages);
      return Response.json({ first: first.ids, second: second.ids });
    }
    if (path === "/queue") {
      const id = new URL(req.url).searchParams.get("id") || "job-1";
      return Response.json(await env.JOBS.send({ id }));
    }
    if (path === "/receipt") {
      const id = new URL(req.url).searchParams.get("id") || "job-1";
      return Response.json({ queue: await env.KV.get("queue:" + id), metadata: await env.KV.get("queue:metadata"), lastBody: await env.KV.get("queue:last-body"), cron: await env.KV.get("cron:last"), cronA: await env.KV.get("cron:* * * * *"), cronB: await env.KV.get("cron:*/1 * * * *"), cronASlot: await env.KV.get("cron:slot:* * * * *"), cronBSlot: await env.KV.get("cron:slot:*/1 * * * *"), duplicate: await env.KV.get("cron:duplicate") });
    }
    if (path === "/recover-queue") {
      await env.KV.put("queue:recover", "yes");
      return new Response("ready");
    }
    if (path === "/workflow/create") {
      const instance = await env.REPORTS.create({ params: { seed: 20, wait: new URL(req.url).searchParams.has("wait") } });
      return Response.json({ id: instance.id });
    }
    if (path === "/workflow/batch") {
      return Response.json(await env.REPORTS.createBatch([{ id: "batch-a", params: { seed: 1 } }, { id: "batch-b", params: { seed: 2 } }]));
    }
    if (path === "/workflow/status") {
      const id = new URL(req.url).searchParams.get("id");
      return Response.json(await (await env.REPORTS.get(id)).status());
    }
    if (path === "/workflow/event") {
      const instance = await env.REPORTS.get(new URL(req.url).searchParams.get("id"));
      await instance.sendEvent({ type: "ready", payload: { value: 77 } });
      return new Response("sent");
    }
    if (path.startsWith("/do/alarm/")) return env.COUNTER.getByName("alarm-probe").fetch("http://alarm" + path.slice(3));
    if (path === "/do") {
      return env.COUNTER.getByName("counter-1").fetch("https://counter.invalid/increment");
    }
    if (path === "/ws") {
      return env.COUNTER.getByName("counter-1").fetch(req);
    }
    return new Response("fetch-ok:" + builtInImage);
  },
  async queue(batch: any, env: Record<string, any>): Promise<void> {
    console.log("queue invocation", batch.queue);
    for (const message of batch.messages) {
      console.info("queue message", message.body.id, "attempt", message.attempts);
      if (message.body.id === "version-probe" && env.RELEASE_TAG !== "v2") {
        // The E2E kills cell-agent after this claim. This old handler must not
        // write a receipt; a lease-expired redelivery migrates to active v2.
        await new Promise(resolve => setTimeout(resolve, 60000));
        throw new Error("old version must not complete version-probe");
      }
      if (message.body.id === "invocation-fail" && message.attempts === 1) throw new Error("invocation failure probe");
      await env.KV.put("queue:last-body", JSON.stringify(message.body));
      if (message.body.id === "poison" && await env.KV.get("queue:recover") !== "yes") {
        message.retry();
        continue;
      }
      await env.KV.put("queue:" + message.body.id, message.body.id === "version-probe" ? "processed:v2" : "processed");
      if (message.body.id === "native-idempotent") await env.KV.put("queue:metadata", JSON.stringify(message.metadata));
      message.ack();
    }
  },
  async scheduled(event: any, env: Record<string, any>): Promise<void> {
    console.warn("cron invocation", event.cron);
    const key = "cron-slot:" + event.cron + ":" + event.scheduledTime;
    const count = Number(await env.KV.get(key) || 0) + 1;
    await env.KV.put(key, String(count));
    if (count > 1) await env.KV.put("cron:duplicate", "yes");
    await env.KV.put("cron:" + event.cron, "ran");
    await env.KV.put("cron:slot:" + event.cron, String(event.scheduledTime));
    await env.KV.put("cron:last", String(event.cron));
    if (event.cron === "* * * * *") { event.noRetry(); throw new Error("scheduled noRetry probe"); }
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
compose exec -T -w /tmp/runtime-baseline-app cell-agent \
  cellhive queue create e2e JOBS >/dev/null
compose exec -T -w /tmp/runtime-baseline-app cell-agent \
  cellhive queue create e2e DLQ >/dev/null
compose exec -T -w /tmp/runtime-baseline-app cell-agent \
  cellhive workflows create e2e REPORTS >/dev/null
set +e
DEPLOY_OUTPUT=$(compose exec -T -w /tmp/runtime-baseline-app cell-agent cellhive deploy e2e - --config wrangler.jsonc 2>&1)
DEPLOY_RC=$?
set -e
if [[ $DEPLOY_RC -ne 0 ]]; then
  printf '%s\n' "$DEPLOY_OUTPUT" >&2
  fail "in-image source deploy failed"
fi
[[ "$DEPLOY_OUTPUT" == *"uploaded bundle"* ]] || fail "source deploy did not upload an esbuild bundle"
compose exec -T cell-agent cellhive secret put e2e api MANAGED_SECRET --value czNjcjN0 >/dev/null

# Force the batch to contend for a single execution lease. Queued instances
# must wait durably rather than blocking each other or losing their wake.
ADMIN_TOKEN=$(compose exec -T cell-agent cellhive creds admin | tr -d '\r\n')
CONSUMER_SETTINGS=$(compose exec -T cell-agent curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://127.0.0.1:8082/client/v4/accounts/e2e/queues/JOBS/consumers)
[[ "$CONSUMER_SETTINGS" == *'"retry_delay":1'* && "$CONSUMER_SETTINGS" == *'"max_wait_time_ms":2000'* ]] \
  || fail "Queue consumer timeout/retry config was not persisted: $CONSUMER_SETTINGS"
compose exec -T cell-agent curl -fsS -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' --data '{"script_name":"api","class_name":"ReportBuilder","concurrency":{"limit":1}}' \
  http://127.0.0.1:8082/client/v4/accounts/e2e/workflows/REPORTS >/dev/null

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

echo "== assets, queue, cron, and workflow user journeys =="
ASSET=$(fetch_path /asset.txt)
if [[ "$ASSET" != "asset-from-versioned-object-storage" ]]; then
  echo "-- deploy output --" >&2
  printf '%s\n' "$DEPLOY_OUTPUT" >&2
  echo "-- routing projection --" >&2
  compose exec -T cell-agent cellhive routes >&2 || true
  echo "-- asset objects --" >&2
  compose exec -T cell-agent sh -c "find /data/bucket/assets -type f -print" >&2 || true
  echo "-- runtime logs --" >&2
  compose logs --tail=120 user-runtime cell-agent >&2 || true
  fail "versioned static asset: got '$ASSET'"
fi

QUEUE_SENT_MS=$(date +%s%3N)
QUEUE_SEND=$(fetch_path '/queue?id=job-42')
[[ "$QUEUE_SEND" == *'"id"'* ]] || fail "queue producer did not return a message id: $QUEUE_SEND"
QUEUE_RECEIPT=""
retry_capture QUEUE_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=job-42' | grep -F '\"queue\":\"processed\"' >/dev/null && printf processed" \
  || fail "real queue consumer did not persist its receipt"
QUEUE_DONE_MS=$(date +%s%3N)
QUEUE_WAIT_MS=$((QUEUE_DONE_MS - QUEUE_SENT_MS))
((QUEUE_WAIT_MS >= 1500 && QUEUE_WAIT_MS <= 15000)) \
  || fail "Queue batch timeout=2s fired outside the real-service window: ${QUEUE_WAIT_MS}ms"
echo "queue partial-batch delivery after ${QUEUE_WAIT_MS}ms (configured max_wait_time_ms=2000)"
echo "== queue DLQ and operator replay =="
fetch_path '/queue?id=poison' >/dev/null
DLQ_STATUS=""
retry_capture DLQ_STATUS compose exec -T cell-agent sh -c \
  "out=\$(cellhive queue status e2e JOBS); echo \"\$out\" | grep -E '\"dead_letter_depth\"[[:space:]]*:[[:space:]]*[1-9]' >/dev/null && printf '%s' \"\$out\"" \
  || { compose exec -T cell-agent cellhive queue status e2e JOBS >&2 || true; compose exec -T cell-agent cellhive queue status e2e DLQ >&2 || true; compose logs --tail=80 cell-agent user-runtime >&2 || true; fail "poison message did not reach the DLQ"; }
admin_api() {
  compose exec -T cell-agent curl -fsS -H "x-cellhive-admin-token: $ADMIN_TOKEN" "$@"
}
echo "== read-only DLQ preview and selected idempotent ack =="
DLQ_PREVIEW=$(admin_api 'http://127.0.0.1:8082/v1/queue/dead-letters?namespace=e2e&queue=JOBS&limit=1')
DLQ_PREVIEW_AGAIN=$(admin_api 'http://127.0.0.1:8082/v1/queue/dead-letters?namespace=e2e&queue=JOBS&limit=1')
assert_eq "$DLQ_PREVIEW" "$DLQ_PREVIEW_AGAIN" "DLQ preview is immutable"
bun "$ROOT/scripts/runtime-operations-check.mjs" preview "$DLQ_PREVIEW"
DLQ_ID=$(bun -e 'console.log(JSON.parse(process.argv.at(-1)).messages[0].id)' "$DLQ_PREVIEW")
for repeat in 1 2; do admin_api -X POST "http://127.0.0.1:8082/v1/queue/dead-letters/ack?namespace=e2e&queue=JOBS&message_id=$DLQ_ID" >/dev/null; done
DLQ_EMPTY=$(admin_api 'http://127.0.0.1:8082/v1/queue/dead-letters?namespace=e2e&queue=JOBS')
bun "$ROOT/scripts/runtime-operations-check.mjs" empty "$DLQ_EMPTY"
# Preserve the existing replay journey with a new poison delivery.
fetch_path '/queue?id=poison' >/dev/null
retry_capture DLQ_STATUS compose exec -T cell-agent sh -c \
  "out=\$(cellhive queue status e2e JOBS); echo \"\$out\" | grep -E '\"dead_letter_depth\"[[:space:]]*:[[:space:]]*[1-9]' >/dev/null && printf '%s' \"\$out\"" \
  || fail "replacement poison message did not reach DLQ"
assert_eq ready "$(fetch_path /recover-queue)" "queue recovery toggle"
REPLAY=$(compose exec -T cell-agent cellhive queue replay-dlq e2e JOBS --limit 1)
[[ "$REPLAY" == *'"replayed": 1'* ]] || fail "DLQ replay did not move a message: $REPLAY"
POISON_RECEIPT=""
retry_capture POISON_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=poison' | grep -F '\"queue\":\"processed\"'" \
  || fail "replayed DLQ message was not processed"
[[ "$(compose exec -T cell-agent cellhive queue status e2e JOBS)" == *'"dead_letter_depth": 0'* ]] \
  || fail "DLQ retained replayed message"

WF_CREATE=$(fetch_path /workflow/create)
WF_ID=$(printf '%s' "$WF_CREATE" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$WF_ID" ]] || fail "workflow create returned no id: $WF_CREATE"
WF_STATUS=""
retry_capture WF_STATUS compose exec -T cell-agent sh -c \
  "out=\$(curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$WF_ID'); echo \"\$out\" | grep -F '\"status\":\"complete\"' >/dev/null && printf '%s' \"\$out\"" \
  || { echo "-- workflow status --" >&2; fetch_path "/workflow/status?id=$WF_ID" >&2 || true; echo "-- workflow logs --" >&2; compose logs --tail=150 cell-agent user-runtime >&2 || true; fail "workflow did not resume from sleep and complete"; }
[[ "$WF_STATUS" == *'"value":42'* ]] || fail "workflow output mismatch: $WF_STATUS"
[[ "$WF_STATUS" == *'"secret":"s3cr3t"'* ]] || fail "workflow env omitted managed secret: $WF_STATUS"
WF_INVOCATIONS=$(admin_api 'http://127.0.0.1:8082/v1/control/invocations?namespace=e2e&worker=api&kind=workflow&limit=100')
bun "$ROOT/scripts/runtime-operations-check.mjs" workflow "$WF_INVOCATIONS"

WF_BATCH=$(fetch_path /workflow/batch)
[[ "$WF_BATCH" == *'"id":"batch-a"'* && "$WF_BATCH" == *'"id":"batch-b"'* ]] \
  || fail "workflow createBatch response mismatch: $WF_BATCH"
for id in batch-a batch-b; do
  DONE=""
  retry_capture DONE compose exec -T cell-agent sh -c \
    "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$id' | grep -F '\"status\":\"complete\"'" \
    || fail "workflow batch instance $id did not complete"
done

echo "== workflow event wait without timeout and durable resume =="
WF_WAIT=$(fetch_path '/workflow/create?wait=1')
WF_WAIT_ID=$(printf '%s' "$WF_WAIT" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$WF_WAIT_ID" ]] || fail "event workflow returned no id"
WAITING=""
retry_capture WAITING compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$WF_WAIT_ID' | grep -F '\"status\":\"waiting\"'" \
  || { fetch_path "/workflow/status?id=$WF_WAIT_ID" >&2 || true; compose logs --tail=120 cell-agent user-runtime >&2 || true; fail "event workflow never parked"; }
assert_eq sent "$(fetch_path "/workflow/event?id=$WF_WAIT_ID")" "event send"
retry_capture WAITING compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$WF_WAIT_ID' | grep -F '\"status\":\"complete\"'" \
  || fail "event workflow remained blocked by its old lease"
[[ "$WAITING" == *'"value":77'* ]] || fail "workflow event payload lost: $WAITING"

echo "== DO alarm timer delivery, failure retry, rearm and consumption =="
assert_eq armed "$(fetch_path /do/alarm/arm)" "alarm arm"
ALARM_STATE=""
retry_capture ALARM_STATE compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' http://user-runtime:8081/do/alarm/state | grep -F '\"count\":3,\"due\":null'" \
  || { compose logs --tail=120 cell-agent do-runtime-gated >&2 || true; fail "alarm retry/rearm did not complete"; }

# Cron remains minute-aligned; the accelerated scheduler/timer polling only
# removes extra polling latency around the next real minute slot.
CRON_RECEIPT=""
retry_capture CRON_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=job-42' | grep -F '\"cronA\":\"ran\",\"cronB\":\"ran\"'" \
  || fail "cron scheduler did not invoke the tenant scheduled() handler"
[[ "$CRON_RECEIPT" == *'"duplicate":null'* ]] || fail "noRetry cron was retried: $CRON_RECEIPT"
[[ "$ENV_JSON" == *'"CELL_TOKEN":"user-cell-token"'* ]] || fail "user CELL_TOKEN is not visible"
[[ "$ENV_JSON" == *'"managedSecret":"s3cr3t"'* ]] || fail "managed secret is not visible in tenant worker env"
assert_not_contains "$ENV_JSON" "$HOST_URL_CANARY" "tenant env"
assert_not_contains "$ENV_JSON" "$CELLHIVE_ROOT_KEY" "tenant env"
KV_RESULT=$(fetch_path /kv)
assert_eq "kv-round-trip" "$KV_RESULT" "KV put/get"

echo "== native short TTL and queue idempotency/metadata =="
assert_eq armed "$(fetch_path /native/kv-short)" "native one-second TTL arm"
sleep 2
assert_eq null "$(fetch_path /native/kv-short-state)" "native one-second TTL expiry"
NATIVE_QUEUE=$(fetch_path /native/queue-idempotency)
[[ "$NATIVE_QUEUE" == *'"first":["'* && "$NATIVE_QUEUE" == *'"second":["'* ]] || fail "native queue sendBatch response: $NATIVE_QUEUE"
NATIVE_FIRST=$(printf '%s' "$NATIVE_QUEUE" | sed -n 's/.*"first":\["\([^"]*\)"\].*/\1/p')
NATIVE_SECOND=$(printf '%s' "$NATIVE_QUEUE" | sed -n 's/.*"second":\["\([^"]*\)"\].*/\1/p')
assert_eq "$NATIVE_FIRST" "$NATIVE_SECOND" "native queue idempotency"
NATIVE_RECEIPT=""
retry_capture NATIVE_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=native-idempotent' | grep -F '\"queue\":\"processed\"'" \
  || { fetch_path /receipt?id=native-idempotent >&2 || true; compose exec -T cell-agent cellhive queue status e2e JOBS >&2 || true; compose logs --tail=120 cell-agent user-runtime >&2 || true; fail "native idempotent queue was not consumed"; }
META_RECEIPT=""
retry_capture META_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=native-idempotent' | grep -F '\"queue\":\"processed\"'" \
  || fail "native queue metadata message missing"
META_JSON=$(fetch_path /receipt?id=native-idempotent)
[[ "$META_JSON" == *'"metadata":"{\"trace\":\"native-trace\",\"n\":\"9007199254740993\"}"'* ]] || fail "native queue metadata was not preserved: $META_JSON"

INTERNAL_TOKEN=$(compose exec -T cell-agent cellhive creds internal | tr -d '\r\n')
[[ -n "$INTERNAL_TOKEN" ]] || fail "could not derive host internal token"
assert_not_contains "$ENV_JSON" "$INTERNAL_TOKEN" "tenant env"

echo "== durable Queue/Cron invocation ID, outcomes and console query =="
fetch_path '/queue?id=invocation-fail' >/dev/null
INVOCATION_RECEIPT=""
retry_capture INVOCATION_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=invocation-fail' | grep -F '\"queue\":\"processed\"'" \
  || fail "failed queue invocation did not retry successfully"
INVOCATIONS=$(admin_api 'http://127.0.0.1:8082/v1/control/invocations?namespace=e2e&worker=api&limit=100')
INVOCATION_ID=$(bun "$ROOT/scripts/runtime-operations-check.mjs" invocations "$INVOCATIONS")
ONE_INVOCATION=$(admin_api "http://127.0.0.1:8082/v1/control/invocations?namespace=e2e&invocation_id=$INVOCATION_ID")
bun "$ROOT/scripts/runtime-operations-check.mjs" one "$ONE_INVOCATION" "$INVOCATION_ID"

echo "== destructive resource cleanup and independent DLQ preservation =="
compose exec -T cell-agent cellhive kv namespace create cleanup KV --scope cleanup/__kv__/physical >/dev/null
compose exec -T cell-agent cellhive queue create cleanup JOBS >/dev/null
compose exec -T cell-agent cellhive queue create cleanup DLQ >/dev/null
admin_api -X PUT --data 'kv-cleanup-payload' \
  'http://127.0.0.1:8082/client/v4/accounts/cleanup/storage/kv/namespaces/KV/values/secret' >/dev/null
assert_eq kv-cleanup-payload "$(admin_api 'http://127.0.0.1:8082/client/v4/accounts/cleanup/storage/kv/namespaces/KV/values/secret')" "KV cleanup seed"
# The Worker journey above proves scoped data-plane execution; internal transfer
# seeds delayed data for a separate resource without an attached consumer.
compose exec -T cell-agent curl -fsS -X POST -H "x-cellhive-internal-token: $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  --data '{"id":"cleanup-seed","body":"cGF5bG9hZA==","content_type":"text"}' \
  'http://127.0.0.1:7001/v1/internal/queue/transfer?ns=cleanup&queue=JOBS' >/dev/null
compose exec -T cell-agent curl -fsS -X POST -H "x-cellhive-internal-token: $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  --data '{"id":"dlq-seed","body":"a2VlcA==","content_type":"text"}' \
  'http://127.0.0.1:7001/v1/internal/queue/transfer?ns=cleanup&queue=DLQ' >/dev/null
compose exec -T cell-agent sh -c 'test -f /data/state/cells/cleanup/__kv__/physical.db && test -f /data/state/cells/cleanup/__queue__/JOBS.db' \
  || fail "cleanup resources were not backed by real SQLite data"
admin_api -X DELETE 'http://127.0.0.1:8082/v1/kv/resources?namespace=cleanup&name=KV' >/dev/null
admin_api -X DELETE 'http://127.0.0.1:8082/v1/queue/resources?namespace=cleanup&name=JOBS' >/dev/null
CLEANUP_STATE=""
for resource in kv:KV queue:JOBS; do
  kind=${resource%%:*}; name=${resource##*:}
  retry_capture CLEANUP_STATE compose exec -T cell-agent sh -c \
    "curl -fsS -H 'x-cellhive-admin-token: $ADMIN_TOKEN' 'http://127.0.0.1:8082/v1/control/resource/cleanup?namespace=cleanup&kind=$kind&name=$name' | grep -F '\"state\":\"complete\"'" \
    || { compose logs --tail=100 cell-agent >&2 || true; fail "resource cleanup did not complete: $resource"; }
done
compose exec -T cell-agent sh -c 'test ! -e /data/state/cells/cleanup/__kv__/physical.db && test ! -e /data/state/cells/cleanup/__queue__/JOBS.db && test -z "$(find /data/bucket/cells/cleanup/__kv__/physical/ltx /data/bucket/cells/cleanup/__queue__/JOBS/ltx -type f -print 2>/dev/null)"' \
  || fail "completed cleanup retained local SQLite or authoritative replica payloads"
PRESERVED_DLQ=$(admin_api 'http://127.0.0.1:8082/v1/queue/dead-letters?namespace=cleanup&queue=DLQ&direct=1')
bun "$ROOT/scripts/runtime-operations-check.mjs" preserved "$PRESERVED_DLQ"
DELETED_TRANSFER_CODE=$(compose exec -T cell-agent curl -sS -o /dev/null -w '%{http_code}' -X POST -H "x-cellhive-internal-token: $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  --data '{"id":"late","body":"eA==","content_type":"text"}' 'http://127.0.0.1:7001/v1/internal/queue/transfer?ns=cleanup&queue=JOBS')
assert_eq 410 "$DELETED_TRANSFER_CODE" "deleted queue rejects late writes"

echo "== gated DO durability and restart =="
DO1=$(fetch_path /do)
assert_eq '{"count":1,"secret":"s3cr3t"}' "$DO1" "first gated DO increment with managed secret"
WS1=""
retry_capture WS1 websocket_roundtrip before-restart 2 \
  || fail "public DO WebSocket failed before runtime restart"
assert_eq '{"echo":"before-restart","count":2}' "$WS1" "public DO WebSocket before restart"
LTX_PROOF=$(compose exec -T cell-agent sh -c "find /data/bucket/cells -type f -name '*.ltx' -print -quit" 2>/dev/null || true)
[[ -n "$LTX_PROOF" ]] || fail "gated DO produced no authoritative bucket LTX object"
assert_eq armed "$(fetch_path /do/alarm/arm)" "alarm armed before runtime restart"
compose restart do-runtime-gated >/dev/null
retry_capture READY compose exec -T cell-agent curl -fsS http://do-runtime-gated:8788/ready \
  || fail "gated do-runtime did not recover after restart"
WS2=""
retry_capture WS2 websocket_roundtrip after-restart 3 \
  || fail "public DO WebSocket failed after runtime restart"
assert_eq '{"echo":"after-restart","count":3}' "$WS2" "public DO WebSocket after restart"
DO2=""
retry_capture DO2 fetch_path /do || fail "DO invoke failed after runtime restart"
assert_eq '{"count":4,"secret":"s3cr3t"}' "$DO2" "gated DO count and managed secret after restart"
retry_capture ALARM_STATE compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' http://user-runtime:8081/do/alarm/state | grep -F '\"count\":4,\"due\":null'" \
  || fail "pending alarm did not survive gated runtime restart"

WF_PIN=$(fetch_path '/workflow/create?wait=1')
WF_PIN_ID=$(printf '%s' "$WF_PIN" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$WF_PIN_ID" ]] || fail "pinned workflow create returned no id: $WF_PIN"
PIN_WAITING=""
retry_capture PIN_WAITING compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$WF_PIN_ID' | grep -F '\"status\":\"waiting\"'" \
  || fail "pinned workflow did not durably park before publish and crash"

echo "== secret rotation publishes a live version and rollback restores it =="
INITIAL_RELEASES=$(compose exec -T cell-agent cellhive releases e2e api)
[[ "$INITIAL_RELEASES" == *'"version": 2'* ]] || fail "initial secret publish did not create v2: $INITIAL_RELEASES"
compose exec -T cell-agent cellhive secret put e2e api MANAGED_SECRET --value cjB0YXRlZA== >/dev/null
ROTATED_RELEASES=$(compose exec -T cell-agent cellhive releases e2e api)
[[ "$ROTATED_RELEASES" == *'"version": 3'* ]] || fail "secret rotation did not create v3: $ROTATED_RELEASES"
ROTATED_ENV=""
retry_capture ROTATED_ENV compose exec -T cell-agent sh -c \
  "out=\$(curl -fsS -H 'Host: e2e-api.cell.test' http://user-runtime:8081/env); echo \"\$out\" | grep -F '\"managedSecret\":\"r0tated\"'" \
  || fail "public worker did not receive rotated secret"
SECRET_ROLLBACK=$(compose exec -T cell-agent cellhive rollback e2e api --idempotency-key secret-rollback-v2)
[[ "$SECRET_ROLLBACK" == *'"active": 2'* ]] || fail "secret rollback did not activate v2: $SECRET_ROLLBACK"
SECRET_ROLLBACK_RETRY=$(compose exec -T cell-agent cellhive rollback e2e api --idempotency-key secret-rollback-v2)
[[ "$SECRET_ROLLBACK_RETRY" == *'"active": 2'* ]] || fail "retried rollback unexpectedly toggled active: $SECRET_ROLLBACK_RETRY"
RESTORED_ENV=""
retry_capture RESTORED_ENV compose exec -T cell-agent sh -c \
  "out=\$(curl -fsS -H 'Host: e2e-api.cell.test' http://user-runtime:8081/env); echo \"\$out\" | grep -F '\"managedSecret\":\"s3cr3t\"'" \
  || fail "public worker did not receive restored secret"
RELEASES_AFTER_ROLLBACK=$(compose exec -T cell-agent cellhive releases e2e api)
[[ "$RELEASES_AFTER_ROLLBACK" != *'"version": 4'* ]] || fail "rollback unexpectedly created v4: $RELEASES_AFTER_ROLLBACK"
echo "secret rotation: v2=s3cr3t, v3=r0tated, rollback=v2=s3cr3t; no v4"

echo "== queue claim crash, lease expiry, and active-version migration =="
fetch_path '/queue?id=version-probe' >/dev/null
LEASED=""
retry_capture LEASED compose exec -T cell-agent sh -c \
  "out=\$(cellhive queue status e2e JOBS); echo \"\$out\" | grep -E '\"leased\"[[:space:]]*:[[:space:]]*[1-9]' >/dev/null && printf '%s' \"\$out\"" \
  || fail "version-probe was not durably claimed before crash"
compose kill -s SIGKILL cell-agent >/dev/null
compose up -d --no-build cell-agent >/dev/null
CRON_RESTART_FLOOR=$(( ($(date +%s) / 60 + 1) * 60000 ))
retry_capture READY compose exec -T cell-agent curl -fsS http://127.0.0.1:7001/readyz \
  || fail "cell-agent did not recover after SIGKILL"
# /readyz only proves the listener is up. The control cell may still be
# reclaiming and opening its authoritative state after an unclean exit.
RECOVERED_RESOURCES=""
for _ in $(seq 1 60); do
  RECOVERED_RESOURCES=$(compose exec -T cell-agent cellhive resource list e2e 2>/dev/null || true)
  if [[ "$RECOVERED_RESOURCES" == *'"name": "KV"'* &&
        "$RECOVERED_RESOURCES" == *'"name": "JOBS"'* &&
        "$RECOVERED_RESOURCES" == *'"name": "REPORTS"'* ]]; then break; fi
  sleep 1
done
[[ "$RECOVERED_RESOURCES" == *'"name": "KV"'* &&
   "$RECOVERED_RESOURCES" == *'"name": "JOBS"'* &&
   "$RECOVERED_RESOURCES" == *'"name": "REPORTS"'* ]] \
  || fail "registered resources did not recover after SIGKILL: $RECOVERED_RESOURCES"
VERSION_DEPLOY=$(compose exec -T -w /tmp/runtime-baseline-app cell-agent \
  cellhive deploy e2e - --config wrangler.jsonc --var RELEASE_TAG=v2)
[[ "$VERSION_DEPLOY" == *'"number": 4'* ]] || fail "post-crash publish did not create version 4: $VERSION_DEPLOY"
VERSION_RECEIPT=""
retry_capture VERSION_RECEIPT compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/receipt?id=version-probe' | grep -F '\"queue\":\"processed:v2\"'" \
  || { compose exec -T cell-agent cellhive queue status e2e JOBS >&2 || true; compose logs --tail=100 cell-agent user-runtime >&2 || true; fail "leased message was not redelivered to active v2"; }
VERSION_INVOCATIONS=$(admin_api 'http://127.0.0.1:8082/v1/control/invocations?namespace=e2e&worker=api&limit=100')
[[ "$VERSION_INVOCATIONS" == *'version-probe'* ]] || fail "version-probe invocation was not observable"
# Both expressions must materialize a new minute after the process restart.
# An earlier receipt cannot satisfy this probe, and the noRetry expression
# must still have one execution per slot. Wait after Queue crash recovery so
# this minute-aligned check does not exhaust the Queue probe's retry budget.
CRON_AFTER_RESTART=""
for _ in $(seq 1 150); do
  CRON_AFTER_RESTART=$(fetch_path '/receipt?id=job-42' 2>/dev/null || true)
  if [[ -n "$CRON_AFTER_RESTART" ]] && python3 - "$CRON_AFTER_RESTART" "$CRON_RESTART_FLOOR" <<'PY'
import json,sys
try:
    r=json.loads(sys.argv[1]); floor=int(sys.argv[2])
    ok=int(r.get('cronASlot') or 0)>=floor and int(r.get('cronBSlot') or 0)>=floor and r.get('duplicate') is None
except (ValueError, TypeError, json.JSONDecodeError): ok=False
sys.exit(0 if ok else 1)
PY
  then break; fi
  sleep 1
done
python3 - "$CRON_AFTER_RESTART" "$CRON_RESTART_FLOOR" <<'PY' || fail "cron did not materialize both new slots after SIGKILL/restart: $CRON_AFTER_RESTART"
import json,sys
r=json.loads(sys.argv[1]); floor=int(sys.argv[2])
assert int(r.get('cronASlot') or 0)>=floor and int(r.get('cronBSlot') or 0)>=floor and r.get('duplicate') is None
PY
echo "cron after cell-agent SIGKILL/restart: both expressions advanced to a new slot without duplicate"
assert_eq sent "$(fetch_path "/workflow/event?id=$WF_PIN_ID")" "post-crash workflow wake"
WF_PIN_DONE=""
retry_capture WF_PIN_DONE compose exec -T cell-agent sh -c \
  "curl -fsS -H 'Host: e2e-api.cell.test' 'http://user-runtime:8081/workflow/status?id=$WF_PIN_ID' | grep -F '\"status\":\"complete\"'" \
  || fail "pinned workflow did not resume after agent SIGKILL and version 4 publish"
[[ "$WF_PIN_DONE" == *'"value":77'* && "$WF_PIN_DONE" == *'"releaseTag":"original"'* && "$WF_PIN_DONE" == *'"secret":"s3cr3t"'* ]] \
  || fail "workflow resumed with wrong pinned version or output: $WF_PIN_DONE"

echo "== worker deletion =="
compose exec -T cell-agent cellhive worker delete e2e api >/dev/null
DELETED=""
retry_capture DELETED compose exec -T cell-agent sh -c \
  "code=\$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: e2e-api.cell.test' http://user-runtime:8081/); [ \"\$code\" = 404 ] && printf '%s' \"\$code\"" \
  || fail "deleted worker route continued serving traffic"

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
