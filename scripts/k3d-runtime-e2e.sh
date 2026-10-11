#!/usr/bin/env bash
# Real multi-Pod Kubernetes acceptance: gated DO ownership, cold takeover, and
# rolling availability. Set CELLHIVE_K3D_AGENTS to add worker nodes. Owns and
# removes its k3d cluster and temporary files.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

CLUSTER=${CELLHIVE_K3D_CLUSTER:-cellhive-e2e-$PPID-$$}
IMAGE=${CELLHIVE_K3D_IMAGE:-cellhive:k3d-e2e}
K3D_AGENTS=${CELLHIVE_K3D_AGENTS:-0}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-k3d-e2e.XXXXXX")
export CELLHIVE_ROOT_KEY=${CELLHIVE_ROOT_KEY:-6b33642d72756e74696d652d6532652d726f6f742d6b65792d3230323630393235}

fail() {
  echo "K3D-RUNTIME-E2E: FAIL: $*" >&2
  kubectl -n cellhive get pods -o wide >&2 || true
  kubectl -n cellhive logs statefulset/cell-agent --tail=100 >&2 || true
  kubectl -n cellhive logs deployment/user-runtime --all-pods=true --tail=100 >&2 || true
  kubectl -n cellhive logs deployment/do-runtime-gated --all-pods=true --tail=150 >&2 || true
  exit 1
}

cleanup() {
  k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT

for tool in docker k3d kubectl jq node; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

retry() {
  local attempts=$1
  shift
  for ((i=1; i<=attempts; i++)); do
    if "$@"; then return 0; fi
    sleep 1
  done
  return 1
}

retry_capture() {
  local __var=$1 attempts=$2
  shift 2
  local out
  for ((i=1; i<=attempts; i++)); do
    if out=$("$@"); then
      printf -v "$__var" '%s' "$out"
      return 0
    fi
    sleep 1
  done
  printf -v "$__var" '%s' "$out"
  return 1
}

public_get() {
  local path=$1
  kubectl -n cellhive exec cell-agent-0 -- curl --fail-with-body -sS -H 'Host: e2e-api.cell.test' "http://user-runtime:8081${path}"
}

echo "== build and load image =="
docker build -f deploy/Dockerfile -t "$IMAGE" .
k3d cluster create "$CLUSTER" --servers 1 --agents "$K3D_AGENTS" --wait
k3d image import -c "$CLUSTER" "$IMAGE"

kubectl create namespace cellhive
kubectl -n cellhive create secret generic cellhive-secrets --from-literal=CELLHIVE_ROOT_KEY="$CELLHIVE_ROOT_KEY"
kubectl apply -k deploy/k8s/overlays/rpo0
kubectl -n cellhive set image statefulset/cell-agent cell-agent="$IMAGE"
kubectl -n cellhive set image deployment/user-runtime user-runtime="$IMAGE"
kubectl -n cellhive set image deployment/do-runtime-gated do-runtime-gated="$IMAGE"
kubectl -n cellhive rollout status statefulset/cell-agent --timeout=180s
kubectl -n cellhive rollout status deployment/user-runtime --timeout=180s
kubectl -n cellhive rollout status deployment/do-runtime-gated --timeout=180s

[[ $(kubectl -n cellhive get deployment do-runtime-gated -o jsonpath='{.status.readyReplicas}') == 2 ]] \
  || fail "gated runtime did not reach two ready Pods"

mkdir -p "$TMP/app/src"
cat >"$TMP/app/wrangler.jsonc" <<'JSON'
{
  "name": "api",
  "main": "src/index.ts",
  "compatibility_date": "2026-09-23",
  "durable_objects": { "bindings": [{ "name": "COUNTER", "class_name": "Counter" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
JSON
cat >"$TMP/app/src/index.ts" <<'TS'
import { DurableObject } from "cloudflare:workers";
export class Counter extends DurableObject {
  increment(): number {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("INSERT INTO counter(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=n+1");
    return Number([...this.ctx.storage.sql.exec("SELECT n FROM counter WHERE id=1")][0]?.n || 0);
  }
  async fetch(): Promise<Response> { return Response.json({ count: this.increment() }); }
}
export default {
  async fetch(req: Request, env: Record<string, any>): Promise<Response> {
    if (new URL(req.url).pathname === "/do") return env.COUNTER.getByName("counter-1").fetch("https://counter.invalid/");
    return new Response("ok");
  }
};
TS

kubectl -n cellhive exec cell-agent-0 -- cellhive app create e2e >/dev/null
kubectl -n cellhive exec cell-agent-0 -- cellhive domain add e2e e2e-api.cell.test >/dev/null
kubectl -n cellhive cp "$TMP/app" cell-agent-0:/tmp/k3d-e2e-app
kubectl -n cellhive exec cell-agent-0 -- sh -c 'cd /tmp/k3d-e2e-app && cellhive deploy e2e api --config wrangler.jsonc --route e2e-api.cell.test' >/dev/null
retry 60 public_get / >/dev/null || fail "deployed Worker route did not become live"

echo "== owner routing and cold takeover =="
FIRST=$(public_get /do)
[[ $FIRST == '{"count":1}' ]] || fail "first DO response: $FIRST"
SHARD=$(node -e 'let h=0x811c9dc5; for(const c of "e2e/api/Counter/counter-1"){h^=c.charCodeAt(0); h=Math.imul(h,0x01000193)>>>0} process.stdout.write(String(h%16))')
OWNER_JSON=$(kubectl -n cellhive exec cell-agent-0 -- cat "/data/bucket/cells/e2e/__do__/api~Counter~shard${SHARD}/owner.json")
OWNER_ADDR=$(jq -r .address <<<"$OWNER_JSON")
OWNER_IP=${OWNER_ADDR%:*}
OWNER_POD=$(kubectl -n cellhive get pods -l app=do-runtime-gated -o json | jq -r --arg ip "$OWNER_IP" '.items[] | select(.status.podIP==$ip) | .metadata.name')
[[ -n $OWNER_POD ]] || fail "owner address $OWNER_ADDR is not a gated runtime Pod"

kubectl -n cellhive delete pod "$OWNER_POD" --wait=true --timeout=90s
kubectl -n cellhive rollout status deployment/do-runtime-gated --timeout=180s
SECOND=""
retry_capture SECOND 60 public_get /do || fail "DO did not recover after owner replacement: $SECOND"
[[ $SECOND == '{"count":2}' ]] || fail "DO state did not survive owner replacement: $SECOND"

echo "== rolling DO availability =="
kubectl -n cellhive exec cell-agent-0 -- sh -c '
  i=0
  while [ "$i" -lt 120 ]; do
    out=$(curl --retry 60 --retry-delay 0 --retry-all-errors --fail-with-body -sS \
      -H "Host: e2e-api.cell.test" http://user-runtime:8081/do) || exit 1
    echo "$out" | grep -Eq "\"count\":[0-9]+" || exit 1
    i=$((i+1))
    sleep 0.1
  done
' &
TRAFFIC_PID=$!
kubectl -n cellhive rollout restart deployment/do-runtime-gated
kubectl -n cellhive rollout status deployment/do-runtime-gated --timeout=240s
wait "$TRAFFIC_PID" || fail "public DO requests failed during gated runtime rollout"
THIRD=$(public_get /do)
[[ $THIRD == '{"count":123}' ]] || fail "DO state did not remain linear through rolling update: $THIRD"

echo "K3D-RUNTIME-E2E: PASS owner=$OWNER_POD shard=$SHARD counts=1,2,123"
