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
K3D_FLEET=${CELLHIVE_K3D_FLEET:-0}
K3D_FLEET_FAULT=${CELLHIVE_K3D_FLEET_FAULT:-0}
K3D_ABC=${CELLHIVE_K3D_ABC:-0}
[[ $K3D_FLEET_FAULT != 1 || $K3D_FLEET == 1 ]] || { echo "fleet fault test requires CELLHIVE_K3D_FLEET=1" >&2; exit 1; }
[[ $K3D_ABC != 1 || $K3D_FLEET == 1 ]] || { echo "A-B-C test requires CELLHIVE_K3D_FLEET=1" >&2; exit 1; }
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-k3d-e2e.XXXXXX")
export CELLHIVE_ROOT_KEY=${CELLHIVE_ROOT_KEY:-6b33642d72756e74696d652d6532652d726f6f742d6b65792d3230323630393235}

fail() {
  echo "K3D-RUNTIME-E2E: FAIL: $*" >&2
  kubectl -n cellhive get pods -o wide >&2 || true
  kubectl -n cellhive logs statefulset/cell-agent --tail=100 >&2 || true
  kubectl -n cellhive logs deployment/user-runtime --all-pods=true --tail=100 >&2 || true
  kubectl -n cellhive logs statefulset/do-runtime-gated --all-pods=true --tail=150 >&2 || true
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
docker tag "$IMAGE" cellhive:dev
if [[ $K3D_FLEET == 1 ]]; then
  mkdir -p "$TMP/shared-bucket"
  chmod 0777 "$TMP/shared-bucket"
  k3d cluster create "$CLUSTER" --servers 1 --agents "$K3D_AGENTS" \
    --volume "$TMP/shared-bucket:/var/lib/cellhive-e2e-bucket@server:0" --wait
else
  k3d cluster create "$CLUSTER" --servers 1 --agents "$K3D_AGENTS" --wait
fi
# Keep every kubectl call pinned to this test cluster even if another process
# changes the machine-wide current context while the test is running.
k3d kubeconfig get "$CLUSTER" > "$TMP/kubeconfig"
export KUBECONFIG="$TMP/kubeconfig"
k3d image import -c "$CLUSTER" "$IMAGE" cellhive:dev

kubectl create namespace cellhive
kubectl -n cellhive create secret generic cellhive-secrets --from-literal=CELLHIVE_ROOT_KEY="$CELLHIVE_ROOT_KEY"
kubectl apply -k deploy/k8s/overlays/rpo0
EXPECTED_DO_REPLICAS=2
if [[ $K3D_ABC == 1 ]]; then
  EXPECTED_DO_REPLICAS=3
  kubectl -n cellhive patch statefulset do-runtime-gated --type=merge -p '{"spec":{"replicas":3}}'
fi
if [[ $K3D_FLEET == 1 ]]; then
  # The test's two cell-agent PVCs remain private; only their FS bucket is
  # shared through the one local k3d node. This exercises real follower fsync.
  kubectl -n cellhive patch statefulset cell-agent --type=json -p '[
    {"op":"replace","path":"/spec/replicas","value":2},
    {"op":"add","path":"/spec/template/spec/volumes","value":[{"name":"shared-bucket","hostPath":{"path":"/var/lib/cellhive-e2e-bucket","type":"Directory"}}]},
    {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"shared-bucket","mountPath":"/data/bucket"}}
  ]'
fi
kubectl -n cellhive set image statefulset/cell-agent cell-agent="$IMAGE"
kubectl -n cellhive set image deployment/user-runtime user-runtime="$IMAGE"
kubectl -n cellhive set image statefulset/do-runtime-gated do-runtime-gated="$IMAGE"
# OrderedReady may keep ordinal 0 on the manifest's dev image while waiting
# for ordinal 1. Wait until the controller observes the new revision before
# replacing the old ordinal; deleting earlier can recreate the dev image.
for ((i=1; i<=30; i++)); do
  UPDATE_REV=$(kubectl -n cellhive get statefulset do-runtime-gated -o jsonpath='{.status.updateRevision}')
  POD_REV=$(kubectl -n cellhive get pod do-runtime-gated-0 -o jsonpath='{.metadata.labels.controller-revision-hash}' 2>/dev/null || true)
  [[ -n $UPDATE_REV && $UPDATE_REV != "$POD_REV" ]] && break
  sleep 1
done
for ((i=1; i<=4; i++)); do
  POD_IMAGE=$(kubectl -n cellhive get pod do-runtime-gated-0 -o jsonpath='{.spec.containers[0].image}' 2>/dev/null || true)
  [[ $POD_IMAGE == "$IMAGE" ]] && break
  kubectl -n cellhive delete pod do-runtime-gated-0 --wait=true --timeout=90s >/dev/null
  retry 30 kubectl -n cellhive get pod do-runtime-gated-0 >/dev/null || fail "gated ordinal 0 was not recreated"
done
[[ $(kubectl -n cellhive get pod do-runtime-gated-0 -o jsonpath='{.spec.containers[0].image}') == "$IMAGE" ]] || fail "gated ordinal 0 kept the dev image"
kubectl -n cellhive rollout status statefulset/cell-agent --timeout=180s
if [[ $K3D_FLEET == 1 ]]; then
  [[ $(kubectl -n cellhive get statefulset cell-agent -o jsonpath='{.status.readyReplicas}') == 2 ]] \
    || fail "fleet did not reach two ready cell-agent Pods"
fi
kubectl -n cellhive rollout status deployment/user-runtime --timeout=180s
kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=180s

[[ $(kubectl -n cellhive get statefulset do-runtime-gated -o jsonpath='{.status.readyReplicas}') == "$EXPECTED_DO_REPLICAS" ]] \
  || fail "gated runtime did not reach $EXPECTED_DO_REPLICAS ready Pods"

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
INTERNAL_TOKEN=$(node -e 'const c=require("crypto"); const k=Buffer.from(process.env.CELLHIVE_ROOT_KEY,"hex"); const p=c.createHmac("sha256",Buffer.alloc(32)).update(k).digest(); process.stdout.write(c.createHmac("sha256",p).update(Buffer.concat([Buffer.from("cellhive/token/internal"),Buffer.from([1])])).digest("base64url"))')
VIEW=$(kubectl -n cellhive exec cell-agent-0 -- curl --fail-with-body -sS -H "x-cellhive-internal-token: $INTERNAL_TOKEN" 'http://127.0.0.1:7001/v1/control/worker?ns=e2e&worker=api')
BUNDLE_SHA=$(jq -r .bundle_sha <<<"$VIEW")
STORAGE_ID=$(jq -r .storage_id <<<"$VIEW")
VERSION=$(jq -r .version <<<"$VIEW")
[[ -n $BUNDLE_SHA && -n $STORAGE_ID && $VERSION =~ ^[0-9]+$ ]] || fail "missing deployed DO identity"
POD_A=do-runtime-gated-0
POD_B=do-runtime-gated-1
POD_C=do-runtime-gated-2
invoke_pod() {
  local pod=$1 ip body
  ip=$(kubectl -n cellhive get pod "$pod" -o jsonpath='{.status.podIP}')
  body=$(jq -nc --arg sha "$BUNDLE_SHA" --arg storage "$STORAGE_ID" --argjson version "$VERSION" \
    '{namespace:"e2e",worker:"api",bundle_sha:$sha,version:$version,storage_id:$storage,class:"Counter",storage_class:"Counter",id:"counter-1",request:{method:"GET",path:"/"}}')
  kubectl -n cellhive exec cell-agent-0 -- curl --fail-with-body -sS --max-time 20 \
    -H "x-cellhive-internal-token: $INTERNAL_TOKEN" -H 'content-type: application/json' \
    -d "$body" "http://$ip:8788/v1/do/invoke"
}
assert_owner() {
  local pod=$1 address ip
  address=$(kubectl -n cellhive exec cell-agent-0 -- cat "/data/bucket/cells/e2e/__do__/api~Counter~shard${SHARD}/owner.json" | jq -r .address)
  ip=$(kubectl -n cellhive get pod "$pod" -o jsonpath='{.status.podIP}')
  [[ $address == "$ip:8788" ]] || fail "owner address $address, expected $pod at $ip"
}
SHARD=$(node -e 'let h=0x811c9dc5; for(const c of "e2e/api/Counter/counter-1"){h^=c.charCodeAt(0); h=Math.imul(h,0x01000193)>>>0} process.stdout.write(String(h%16))')
FIRST=""
retry_capture FIRST 60 invoke_pod "$POD_A" || fail "A initial invoke failed: $FIRST"
[[ $FIRST == '{"count":1}' ]] || fail "A first DO response: $FIRST"
if [[ $K3D_FLEET == 1 ]]; then
  POINTER_COUNT=$(kubectl -n cellhive exec cell-agent-0 -- sh -c 'find /data/bucket/cells -name do-recovery.json | wc -l')
  [[ $POINTER_COUNT -gt 0 ]] || fail "DO fleet write did not establish a recovery pointer"
  FOLLOWER_DO_LOGS=$(kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/state/peer-spool/_sessions -name fragments.log -exec grep -al "workerd/" {} + | wc -l')
  [[ $FOLLOWER_DO_LOGS -gt 0 ]] || fail "DO fleet write did not reach the follower spool"
fi
assert_owner "$POD_A"
PVC_A=$(kubectl -n cellhive get pvc "data-$POD_A" -o jsonpath='{.spec.volumeName}')
kubectl -n cellhive delete pod "$POD_A" --wait=true --timeout=90s
kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=180s
SECOND=""
retry_capture SECOND 60 invoke_pod "$POD_B" || fail "B did not recover after A replacement: $SECOND"
[[ $SECOND == '{"count":2}' ]] || fail "DO state did not survive owner replacement: $SECOND"
assert_owner "$POD_B"

if [[ $K3D_ABC == 1 ]]; then
  echo "== A -> B -> C with B's follower-only write =="
  sleep 2
  BEFORE_LTX=$(kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type f -path "*/ltx/*" | wc -l')
  kubectl -n cellhive exec cell-agent-0 -- sh -c 'find /data/bucket/cells/workerd -type d \( -name ltx -o -path "*/ltx/*" \) -exec chmod a-w {} +'
  THIRD=""
  retry_capture THIRD 10 invoke_pod "$POD_B" || fail "B's follower-only write was not acknowledged: $THIRD"
  [[ $THIRD == '{"count":3}' ]] || fail "B's blocked-COS write returned $THIRD"
  sleep 1
  AFTER_LTX=$(kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type f -path "*/ltx/*" | wc -l')
  [[ $AFTER_LTX == "$BEFORE_LTX" ]] || fail "B's write reached COS before takeover ($BEFORE_LTX->$AFTER_LTX)"
  kubectl -n cellhive delete pod "$POD_B" --grace-period=0 --force --wait=true >/dev/null
  kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type d \( -name ltx -o -path "*/ltx/*" \) -exec chmod a+w {} +'
  kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=180s
  FOURTH=""
  retry_capture FOURTH 90 invoke_pod "$POD_C" || fail "C did not recover B's follower-only write: $FOURTH"
  [[ $FOURTH == '{"count":4}' ]] || fail "B's acknowledged write was absent on C: $FOURTH"
  assert_owner "$POD_C"
  echo "K3D-RUNTIME-E2E: PASS owners=$POD_A,$POD_B,$POD_C counts=1,2,3,4 follower_only=B pvc_A=$PVC_A"
  exit 0
fi

# The successor's acknowledged write must be recoverable by the original Pod
# while its PVC still contains the earlier local value.
[[ $(kubectl -n cellhive get pvc "data-$POD_A" -o jsonpath='{.spec.volumeName}') == "$PVC_A" ]] || fail "A PVC changed across Pod restart"
kubectl -n cellhive delete pod "$POD_B" --wait=true --timeout=90s
kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=180s
THIRD=""
retry_capture THIRD 60 invoke_pod "$POD_A" || fail "A did not recover after B replacement: $THIRD"
[[ $THIRD == '{"count":3}' ]] || fail "B's acknowledged write was absent on A return: $THIRD"
assert_owner "$POD_A"

EXPECTED_FINAL=124
if [[ $K3D_FLEET_FAULT == 1 ]]; then
  echo "== follower-confirmed DO write before COS upload =="
  sleep 2
  BEFORE_LTX=$(kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type f -path "*/ltx/*" | wc -l')
  kubectl -n cellhive exec cell-agent-0 -- sh -c 'find /data/bucket/cells/workerd -type d \( -name ltx -o -path "*/ltx/*" \) -exec chmod a-w {} +'
  FOURTH=""
  retry_capture FOURTH 10 invoke_pod "$POD_A" || fail "follower did not acknowledge while COS LTX was blocked: $FOURTH"
  [[ $FOURTH == '{"count":4}' ]] || fail "blocked-COS write returned $FOURTH"
  sleep 1
  AFTER_LTX=$(kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type f -path "*/ltx/*" | wc -l')
  [[ $AFTER_LTX == "$BEFORE_LTX" ]] || fail "DO LTX reached COS before forced takeover ($BEFORE_LTX->$AFTER_LTX)"
  kubectl -n cellhive delete pod "$POD_A" --grace-period=0 --force --wait=true >/dev/null
  kubectl -n cellhive exec cell-agent-1 -- sh -c 'find /data/bucket/cells/workerd -type d \( -name ltx -o -path "*/ltx/*" \) -exec chmod a+w {} +'
  kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=180s
  FIFTH=""
  retry_capture FIFTH 90 invoke_pod "$POD_B" || fail "B did not recover follower-only write: $FIFTH"
  [[ $FIFTH == '{"count":5}' ]] || fail "follower-only acknowledged write was lost: $FIFTH"
  assert_owner "$POD_B"
  EXPECTED_FINAL=126
fi

echo "== rolling DO availability =="
kubectl -n cellhive exec cell-agent-0 -- sh -c '
  i=0
  previous=0
  while [ "$i" -lt 120 ]; do
    out=$(curl --retry 60 --retry-delay 0 --retry-all-errors --fail-with-body -sS \
      -H "Host: e2e-api.cell.test" http://user-runtime:8081/do) || exit 1
    count=$(echo "$out" | sed -n "s/.*\"count\":\([0-9][0-9]*\).*/\1/p")
    [ -n "$count" ] && [ "$count" -gt "$previous" ] || exit 1
    previous=$count
    i=$((i+1))
    sleep 0.1
  done
' &
TRAFFIC_PID=$!
kubectl -n cellhive rollout restart statefulset/do-runtime-gated
kubectl -n cellhive rollout status statefulset/do-runtime-gated --timeout=240s
wait "$TRAFFIC_PID" || fail "public DO requests failed during gated runtime rollout"
FINAL=$(public_get /do)
FINAL_COUNT=$(printf '%s' "$FINAL" | sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p')
[[ -n $FINAL_COUNT && $FINAL_COUNT -ge $EXPECTED_FINAL ]] || fail "DO state did not remain linear through rolling update: $FINAL"

echo "K3D-RUNTIME-E2E: PASS owners=$POD_A,$POD_B,$POD_A shard=$SHARD counts=1,2,3,$FINAL_COUNT pvc=$PVC_A"
