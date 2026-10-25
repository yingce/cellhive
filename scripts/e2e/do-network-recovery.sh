#!/usr/bin/env bash
# Isolated actual DO runtime: an outstanding log-capture request must not pin
# the old cell-agent address after DNS changes. No production volumes/secrets.
set -euo pipefail
cd "$(dirname "$0")/../.."
image=${CELLHIVE_E2E_IMAGE:-cellhive:dev}
out=${CELLHIVE_E2E_OUTPUT:-$(mktemp -d /tmp/cellhive-do-network.XXXXXX)}
mkdir -p "$out"
out=$(realpath "$out")
prefix=cellhive-do-network-$$
net=$prefix
old=$prefix-old
new=$prefix-new
runtime=$prefix-runtime
cleanup() {
  docker logs "$runtime" > "$out/runtime.log" 2>&1 || true
  docker rm -f "$runtime" "$old" "$new" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT
CGO_ENABLED=0 go build -o "$out/backend" ./scripts/e2e/fixtures/do-network-backend
docker network create "$net" >/dev/null
docker run -d --name "$old" --network "$net" -e HANG_POLL=1 -e NAME=old -v "$out:/probe:ro" --entrypoint /probe/backend "$image" >/dev/null
docker run -d --name "$new" --network "$net" -e NAME=new -v "$out:/probe:ro" --entrypoint /probe/backend "$image" >/dev/null
old_ip=$(docker inspect "$old" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
new_ip=$(docker inspect "$new" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
mounts=()
if [[ ${CELLHIVE_E2E_USE_IMAGE_JS:-0} != 1 ]]; then mounts=(-v "${CELLHIVE_E2E_WORKERD_SOURCE:-$PWD/workerd}:/app/workerd:ro"); fi
docker run -d --name "$runtime" --network "$net" --add-host "recovery-cell:$old_ip" \
  -p 127.0.0.1::8788 "${mounts[@]}" \
  -e CELLHIVE_ROOT_KEY=isolated-do-recovery-test-only \
  -e CELLHIVE_CELL_URL=http://recovery-cell:7001 \
  -e CELLHIVE_DATA_DIR=/tmp/state -e CELLHIVE_RUNTIME_DIR=/tmp/runtime \
  --entrypoint /usr/local/bin/do-runtime "$image" >/dev/null
addr=$(docker port "$runtime" 8788/tcp)
ready="http://$addr/ready"
for ((i=0;i<50;i++)); do
  if curl -fsS --max-time 2 "$ready" > "$out/before.json" 2>/dev/null; then break; fi
  sleep 0.1
done
grep -q '"ready":true' "$out/before.json"
docker exec "$runtime" curl -fsS --max-time 3 "http://$old_ip:7001/stats" > "$out/old-stats.json"
grep -Eq '"polls":[1-9]' "$out/old-stats.json"
docker exec "$runtime" curl -fsS --max-time 3 "http://$old_ip:7001/hang" >/dev/null
# Keep old sockets open and silent; only the authoritative hostname changes.
docker exec -u 0 "$runtime" sh -c 'awk -v ip="$1" '\''$2=="recovery-cell" {$1=ip} {print}'\'' /etc/hosts > /tmp/recovery-hosts; cat /tmp/recovery-hosts > /etc/hosts' sh "$new_ip"
docker exec "$runtime" curl -fsS --max-time 3 http://recovery-cell:7001/readyz > "$out/direct-new.json"
start=$SECONDS
recovered=0
while (( SECONDS-start < 15 )); do
  code=$(curl -sS --max-time 3 -o "$out/after.json" -w '%{http_code}' "$ready")
  printf 'elapsed=%ss http=%s body=%s\n' "$((SECONDS-start))" "$code" "$(cat "$out/after.json")" >> "$out/probes.log"
  if [[ $code == 200 ]]; then recovered=1; break; fi
  sleep 1
done
cat "$out/probes.log"
if [[ $recovered != 1 ]]; then echo "FAIL: new backend reachable but DO still pinned to old address; evidence=$out"; exit 1; fi
for ((i=0;i<3;i++)); do curl -fsS --max-time 3 "$ready" >/dev/null; sleep 1; done
docker exec "$runtime" curl -fsS --max-time 3 http://recovery-cell:7001/stats > "$out/new-stats.json"
grep -Eq '"polls":[1-9]' "$out/new-stats.json"
[[ $(docker inspect "$runtime" --format '{{.RestartCount}}') == 0 ]]
echo "PASS: DO and log polling recovered across $old_ip -> $new_ip without restart; evidence=$out"
