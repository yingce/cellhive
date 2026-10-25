set -eu
phase=$1
region=$2
ns=ops-runtime-20261025
indices="0 1"
[ "$region" != local ] || indices="0"
target() { if [ "$region" = local ]; then printf "http://127.0.0.1:7001"; else printf "http://cellhive-%s-cell-agent-%s.cellhive-%s-cell-agent:7001" "$region" "$1" "$region"; fi; }
name=rollout
base="http://cellhive-${region}-cell-agent"
admin=$(cellhive creds admin)
token=$(cellhive token --ns "$ns" --kind kv --name "$name" --ttl 30m)
request() {
  printf 'header = "x-cellhive-scope-token: %s"\n' "$token" | curl -fsS --connect-timeout 5 --max-time 30 --config - "$@"
}
if [ "$phase" = before ]; then
 printf 'header = "x-cellhive-admin-token: %s"\n' "$admin" | curl -fsS --max-time 30 --config - -H 'content-type: application/json' --data '{"namespace":"ops-runtime-20261025","kind":"kv","name":"rollout","scope":"ops-runtime-20261025/__kv__/rollout"}' http://127.0.0.1:8082/v1/control/resource
 for i in $indices; do
   request -X POST --data-binary "before-node-$i-runtime288" "$(target "$i")/v1/kv/put?ns=$ns&key=runtime288-pre-$i&expiration_ttl=86400"
 done
fi
for i in $indices; do
 for k in $indices; do
  got=$(request "$(target "$i")/v1/kv/get?ns=$ns&key=runtime288-pre-$k")
  [ "$got" = "before-node-$k-runtime288" ] || { echo "value mismatch node=$i key=$k"; exit 1; }
 done
 if [ "$phase" = after ]; then
  request -X POST --data-binary "after-node-$i-runtime288" -w '\nPUT HTTP=%{http_code} TOTAL=%{time_total}s\n' "$(target "$i")/v1/kv/put?ns=$ns&key=runtime288-post-$i&expiration_ttl=600"
  got=$(request "$(target "$i")/v1/kv/get?ns=$ns&key=runtime288-post-$i")
  [ "$got" = "after-node-$i-runtime288" ] || exit 1
 fi
 echo "node=$i phase=$phase values=PASS"
done
