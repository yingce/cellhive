set -eu
region=$1
T=$(cellhive creds internal)
meta=$(printf 'header = "x-cellhive-internal-token: %s"\n' "$T" | curl -fsS --config - 'http://127.0.0.1:7001/v1/control/worker?ns=ops-runtime288-do&worker=probe')
sha=$(printf '%s' "$meta" | sed -n 's/.*"bundle_sha":"\([^"]*\)".*/\1/p')
storage=$(printf '%s' "$meta" | sed -n 's/.*"storage_id":"\([^"]*\)".*/\1/p')
[ -n "$sha" ] && [ -n "$storage" ]
T=$(cellhive token --ns ops-runtime288-do --kind do --name PROBE --ttl 10m)
path=read
for node in 0 1; do
 printf '{"namespace":"ops-runtime288-do","worker":"probe","class":"Probe","storage_class":"Probe","storage_id":"%s","version":1,"id":"persist","bundle_sha":"%s","request":{"path":"/%s","method":"POST","body":"durable-before-runtime288-%s"}}' "$storage" "$sha" "$path" "$region" > /tmp/runtime288-seed.json
 out=$(printf 'header = "x-cellhive-scope-token: %s"\n' "$T" | curl -fsS --max-time 60 --config - -H 'content-type: application/json' --data-binary @/tmp/runtime288-seed.json "http://cellhive-$region-cell-agent-$node.cellhive-$region-cell-agent:7001/v1/do/invoke")
 printf "canary response: %s\n" "$out"
 if [ "$path" = seed ]; then [ "$out" = seeded ]; else [ "$out" = "durable-before-runtime288-$region" ]; fi
 echo "$region scoped DO read through agent-$node PASS"
done
