set -eu
phase=$1
region=$2
export CELLHIVE_ADMIN_URL=http://127.0.0.1:8082
export CELLHIVE_ADMIN_TOKEN=$(cellhive creds admin)
ns=ops-runtime288-do
host=runtime288-acceptance.invalid
if [ "$region" = local ]; then public=http://user-runtime:8081; else public=http://cellhive-$region-user-runtime:8081; fi
request() { curl --silent --show-error --connect-timeout 5 --max-time 60 -H "Host: $host" "$@"; }
admin() { printf 'header = "x-cellhive-admin-token: %s"\n' "$CELLHIVE_ADMIN_TOKEN" | curl -fsS --connect-timeout 5 --max-time 60 --config - "$@"; }
if [ "$phase" = before ]; then
 cat > /tmp/runtime288-probe.js <<'JS'
import { DurableObject } from "cloudflare:workers";
export class Probe extends DurableObject {
 async fetch(req) {
  const p=new URL(req.url).pathname;
  if(p==='/seed'){await this.ctx.storage.put('before',await req.text());return new Response('seeded');}
  if(p==='/read')return new Response(await this.ctx.storage.get('before')||'missing');
  if(p==='/binary'){
   const h=new Headers({'X-Probe':'runtime288'});h.append('Set-Cookie','a=b');h.append('Set-Cookie','c=d');
   return new Response(await req.arrayBuffer(),{headers:h});
  }
  if(p==='/clear'){
   const values={};for(let i=0;i<300;i++)values['k'+i]=i;
   await this.ctx.storage.put(values);await this.ctx.storage.deleteAll();
   return new Response(String((await this.ctx.storage.list()).size));
  }
  if(p==='/redirect')return new Response(null,{status:302,headers:{Location:'/read'}});
  return new Response('ok');
 }
}
export default {async fetch(req,env){const id=new URL(req.url).pathname==='/clear'?'clear':'persist';return env.PROBE.getByName(id).fetch(req);}};
JS
 cellhive app create "$ns" >/dev/null
 cellhive domain add "$ns" "$host" >/dev/null
 sha=$(sha256sum /tmp/runtime288-probe.js | cut -d ' ' -f1)
 admin -X POST --data-binary @/tmp/runtime288-probe.js "$CELLHIVE_ADMIN_URL/v1/control/bundle" >/dev/null
 printf '{"namespace":"%s","worker":"probe","bundle_sha":"%s","bindings":[{"type":"do","name":"PROBE","id":"Probe"}],"migrations":[{"tag":"v1","new_sqlite_classes":["Probe"]}],"idempotency_key":"runtime288-acceptance"}' "$ns" "$sha" > /tmp/runtime288-deploy.json
 admin -H 'content-type: application/json' --data-binary @/tmp/runtime288-deploy.json "$CELLHIVE_ADMIN_URL/v1/control/deploy" >/dev/null
 cellhive route add "$ns" "$host" probe >/dev/null
 n=0
 until [ "$(request "$public/ping")" = ok ]; do n=$((n+1)); [ "$n" -lt 30 ] || exit 1; sleep 1; done
 [ "$(request -X POST --data-binary "durable-before-runtime288-$region" "$public/seed")" = seeded ]
fi
[ "$(request "$public/read")" = "durable-before-runtime288-$region" ]
echo "$region DO persisted value phase=$phase PASS"
if [ "$phase" = after ]; then
 printf '\000\377\200A' > /tmp/runtime288-bytes
 code=$(request -X POST --data-binary @/tmp/runtime288-bytes -D /tmp/runtime288-headers -o /tmp/runtime288-response -w '%{http_code}' "$public/binary")
 [ "$code" = 200 ]; cmp /tmp/runtime288-bytes /tmp/runtime288-response
 grep -qi '^x-probe: runtime288' /tmp/runtime288-headers
 [ "$(grep -ic '^set-cookie:' /tmp/runtime288-headers)" = 2 ]
 [ "$(request "$public/clear")" = 0 ]
 code=$(request -D /tmp/runtime288-redirect -o /dev/null -w '%{http_code}' "$public/redirect")
 [ "$code" = 302 ]; grep -qi '^location: /read' /tmp/runtime288-redirect
 echo "$region DO binary+headers+cookies+deleteAll+redirect PASS"
fi
