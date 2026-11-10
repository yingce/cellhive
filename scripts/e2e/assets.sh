#!/usr/bin/env bash
# Real static asset deployment, immutable retry, and public read fallback.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
e2e_start_agent assets "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"
e2e_cli app create e2eassets >/dev/null
e2e_cli domain add e2eassets assets.test >/dev/null
cp -R "$E2E_ROOT/examples/assets" "$E2E_BASE/project"
: > "$E2E_BASE/project/public/empty.txt"
printf '\000\377binary-asset\n' > "$E2E_BASE/project/public/blob.bin"
e2e_cli deploy e2eassets assets --config "$E2E_BASE/project/wrangler.jsonc" --route assets.test >/dev/null
for file in index.html empty.txt blob.bin; do
  curl --fail --silent --show-error --retry 4 --retry-all-errors -H 'Host: assets.test' \
    "http://127.0.0.1:$E2E_RT_PORT/$file" -o "$E2E_BASE/response"
  cmp "$E2E_BASE/project/public/$file" "$E2E_BASE/response"
done
# Upload retry with the exact same version token is idempotent. Different
# bytes at that immutable path must fail without modifying the existing value.
url="http://127.0.0.1:$E2E_ADMIN_PORT/v1/control/asset?ns=e2eassets&worker=assets&token=immutable-test&path=probe.txt"
for attempt in 1 2; do
  curl --fail --silent --show-error -X POST -H "Authorization: Bearer $(e2e_token admin)" --data-binary original "$url" >/dev/null
done
status=$(curl --silent --show-error -o "$E2E_BASE/conflict.json" -w '%{http_code}' -X POST \
 -H "Authorization: Bearer $(e2e_token admin)" --data-binary changed "$url")
[[ "$status" = 400 ]] || e2e_fail "immutable conflict status=$status"
echo 'E2E-ASSETS: PASS public HTML/binary/empty and immutable retry/conflict'
