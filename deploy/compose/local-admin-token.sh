#!/usr/bin/env bash
# Print the local admin token derived from deploy/compose/cellhive.local.env.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
set -a
. "$HERE/cellhive.local.env"
set +a
node --input-type=module -e 'import {hkdfSync} from "node:crypto";let r=process.env.CELLHIVE_ROOT_KEY.trim();let k=Buffer.from(r,"base64");if(k.length!==32)k=Buffer.from(r,"hex");process.stdout.write(Buffer.from(hkdfSync("sha256",k,Buffer.alloc(0),"cellhive/token/admin",32)).toString("base64url"))'
