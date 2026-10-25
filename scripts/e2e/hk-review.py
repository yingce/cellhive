#!/usr/bin/env python3
"""Run review/Queue E2E against the dedicated HK COS bucket with unique namespaces.
Local compute, real remote storage. Credentials never printed or persisted.
Usage: python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3
"""
import argparse
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials', required=True)
parser.add_argument('--port-base', type=int, default=26500)
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
creds = {}
for line in Path(args.credentials).read_text().splitlines():
    line = line.strip().removeprefix('export ')
    if not line or line.startswith('#') or '=' not in line:
        continue
    key, value = line.split('=', 1)
    if key.strip() in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        creds[key.strip()] = value.strip().strip('\"\'')
if not all(creds.get(k) for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')):
    raise SystemExit('credential file missing required AWS keys')
env = {k: v for k, v in os.environ.items()
       if not k.upper().endswith('_PROXY') and not k.startswith(('CELLHIVE_', 'COS_', 'AWS_', 'OSS_'))}
env.update(CELLHIVE_BUCKET='cos://vwork-e2e-hk-1376795072',
           COS_ENDPOINT='https://vwork-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com',
           COS_SECRET_ID=creds['AWS_ACCESS_KEY_ID'], COS_SECRET_KEY=creds['AWS_SECRET_ACCESS_KEY'],
           NO_PROXY='*', no_proxy='*', CELLHIVE_DURABILITY='bucket',
           CELLHIVE_WORKERD='/opt/cellhive/deploy/workerd-fork/workerd', E2E_KEEP='1',
           HK_REVIEW_ROOT=str(root))
run = 'hk-review-' + str(int(time.time())) + '-' + secrets.token_hex(3)
artifacts = Path(tempfile.mkdtemp(prefix=run + '-'))
print(f'HK-REVIEW artifacts={artifacts} compute=local storage=HK-COS durability=bucket', flush=True)
failed = []
for index, (script, original_ns) in enumerate([
        ('review-fixes.sh', 'review'), ('queue.sh', 'e2eq'), ('queue-multinode.sh', 'e2emn')]):
    ns = f'{run}-{index}'
    case = artifacts / str(index)
    case.mkdir()
    lib = (root / 'scripts/e2e/lib.sh').read_text()
    lib = lib.replace('E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"',
                      'E2E_ROOT="$HK_REVIEW_ROOT"')
    lib = lib.replace('CELLHIVE_NODE_ID="$node"', 'CELLHIVE_NODE_ID="$HK_REVIEW_NS-$node"')
    # Clean only this run's unique application, before terminating test processes.
    lib = lib.replace('e2e_cleanup() {\n', '''e2e_cleanup() {
  local port code cleaned=0
  for port in "$E2E_ADMIN_PORT" "$((E2E_PORT_BASE + 5))"; do
    code=$(curl -s -m 120 -o "$E2E_BASE/cleanup.json" -w '%{http_code}' -X DELETE \\
      -H "x-cellhive-admin-token: $CELLHIVE_ADMIN_TOKEN" \\
      "http://127.0.0.1:$port/v1/control/app?namespace=$HK_REVIEW_NS" || true)
    if [[ "$code" = 200 ]]; then cleaned=1; break; fi
  done
  echo "HK-CLEANUP ns=$HK_REVIEW_NS success=$cleaned http=$code"
  [[ "$cleaned" = 1 ]] && touch "$HK_CASE_DIR/cleanup.ok"
''')
    (case / 'lib.sh').write_text(lib)
    text = (root / 'scripts/e2e' / script).read_text()
    # All namespace occurrences are literal in these scripts; preserve fixture paths.
    text = text.replace(original_ns, ns)
    text = text.replace(f'{ns}-fixes.js', 'review-fixes.js')
    (case / script).write_text(text)
    case_env = dict(env, E2E_PORT_BASE=str(args.port_base + index * 20),
                    HK_REVIEW_NS=ns, HK_CASE_DIR=str(case))
    print(f'START {script} ns={ns}', flush=True)
    with (case / 'result.log').open('w') as log:
        result = subprocess.run(['bash', str(case / script)], cwd=root, env=case_env,
                                stdout=log, stderr=subprocess.STDOUT)
    output = (case / 'result.log').read_text()
    for key in creds.values():
        output = output.replace(key, '***')
    (case / 'result.log').write_text(output)
    print(output, end='', flush=True)
    passed = result.returncode == 0 and (case / 'cleanup.ok').exists()
    print(f'END {script} exit={result.returncode} cleanup={(case / "cleanup.ok").exists()}', flush=True)
    if not passed:
        failed.append(script)
    # The next isolated process uses a different node/root identity. Let the last
    # killed test session expire before taking the shared test registry owner.
    time.sleep(12)
print(f'HK-REVIEW: {"FAIL " + str(failed) if failed else "PASS"} artifacts={artifacts}', flush=True)
raise SystemExit(bool(failed))
