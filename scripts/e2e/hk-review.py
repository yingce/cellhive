#!/usr/bin/env python3
"""Run review/Queue E2E against the dedicated HK COS bucket with unique namespaces.
Local compute, real remote storage. Credentials never printed or persisted.
Usage: python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3
"""
import argparse
import fcntl
import os
import re
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import socket

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials', required=True)
parser.add_argument('--port-base', type=int, default=26500)
parser.add_argument('--workerd', default='/opt/cellhive/deploy/workerd-fork/workerd', help='explicit workerd binary for this acceptance run')
parser.add_argument('--trace-endpoint', default='')
parser.add_argument('--suite', choices=('do-idle-retirement', 'background', 'assets', 'do-transfer', 'queue-multinode', 'review', 'workflow', 'performance', 'performance-dual', 'performance-tail', 'batch-recovery', 'routing-safety', 'epoch-stress', 'cell-transfer', 'do-paging-fleet'), default='review')
parser.add_argument('--artifacts-dir', default='', help='parent directory for persistent sanitized test artifacts')
parser.add_argument('--namespace', default='', help='fixed application namespace (single-suite runs only)')
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
if args.suite == 'do-paging-fleet':
    try:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
    except OSError as error:
        raise SystemExit(f'PRECHECK FAIL: local listener unavailable: {error}')
# All suites share the bucket's platform control cells, even with unique app
# namespaces. Different per-run root keys cannot safely run concurrently.
run_lock = open('/tmp/cellhive-hk-review.lock', 'a')
print('HK-REVIEW waiting for exclusive shared-bucket test lock', flush=True)
fcntl.flock(run_lock, fcntl.LOCK_EX)
# Never mistake an older test runtime's readiness endpoint for this run.
# Reserve/check the full three-case port window before creating remote state.
listeners = []
try:
    for port in range(args.port_base, args.port_base + (128 if args.suite == 'background' else 48)):
        listener = socket.socket()
        listeners.append(listener)
        # Match Go listeners: TIME_WAIT from the prior serialized suite is
        # reusable, but an active listener must still reject this reservation.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('0.0.0.0', port))
        listener.listen()
except OSError as error:
    raise SystemExit(f'PRECHECK FAIL: port {port} unavailable: {error}')
finally:
    for listener in listeners:
        listener.close()
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
env.update(CELLHIVE_BUCKET='cos://cellhive-e2e-hk-1376795072',
           COS_ENDPOINT='https://cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com',
           COS_SECRET_ID=creds['AWS_ACCESS_KEY_ID'], COS_SECRET_KEY=creds['AWS_SECRET_ACCESS_KEY'],
           NO_PROXY='*', no_proxy='*', CELLHIVE_DURABILITY='bucket',
           CELLHIVE_WORKERD=str(Path(args.workerd).resolve()), E2E_KEEP='1',
           HK_REVIEW_ROOT=str(root))
if args.suite in ('performance-dual', 'performance-tail', 'batch-recovery', 'routing-safety', 'epoch-stress', 'do-paging-fleet'):
    env.update(CELLHIVE_DURABILITY='auto', E2E_DUAL_AGENT='1')
if args.suite == 'performance-tail':
    env['PERF_TAIL_DIAG']='1'
if args.trace_endpoint:
    env['PERF_TRACE']='1'
    env.update(CELLHIVE_OTLP_ENDPOINT=args.trace_endpoint, CELLHIVE_TRACES_SAMPLE_RATIO='1')
run = 'hk-review-' + str(int(time.time())) + '-' + secrets.token_hex(3)
artifact_parent = Path(args.artifacts_dir) if args.artifacts_dir else (root / 'docs/reports/artifacts' if args.suite == 'do-paging-fleet' else None)
if artifact_parent is not None:
    artifact_parent.mkdir(parents=True, exist_ok=True)
artifacts = Path(tempfile.mkdtemp(prefix=run + '-', dir=artifact_parent))
print(f'HK-REVIEW artifacts={artifacts} compute=local storage=HK-COS durability={env["CELLHIVE_DURABILITY"]}', flush=True)
version = subprocess.check_output([env['CELLHIVE_WORKERD'], '--version'], text=True).strip()
print(f'WORKERD binary={env["CELLHIVE_WORKERD"]} version={version}', flush=True)
failed = []
suites = {
    'do-idle-retirement': [('do-idle-retirement.sh', 'e2epaging')],
    'background': [('queue.sh', 'e2eq'), ('queue-concurrency.sh', 'e2econc'), ('queue-multinode.sh', 'e2emn'), ('workflow.sh', 'e2ewf'), ('workflow-recovery.sh', 'e2ewfr'), ('examples.sh', 'demo')],
    'assets': [('assets.sh', 'e2eassets')],
    'do-transfer': [('do-transfer.sh', 'e2epaging')],
    'queue-multinode': [('queue-multinode.sh', 'e2emn')],
    'do-paging-fleet': [('do-paging-fleet.sh', 'e2epaging')],
    'cell-transfer': [('cell-transfer.sh', 'e2etransfer')],
    'batch-recovery': [('kv-batch-recovery.sh', 'e2ebatch')],
    'routing-safety': [('kv-routing-safety.sh', 'e2eroute')],
    'epoch-stress': [('kv-epoch-stress.sh', 'e2ebatch')],
    'performance': [('kv-performance.sh', 'e2eperf')],
    'performance-dual': [('kv-performance.sh', 'e2eperf')],
    'performance-tail': [('kv-performance.sh', 'e2eperf')],
    'review': [('review-fixes.sh', 'review'), ('queue.sh', 'e2eq'), ('queue-multinode.sh', 'e2emn')],
    'workflow': [('workflow.sh', 'e2ewf'), ('workflow-recovery.sh', 'e2ewfr')],
}
for index, (script, original_ns) in enumerate(suites[args.suite]):
    ns = args.namespace if args.namespace and index == 0 else f'{run}-{index}'
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
      -H "Authorization: Bearer $(e2e_token admin)" \\
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
    # Host ownership outlives application deletion until asynchronous cleanup
    # finishes. Isolate hosts as well as namespaces across acceptance runs.
    text = re.sub(r'\b([a-zA-Z0-9.-]+)\.test\b',
                  lambda match: f'{match.group(1)}.{run}.test', text)
    if script == 'examples.sh':
        # The Python example deployer also owns namespace and hostname literals.
        example = (root / 'scripts/e2e/examples.py').read_text()
        example = example.replace("'demo'", repr(ns))
        example = example.replace('.examples.test', f'.{run}.examples.test')
        (case / 'examples.py').write_text(example)
        text = text.replace('$E2E_ROOT/scripts/e2e/examples.py', str(case / 'examples.py'))
    (case / script).write_text(text)
    case_env = dict(env, E2E_PORT_BASE=str(args.port_base + index * 20),
                    HK_REVIEW_NS=ns, HK_CASE_DIR=str(case))
    if script == 'examples.sh':
        case_env['E2E_EXAMPLES'] = 'cron,do-alarm,queues,workflows'
        if os.environ.get('CELLHIVE_ESBUILD'):
            case_env['CELLHIVE_ESBUILD'] = os.environ['CELLHIVE_ESBUILD']
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
