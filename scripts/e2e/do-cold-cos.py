#!/usr/bin/env python3
"""Real DO SQLite capture / COS cold read / update / independent restore acceptance."""
import argparse
import fcntl
import os
from pathlib import Path
import subprocess
import socket
import shutil
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials', default='/opt/.secrets/s3')
parser.add_argument('--workerd', required=True)
parser.add_argument('--paged', action='store_true')
parser.add_argument('--large', action='store_true', help='10GiB native paged DO, bounded seed/verify batches')
parser.add_argument('--log', help='persistent sanitized log path (default: docs/reports/artifacts)')
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
log_path = Path(args.log) if args.log else root / 'docs/reports/artifacts' / f'do-cold-{time.time_ns()}.log'
log_path.parent.mkdir(parents=True, exist_ok=True)
log = log_path.open('x', buffering=1)
creds = {}

def emit(line):
    for value in creds.values():
        line = line.replace(value, '***')
    print(line, end='', flush=True)
    log.write(line)
    log.flush()
    os.fsync(log.fileno())

emit(f'log={log_path} large={args.large}\n')
try:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
except OSError as error:
    emit(f'PRECHECK FAIL: local listener unavailable: {error}\n')
    raise SystemExit(2)
if args.large:
    scratch = os.environ.get('TMPDIR', '/tmp')
    free = shutil.disk_usage(scratch).free
    if free < 36 * (1 << 30):
        emit(f'PRECHECK FAIL: 10GiB fixture needs 36GiB scratch headroom; free={free}\n')
        raise SystemExit(2)

for line in Path(args.credentials).read_text().splitlines():
    line = line.strip().removeprefix('export ')
    if '=' not in line or line.startswith('#'):
        continue
    key, value = line.split('=', 1)
    if key.strip() in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        creds[key.strip()] = value.strip().strip('\"\'')
if not all(creds.get(k) for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')):
    raise SystemExit('credential file missing required keys')
env = {k: v for k, v in os.environ.items()
       if not k.startswith(('CELLHIVE_', 'COS_', 'AWS_', 'OSS_'))
       and not k.upper().endswith('_PROXY')}
env.update(CELLHIVE_DO_COS_TEST='1', CELLHIVE_WORKERD=str(Path(args.workerd).resolve()),
           COS_BUCKET='cellhive-e2e-hk-1376795072',
           COS_ENDPOINT='https://cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com',
           COS_SECRET_ID=creds['AWS_ACCESS_KEY_ID'], COS_SECRET_KEY=creds['AWS_SECRET_ACCESS_KEY'],
           NO_PROXY='*', no_proxy='*', GOMAXPROCS='4')
test = 'TestDOCOSColdReadAndRecapture'
if args.large:
    args.paged = True
if args.paged:
    env['CELLHIVE_DO_PAGING_TEST'] = '1'
    test = 'TestNativeDOPagedColdReadAndRecapture'
if args.large:
    env['CELLHIVE_DO_LARGE_TEST'] = '1'
    test = 'TestNativeDOLargePagedColdReadAndRecapture'
with open('/tmp/cellhive-hk-review.lock', 'a') as lock:
    emit('DO-COS waiting for shared-bucket lock\n')
    fcntl.flock(lock, fcntl.LOCK_EX)
    version = subprocess.check_output([env['CELLHIVE_WORKERD'], '--version'], text=True).strip()
    emit(f'workerd={version} paged={args.paged} compute=local storage=HK-COS\n')
    cmd = ['go', 'test', '-p', '2', '-tags',
           'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook',
           './internal/doruntime', '-run', '^'+test+'$', '-count=1', '-v', '-timeout=60m' if args.large else '-timeout=8m']
    proc = subprocess.Popen(cmd, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for line in proc.stdout:
            emit(line)
        code = proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()
        emit('INTERRUPTED: acceptance incomplete\n')
        raise
    emit(f'EXIT={code}\n')
    raise SystemExit(code)
