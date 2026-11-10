#!/usr/bin/env python3
"""Opt-in real SQLite/COS acceptance. Default: 10 GiB populated BLOBs.

Serializes HK bucket use; credentials never appear in argv or output. The
compressible fixture measures SQLite size and actual encoded upload separately.
Use --payload-mib 16 for a small real-COS smoke run (not a 10 GiB acceptance).
"""
import argparse
import fcntl
import os
from pathlib import Path
import shutil
import subprocess
import threading

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials', default='/opt/.secrets/s3')
parser.add_argument('--payload-mib', type=int, default=10240)
parser.add_argument('--scratch', default='/tmp', help='filesystem for sequential source/restore')
parser.add_argument('--partial-query', action='store_true', help='measure cold paged SQLite reads instead of full materialization')
args = parser.parse_args()
if not 1 <= args.payload_mib <= 65536:
    parser.error('--payload-mib must be 1..65536')
root = Path(__file__).resolve().parents[2]
creds = {}
for line in Path(args.credentials).read_text().splitlines():
    line = line.strip().removeprefix('export ')
    if '=' not in line or line.startswith('#'):
        continue
    key, value = line.split('=', 1)
    if key.strip() in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        creds[key.strip()] = value.strip().strip('\"\'')
if not all(creds.get(k) for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')):
    raise SystemExit('credential file missing required AWS keys')
with open('/tmp/cellhive-hk-review.lock', 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    scratch = Path(args.scratch).resolve()
    free = shutil.disk_usage(scratch).free
    payload = args.payload_mib * 1024 * 1024
    # Source and restored DB never coexist. Includes snapshot spool, page hash
    # spool, SQLite overhead, and a 1 GiB working margin (build cache separate).
    required = int(payload * 1.15) + (1 << 30)
    if free < required:
        raise SystemExit(f'insufficient scratch disk: free={free} required={required}')
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('CELLHIVE_', 'COS_', 'AWS_', 'OSS_'))
           and not k.upper().endswith('_PROXY')}
    env.update(CELLHIVE_COS_LARGE_TEST='1', CELLHIVE_COS_LARGE_BYTES=str(payload),
               COS_BUCKET='cellhive-e2e-hk-1376795072',
               COS_ENDPOINT='https://cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com',
               COS_SECRET_ID=creds['AWS_ACCESS_KEY_ID'],
               COS_SECRET_KEY=creds['AWS_SECRET_ACCESS_KEY'],
               NO_PROXY='*', no_proxy='*', TMPDIR=str(scratch))
    if args.partial_query:
        env['CELLHIVE_COS_PARTIAL_QUERY'] = '1'
    command = ['/usr/bin/time', '-v', 'go', 'test', '-tags',
               'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook',
               './internal/replica', '-run', '^TestCOSLargeSQLiteStreaming$',
               '-count=1', '-v', '-timeout=65m']
    print(f'payload_bytes={payload} scratch_free_bytes={free} timeout=65m', flush=True)
    stop_monitor = threading.Event()
    minimum_free = [free]

    def monitor_disk():
        while not stop_monitor.wait(1):
            minimum_free[0] = min(minimum_free[0], shutil.disk_usage(scratch).free)

    monitor = threading.Thread(target=monitor_disk, daemon=True)
    monitor.start()
    proc = subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    try:
        for line in proc.stdout:
            for secret in creds.values():
                line = line.replace(secret, '***')
            print(line, end='', flush=True)
        code = proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()
        raise SystemExit(130)
    finally:
        stop_monitor.set()
        monitor.join()
        print(f'scratch_filesystem_peak_used_delta_bytes={free - minimum_free[0]} sampling_seconds=1 shared_filesystem=true', flush=True)
    raise SystemExit(code)
