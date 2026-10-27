#!/usr/bin/env python3
"""Run production storage contracts against the dedicated HK COS test bucket.

No Docker or MinIO. Credentials are passed via environment, never command args.
"""
import argparse
import fcntl
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials', required=True)
parser.add_argument('--native-repeat', type=int, default=0,
                    help='Run only native concurrent-create contract N times')
parser.add_argument('--append-diag', action='store_true',
                    help='Record safe HTTP response metadata for native append tests')
args = parser.parse_args()
if args.native_repeat < 0:
    parser.error("--native-repeat must be nonnegative")
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

# Serialize against the other HK harnesses using the same dedicated bucket.
run_lock = open('/tmp/cellhive-hk-review.lock', 'a')
fcntl.flock(run_lock, fcntl.LOCK_EX)
env = {k: v for k, v in os.environ.items()
       if not k.startswith(('CELLHIVE_', 'COS_', 'AWS_', 'OSS_'))
       and not k.upper().endswith('_PROXY')}
env.update(
    CELLHIVE_NATIVE_TEST_PROVIDER='cos', CELLHIVE_COS_TEST='1',
    COS_BUCKET='vwork-e2e-hk-1376795072',
    COS_ENDPOINT='https://vwork-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com',
    COS_SECRET_ID=creds['AWS_ACCESS_KEY_ID'],
    COS_SECRET_KEY=creds['AWS_SECRET_ACCESS_KEY'], NO_PROXY='*', no_proxy='*',
    CELLHIVE_S3_TEST_PROVIDER='cos',
    CELLHIVE_S3_TEST_ENDPOINT='https://cos.ap-hongkong.myqcloud.com',
    CELLHIVE_S3_TEST_REGION='ap-hongkong',
    CELLHIVE_S3_TEST_BUCKET='vwork-e2e-hk-1376795072',
    CELLHIVE_S3_TEST_ACCESS=creds['AWS_ACCESS_KEY_ID'],
    CELLHIVE_S3_TEST_SECRET=creds['AWS_SECRET_ACCESS_KEY'],
    CELLHIVE_S3_TEST_PATH_STYLE='false',
    CELLHIVE_S3_TEST_DISABLE_CHECKSUMS='true')
tests = (
    'TestS3ReplicationRestoreChain', 'TestProviderCloudDiagnose', 'TestProviderListCloud',
    'TestNativeAppendCloud', 'TestCOSConditionalDelete',
    'TestCOSDataContract', 'TestCOSLargePagedRestore',
)
if args.native_repeat:
    tests = ('TestNativeAppendCloud',)
if args.append_diag:
    env.update(CELLHIVE_COS_APPEND_DIAG='1', CELLHIVE_COS_APPEND_DIAG_ALL='1')
repeat = args.native_repeat or 1
print('COS contracts: production native-append adapter + S3 data plane; dedicated HK bucket', flush=True)
result = subprocess.run(
    ['go', 'test', '-tags',
     'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook',
     './internal/bucket', './internal/replica', '-run', '^(' + '|'.join(tests) + ')$',
     f'-count={repeat}', '-v', '-timeout=10m'],
    env=env, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
output = result.stdout
for value in creds.values():
    output = output.replace(value, '***')
print(output, end='', flush=True)
missing = [name for name in tests if output.count(f'--- PASS: {name} (') != repeat]
if result.returncode or missing:
    print(f'COS-CONTRACT: FAIL exit={result.returncode} not_passed={missing}', flush=True)
    raise SystemExit(1)
print(f'COS-CONTRACT: PASS {len(tests)*repeat}/{len(tests)*repeat} (no skips)', flush=True)
