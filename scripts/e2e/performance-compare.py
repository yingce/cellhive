#!/usr/bin/env python3
"""Alternate preserved/candidate binaries using one identical real-COS harness."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline', required=True, type=Path)
p.add_argument('--candidate', required=True, type=Path)
p.add_argument('--workerd', required=True, type=Path)
p.add_argument('--output', required=True, type=Path)
p.add_argument('--credentials', default='/opt/.secrets/s3')
p.add_argument('--minimum-seconds', type=float, default=3)
p.add_argument('--pairs', type=int, default=2)
args = p.parse_args()
if not 1 <= args.pairs <= 5 or not 0 < args.minimum_seconds <= 60:
    p.error('pairs must be1..5 and minimum-seconds must be >0 and <=60')
root = Path(__file__).resolve().parents[2]
args.output = args.output.resolve()
args.output.mkdir(parents=True, exist_ok=False)
binaries = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve()}
def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
manifest = dict(workerd=str(args.workerd.resolve()), workerd_sha256=sha(args.workerd),
                minimum_seconds=args.minimum_seconds, rounds_per_run=1,
                affinity=sorted(os.sched_getaffinity(0)), durability='fleet ACK required by harness',
                topology='local two cell-agents plus user-runtime/workerd; real HK COS',
                payload_bytes=1024, concurrency=[1, 8, 32], signing='same jwt.cjs for both binaries',
                scripts={name: sha(root/'scripts/e2e'/name) for name in
                         ['kv-performance.cjs', 'kv-performance.sh', 'jwt.cjs', 'lib.sh', 'hk-review.py']},
                binaries={label: {name: sha(directory/name) for name in
                                  ['cell-agent', 'cellhive', 'user-runtime', 'do-runtime']}
                          for label, directory in binaries.items()})
(args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
results = []
for pair in range(args.pairs):
    for label in ['baseline', 'candidate']:
        number = len(results)+1
        log = args.output/f'{number:02d}-{label}.log'
        env = dict(os.environ, GOMAXPROCS='4', E2E_BIN=str(binaries[label]),
                   PERF_MIN_SECONDS=str(args.minimum_seconds), PERF_ROUNDS='1', PERF_PRIME_ENTRIES='1')
        # Diagnostic modes change traffic/overhead and must not leak into RPS runs.
        for key in ['PERF_IDLE_GAPS', 'PERF_TRACE', 'PERF_TAIL_DIAG']:
            env.pop(key, None)
        started = time.monotonic()
        print(f'START {number} {label}', flush=True)
        with log.open('x') as output:
            run = subprocess.run(['python3', 'scripts/e2e/hk-review.py', '--credentials', args.credentials,
                                  '--suite', 'performance-dual', '--port-base', '26900',
                                  '--workerd', str(args.workerd.resolve()), '--artifacts-dir', str(args.output)],
                                 cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT)
        result = dict(label=label, pair=pair+1, exit=run.returncode,
                      seconds=round(time.monotonic()-started, 3), log=str(log))
        results.append(result)
        (args.output/'runs.json').write_text(json.dumps(results, indent=2)+'\n')
        print(json.dumps(result), flush=True)
        if run.returncode:
            raise SystemExit(run.returncode)
