#!/usr/bin/env python3
"""Collect real COS idle request traces; service binaries are selected explicitly."""
import argparse
import gzip
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--label', required=True)
parser.add_argument('--bin', required=True)
parser.add_argument('--workerd', required=True)
parser.add_argument('--credentials', default='/opt/.secrets/s3')
parser.add_argument('--port-base', type=int, default=26700)
parser.add_argument('--trace-port', type=int, default=28200)
args = parser.parse_args()
if not args.label or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-_' for c in args.label):
    parser.error('label must contain lowercase letters, digits, hyphen or underscore')
root = Path(__file__).resolve().parents[2]
art = root / 'docs/reports/artifacts/2026-11-10-optimization'
trace = art / (args.label + '-traces')
trace.mkdir(parents=True, exist_ok=False)
for port in range(args.port_base, args.port_base + 6):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', port))

class Handler(http.server.BaseHTTPRequestHandler):
    counter = 0
    lock = threading.Lock()
    def log_message(self, *unused):
        pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        if self.headers.get('Content-Encoding') == 'gzip':
            body = gzip.decompress(body)
        with self.lock:
            Handler.counter += 1
            n = Handler.counter
        (trace / f'{n:06d}.pb').write_bytes(body)
        self.send_response(200)
        self.send_header('Content-Type', 'application/x-protobuf')
        self.send_header('Content-Length', '0')
        self.end_headers()

server = http.server.ThreadingHTTPServer(('127.0.0.1', args.trace_port), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
env = dict(os.environ, GOMAXPROCS='4', E2E_BIN=str(Path(args.bin).resolve()),
           PERF_IDLE_GAPS='1', PERF_IDLE_ROUNDS='2')
start = time.monotonic()
log = art / (args.label + '.log')
try:
    with log.open('x') as output:
        proc = subprocess.run(['python3', 'scripts/e2e/hk-review.py', '--credentials', args.credentials,
                               '--suite', 'performance-dual', '--port-base', str(args.port_base),
                               '--workerd', args.workerd, '--artifacts-dir', str(art),
                               '--trace-endpoint', f'http://127.0.0.1:{args.trace_port}'],
                              cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT)
finally:
    server.shutdown()
    server.server_close()
result = dict(exit=proc.returncode, seconds=round(time.monotonic()-start, 3),
              trace_batches=Handler.counter, log=str(log), binary_directory=env['E2E_BIN'])
(art / (args.label + '-result.json')).write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result), flush=True)
raise SystemExit(proc.returncode)
