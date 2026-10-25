#!/usr/bin/env python3
"""Fault injection for workflow-recovery.sh. No credentials/tokens in artifacts."""
import argparse
import base64
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import urllib.parse

ns = argparse.ArgumentParser()
ns.add_argument('--namespace', required=True)
ns = ns.parse_args().namespace
root = Path(os.environ['E2E_ROOT'])
base = Path(os.environ['E2E_BASE'])
agent, admin, public, internal = [int(os.environ[k]) for k in
    ('E2E_AGENT_PORT', 'E2E_ADMIN_PORT', 'E2E_RT_PORT', 'E2E_RT_INTERNAL')]
proxy_port = internal + 7
held = threading.Event()
release = threading.Event()
armed = threading.Event()
seen = {}
seen_lock = threading.Lock()

def request(port, path, method='GET', body=None, headers=None, timeout=120):
    c = http.client.HTTPConnection('127.0.0.1', port, timeout=timeout)
    try:
        c.request(method, path, body=body, headers=headers or {})
        r = c.getresponse()
        return r.status, r.read()
    finally:
        c.close()

class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *_): pass
    def handle(self):
        try:
            super().handle()
        except ConnectionResetError:
            # Workerd may reset idle keep-alive sockets during isolate teardown.
            # In-flight transport errors are reported separately in forward().
            pass
    def forward(self):
        data = self.rfile.read(int(self.headers.get('content-length', '0')))
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if '/v1/internal/workflow/' in self.path and q.get('run'):
            with seen_lock:
                seen[q['id'][0]] = (q['run'][0], self.headers.get('x-cellhive-internal-token'), q['workflow'][0])
        try:
            code, body = request(agent, self.path, self.command, data,
                                 {k:v for k,v in self.headers.items() if k.lower() not in ('host','connection','transfer-encoding')})
            if armed.is_set() and '/event/consume?' in self.path and code == 200 and json.loads(body).get('found'):
                armed.clear()
                # The real agent has waited for the bucket durability proof;
                # withhold its response until the runtime and agent are killed.
                held.set()
                release.wait(180)
            self.send_response(code)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (OSError, http.client.HTTPException) as exc:
            print('PROXY transport error', self.command, urllib.parse.urlsplit(self.path).path, type(exc).__name__, flush=True)
            self.close_connection = True
    do_GET = do_POST = do_PUT = do_DELETE = forward

class ProxyServer(http.server.ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True

proxy = ProxyServer(('127.0.0.1', proxy_port), Proxy)
threading.Thread(target=proxy.serve_forever, daemon=True).start()

def wait_for(fn, label, seconds=180):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            last = fn()
            if last: return last
        except (OSError, http.client.HTTPException): pass
        time.sleep(.3)
    raise AssertionError(f'{label} timeout, last={last}')

def spawn(name, binary, extra):
    env = dict(os.environ, **{k:str(v) for k,v in extra.items()})
    log = open(base / (name + '.log'), 'ab')
    p = subprocess.Popen([str(Path(os.environ['E2E_BIN']) / binary)], env=env,
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    (base / (name + '.pid')).write_text(str(p.pid))
    return p

def start_runtime():
    p = spawn('fault-runtime', 'user-runtime', {
        'CELLHIVE_CELL_URL': f'http://127.0.0.1:{proxy_port}',
        'CELLHIVE_USER_RUNTIME_PORT': public, 'CELLHIVE_USER_RUNTIME_INTERNAL_PORT': internal,
        'CELLHIVE_RUNTIME_DIR': base / 'runtime', 'CELLHIVE_USER_RUNTIME_JS': root / 'workerd/user-runtime',
        'CELLHIVE_FACADES_JS': root / 'workerd/platform/facades.js'})
    wait_for(lambda: request(public, '/ready', timeout=2)[0] == 200, 'runtime ready')
    return p

def call(path, **query):
    code, body = request(public, path+'?'+urllib.parse.urlencode(query), headers={'Host':'workflow-recovery.test'})
    assert code == 200, (path, code, body)
    return body

def status(id, kind='event'):
    s = json.loads(call('/status', id=id, kind=kind))
    assert s['status'] != 'errored', s
    return s

def stop(pid):
    try: os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError: pass

runtime = start_runtime()
wait_for(lambda: request(public, '/ping', headers={'Host':'workflow-recovery.test'}) == (200,b'ok'), 'route')
try:
    # Real runtime callback fencing: capture the superseded run identity.
    call('/create', id='fence')
    wait_for(lambda: status('fence')['status'] == 'waiting', 'initial park')
    with seen_lock: old_token, auth, workflow_name = seen['fence']
    call('/restart', id='fence')
    wait_for(lambda: status('fence')['status'] == 'waiting', 'restarted park')
    for op, method in [('step','PUT'), ('finish','POST')]:
        q = urllib.parse.urlencode(dict(ns=ns, workflow=workflow_name, id='fence', name='ready', run=old_token))
        code, body = request(agent, '/v1/internal/workflow/'+op+'?'+q, method,
                             base64.b64encode(b'"old"'), {'x-cellhive-internal-token':auth})
        assert code == 409, (op,code,body)
    call('/event', id='fence', n=7)
    result = wait_for(lambda: (s if (s:=status('fence'))['status']=='complete' else None), 'new execution completion')
    assert result['output']['payload']['n'] == 7, result
    print('PASS stale step/finish callbacks return 409; new execution output=7', flush=True)

    # All events exist before parallel waits, avoiding intentional parking races.
    call('/create', id='parallel', kind='parallel')
    for n in range(16):
        call('/event', id='parallel', kind='parallel', n=n)
    result = wait_for(lambda: (s if (s:=status('parallel','parallel'))['status']=='complete' else None), 'parallel consume')
    values = sorted(v['payload']['n'] for v in result['output'])
    assert values == list(range(16)), values
    print('PASS real workerd 16 concurrent waits consume 16 distinct events', flush=True)

    call('/create', id='crash')
    wait_for(lambda: status('crash')['status']=='waiting', 'crash fixture park')
    armed.set()
    call('/event', id='crash', n=42)
    assert held.wait(30), 'did not intercept confirmed consume response'
    # No step.put can have happened: consume response has not reached workerd.
    stop(runtime.pid)
    for file in base.glob('*.pid'):
        if file.stem not in ('fault-runtime',): stop(int(file.read_text()))
    release.set()
    runtime.wait(timeout=10)
    print('INJECT SIGKILL runtime + agent after durable consume, before response; fresh local data directory', flush=True)
    time.sleep(12)
    cold = spawn('cold-agent', 'cell-agent', {
        'CELLHIVE_NODE_ID': ns+'-cold', 'CELLHIVE_REST_ADDR':f'127.0.0.1:{agent}',
        'CELLHIVE_ADMIN_ADDR':f'127.0.0.1:{admin}', 'CELLHIVE_DATA_DIR':base/'cold-agent',
        'CELLHIVE_BUCKET_DIR':base/'bucket', 'CELLHIVE_DISPATCH_URL':f'http://127.0.0.1:{internal}',
        'CELLHIVE_QUEUE_INTERVAL':'1s', 'CELLHIVE_TIMER_INTERVAL':'1s'})
    wait_for(lambda: request(agent,'/readyz', timeout=2)[0]==200, 'cold agent')
    runtime = start_runtime()
    result = wait_for(lambda: (s if (s:=status('crash'))['status']=='complete' else None), 'cold recovered event result', seconds=200)
    assert result['output']['payload']['n']==42, result
    assert status('fence')['output']['payload']['n']==7
    assert sorted(v['payload']['n'] for v in status('parallel','parallel')['output'])==list(range(16))
    print('PASS fresh agent restores all three results from object storage; lost response event output=42', flush=True)
    print('E2E-WORKFLOW-RECOVERY: PASS', flush=True)
finally:
    release.set()
    proxy.shutdown()
    # Parent shell owns app cleanup and all process PID files.
