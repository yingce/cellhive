#!/usr/bin/env python3
"""Time real Wrangler subprocesses and verify active-version transitions."""
import json, os, re, subprocess, time, urllib.request
from pathlib import Path
cli = os.environ.get('WRANGLER_CLI_PATH')
if not cli:
    print('WRANGLER-PERFORMANCE: SKIP (set WRANGLER_CLI_PATH)')
    raise SystemExit(0)
base = Path(os.environ['PERF_RUN_DIR'])
ns = os.environ['PERF_NS']
api = os.environ['PERF_ADMIN'] + '/client/v4'
def fresh_token():
    return subprocess.check_output(['node', str(Path(os.environ['PERF_ROOT'])/'scripts/e2e/jwt.cjs'), 'admin'], text=True)
token = fresh_token()
config = {'name':'perf', 'main':str(Path(os.environ['PERF_ROOT'])/'scripts/e2e/fixtures/kv-performance.js'),
          'compatibility_date':'2026-09-16', 'kv_namespaces':[{'binding':'KV','id':'KV'}]}
(base/'wrangler.json').write_text(json.dumps(config))
env = dict(os.environ, CLOUDFLARE_API_BASE_URL=api, CLOUDFLARE_API_TOKEN=token,
           CLOUDFLARE_ACCOUNT_ID=ns, WRANGLER_SEND_METRICS='false', CI='true')
def active():
    req=urllib.request.Request(api+f'/accounts/{ns}/workers/scripts/perf/deployments',headers={'Authorization':'Bearer '+fresh_token()})
    with urllib.request.urlopen(req,timeout=30) as response:
        return json.load(response)['result']['deployments'][0]['versions'][0]['version_id']
for batch in range(1,4):
    marker=ns+":"+str(batch)
    config["vars"]={"PERF_ACCEPTANCE_VERSION":marker}
    (base/"wrangler.json").write_text(json.dumps(config))
    previous=active()
    token = fresh_token()
    env['CLOUDFLARE_API_TOKEN'] = token
    started=time.monotonic()
    result=subprocess.run(['node',cli,'versions','upload','--config',str(base/'wrangler.json')],env=env,cwd=base,capture_output=True,text=True,timeout=120)
    elapsed=time.monotonic()-started
    if result.returncode: raise RuntimeError('Wrangler upload failed: '+result.stderr.replace(token,'***'))
    match=re.search(r'Worker Version ID:\s*(\S+)',result.stdout)
    if not match: raise RuntimeError('Wrangler returned no version ID')
    version=match.group(1)
    if active()!=previous: raise RuntimeError('upload changed active')
    print(json.dumps({'layer':'wrangler','mode':'versions-upload','round':batch,'elapsed_ms':round(elapsed*1000,2),'version':version,'active_unchanged':True}),flush=True)
    token = fresh_token()
    env['CLOUDFLARE_API_TOKEN'] = token
    started=time.monotonic()
    result=subprocess.run(['node',cli,'versions','deploy',version+'@100%','--yes','--config',str(base/'wrangler.json')],env=env,cwd=base,capture_output=True,text=True,timeout=120)
    elapsed=time.monotonic()-started
    if result.returncode: raise RuntimeError('Wrangler deploy failed: '+result.stderr.replace(token,'***'))
    if active()!=version: raise RuntimeError('deployment did not activate uploaded version')
    # Verify the selected official runtime can execute the activated version,
    # not only that the control-plane active pointer was updated.
    deadline=time.monotonic()+30
    while True:
        try:
            probe=urllib.request.Request(os.environ['PERF_RUNTIME']+'/perf-version',headers={'Host':os.environ['PERF_HOST']})
            with urllib.request.urlopen(probe,timeout=5) as response:
                if response.status==200 and response.read()==marker.encode(): break
        except Exception:
            pass
        if time.monotonic()>=deadline: raise RuntimeError('activated worker did not serve the expected version marker')
        time.sleep(.1)
    print(json.dumps({'layer':'wrangler','mode':'versions-deploy','round':batch,'elapsed_ms':round(elapsed*1000,2),'version':version,'active_verified':True,'runtime_version_verified':True}),flush=True)
