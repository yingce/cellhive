#!/usr/bin/env python3
"""Real example deployment checks. AI uses an explicit local protocol fixture.
React-static is a configuration-only template; supply a test HTML fixture.
Hyperdrive's sample checks configuration only; mysql-auto.sh checks actual SQL.
"""
import http.client,json,os,shutil,subprocess,sys,time
from pathlib import Path
if sys.argv[1]=='ai':
 from http.server import BaseHTTPRequestHandler,HTTPServer
 class Handler(BaseHTTPRequestHandler):
  def do_POST(self):
   data=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
   assert self.path=='/chat/completions' and data['messages'][0]['content']=='example-prompt'
   assert self.headers['Authorization']=='Bearer example-fixture-key'
   body=json.dumps({'choices':[{'message':{'content':'example-ai-ok'}}]}).encode()
   self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 HTTPServer(('127.0.0.1',int(sys.argv[2])),Handler).serve_forever();sys.exit()
root=Path(os.environ['E2E_ROOT']);base=Path(os.environ['E2E_BASE']);src=base/'examples'
shutil.copytree(root/'examples',src)
env=os.environ.copy();env['CELLHIVE_ADMIN_URL']='http://127.0.0.1:'+env['E2E_ADMIN_PORT']
cli=str(Path(env['E2E_BIN'])/'cellhive');results=[]
def command(args,**kw):
 p=subprocess.run(args,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=600,**kw)
 if p.returncode:raise RuntimeError(f'command exit {p.returncode}: '+p.stdout[-4000:])
 return p.stdout
def ctl(*args):return command([cli,*args])
def fetch(name,path='/',method='GET',body=None,headers=None):
 h={'Host':name+'.examples.test'};h.update(headers or {})
 c=http.client.HTTPConnection('127.0.0.1',int(env['E2E_RT_PORT']),timeout=60)
 c.request(method,path,body,h);r=c.getresponse();data=r.read();status=r.status;c.close()
 if status==503 and data==b'routing lookup unavailable\n':
  raise ConnectionError('routing publication pending')
 assert status<400,(name,path,method,status,data[:1000])
 return data
def obj(*a,**kw):return json.loads(fetch(*a,**kw))
def poll(fn,seconds=60):
 end=time.monotonic()+seconds
 while True:
  try:
   if fn():return
  except (AssertionError,KeyError):pass
  if time.monotonic()>end:raise AssertionError('poll deadline')
  time.sleep(.5)
def check(name):
 if name=='hello':assert b'hello from' in fetch(name)
 elif name=='webapi':assert obj(name,'/?a=1')['query']=={'a':'1'}
 elif name=='body':
  assert obj(name,method='POST',body='{"a":1}',headers={'Content-Type':'application/json'})=={'got':{'a':1}}
  assert fetch(name,method='POST',body=b'\x00\xffnative')==b'\x00\xffnative'
 elif name=='kv':
  fetch(name,'/k','PUT','example');assert obj(name,'/k')['value']=='example';fetch(name,'/k','DELETE');assert obj(name,'/k')['value'] is None
 elif name=='d1':
  assert obj(name,method='POST',body='{"body":"example"}',headers={'Content-Type':'application/json'})['changes']==1
  assert obj(name)['notes'][0]['body']=='example'
 elif name=='r2':
  fetch(name,'/k','PUT','example',{'Content-Type':'text/plain'});assert fetch(name,'/k')==b'example';fetch(name,'/k','HEAD');assert obj(name)['objects'][0]['key']=='k';fetch(name,'/k','DELETE');assert obj(name)['objects']==[]
 elif name in ('do-counter','async'):
  assert obj(name)['n']==1;assert obj(name)['n']==2
 elif name=='do-rpc':
  assert obj(name,method='POST')['n']==1;assert obj(name)['n']==1
 elif name=='router':
  assert obj(name,'/a')['n']==1;assert obj(name,'/b')['n']==1;assert obj(name,'/a')['n']==2
 elif name=='do-alarm':
  assert obj(name,'/arm')['armed'];poll(lambda:obj(name)['fires']>=1)
 elif name=='do-websocket':
  # Bun supports explicit Host on a real WebSocket handshake.
  code='''const ws = new WebSocket(process.argv[2], {headers:{Host:"do-websocket.examples.test"}});const timer=setTimeout(()=>process.exit(2),15000);ws.onopen=()=>ws.send("example");ws.onclose=e=>{if(e.code!==1000)console.error('websocket close',e.code,e.reason);process.exit(e.code===1000?0:5)};ws.onmessage=e=>{const x=JSON.parse(e.data);if(x.echo!=="example"||x.n!==1)process.exit(3);clearTimeout(timer);ws.close(1000,'done');};ws.onerror=()=>process.exit(4);'''
  p=base/'ws.cjs';p.write_text(code);command(['bun',str(p),'ws://127.0.0.1:'+env['E2E_RT_PORT']+'/'])
 elif name=='vars-secrets':assert obj(name)=={'mode':'demo','hasToken':True}
 elif name=='vectorize':fetch(name,'/seed');assert obj(name)['matches'][0]['id']=='a'
 elif name=='hyperdrive':assert obj(name)['configured']
 elif name=='ai':assert obj(name,'/?prompt=example-prompt')['response']=='example-ai-ok'
 elif name=='service-api':assert fetch(name)==b'api ok\n'
 elif name=='service-caller':assert obj(name)=={'fetched':'api ok\n','rpc':'hi bob','sum':5}
 elif name=='worker-rpc-api':assert fetch(name)==b'math ok\n'
 elif name=='worker-rpc-caller':assert obj(name)=={'sum':5,'boxed':{'input':1,'checked':True}}
 elif name=='queues':
  assert obj(name,'/?n=7')['sent']==7
  poll(lambda:json.loads(ctl('queue','stats','demo','JOBS'))['stats']['depth']==0)
 elif name=='workflows':
  ident=obj(name,'/create?seed=4')['id'];poll(lambda:obj(name,'/status?id='+ident)['status']=='complete');assert obj(name,'/status?id='+ident)['output']['value']==10
 elif name=='cron':
  assert b'runs every minute' in fetch(name)
  poll(lambda:'cron fired' in (base/'rt.log').read_text(),seconds=75)
 elif name=='assets':assert b'Static asset' in fetch(name);assert obj(name,'/api')['from']=='worker'
 elif name=='react-static':assert b'template-fixture' in fetch(name,'/nested/path')
 elif name=='astro-ssr':assert b'Astro SSR on CellHive' in fetch(name);assert b'About this example' in fetch(name,'/about');assert len(fetch(name,'/style.css'))>0
 else:raise AssertionError('uncovered example '+name)
paths=['hello','webapi','body','kv','d1','r2','do-counter','do-rpc','do-websocket','do-alarm','async','router','vars-secrets','vectorize','hyperdrive','ai','services/api','services/caller','worker-rpc/api','worker-rpc/caller','queues','workflows','cron','assets','react-static','astro']
if env.get('E2E_EXAMPLES'):paths=env['E2E_EXAMPLES'].split(',')
for item in paths:
 started=time.monotonic();name=item.replace('/','-')
 try:
  project=src/item
  if item=='astro':
   command(['npm','install','--no-audit','--no-fund'],cwd=project);command(['npm','run','build'],cwd=project)
  if item=='react-static':
   (project/'dist').mkdir();(project/'dist/index.html').write_text('<h1>template-fixture</h1>')
  # Strip JSONC comments using the repository's own CLI mapper, not JSON parsing.
  names={'services/api':'service-api','services/caller':'service-caller','worker-rpc/api':'worker-rpc-api','worker-rpc/caller':'worker-rpc-caller','astro':'astro-ssr'};name=names.get(item,item)
  host=name+'.examples.test';ctl('domain','add','demo',host)
  args=['deploy','demo',name,'--config',str(project/'wrangler.jsonc'),'--route',host]
  if item=='vars-secrets':
   secret=base/'example-secret.json';secret.write_text('{"TOKEN":"test-only-example"}');args+=['--secrets-file',str(secret)]
  ctl(*args)
  check(name)
  result={'example':item,'status':'PASS','seconds':round(time.monotonic()-started,2)}
 except Exception as e:result={'example':item,'status':'FAIL','error':str(e),'seconds':round(time.monotonic()-started,2)}
 results.append(result);print(json.dumps(result),flush=True)
(base/'example-results.json').write_text(json.dumps(results,indent=2))
print('EXAMPLES',sum(x['status']=='PASS' for x in results),'/',len(results),flush=True)
if env.get('E2E_EXAMPLE_DEBUG_SECONDS'):time.sleep(int(env['E2E_EXAMPLE_DEBUG_SECONDS']))
sys.exit(any(x['status']=='FAIL' for x in results))
