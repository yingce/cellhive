import sys,os,json,base64,subprocess,time,socket,http.client,tempfile
region=sys.argv[1];digest=sys.argv[2]
for k in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY']:os.environ.pop(k,None)
os.environ['NO_PROXY']='*'
k=['kubectl','--kubeconfig','/root/.kube/config_tke'+('_us' if region=='usw' else ''),'-n','cellhive-'+region]
def out(args):return subprocess.check_output(args,text=True)
def call(port,method,path,data=None,headers=None):
 c=http.client.HTTPConnection('127.0.0.1',port,timeout=90);c.request(method,path,data,headers or {});r=c.getresponse();b=r.read();c.close();return r.status,b
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
procs=[];created=False;ns='release-'+region+'-'+str(int(time.time()))
try:
 if region=='local':
  info=json.loads(out(['docker','inspect','cellhive-cell-agent-1']))[0]
  envdict=dict(x.split('=',1) for x in info['Config']['Env'])
  root=envdict['CELLHIVE_ROOT_KEY'];admin=8082;public=8081
  rest=port();ip=next(iter(info['NetworkSettings']['Networks'].values()))['IPAddress']
  # Agent REST is internal; port-forward using a minimal local TCP relay.
  import threading,socketserver
  class Relay(socketserver.BaseRequestHandler):
   def handle(self):
    import select
    remote=socket.create_connection((ip,7001));socks=[self.request,remote]
    try:
     while True:
      for src in select.select(socks,[],[],90)[0]:
       b=src.recv(65536)
       if not b:return
       (remote if src is self.request else self.request).sendall(b)
    finally:remote.close()
  srv=socketserver.ThreadingTCPServer(('127.0.0.1',rest),Relay);threading.Thread(target=srv.serve_forever,daemon=True).start()
 else:
  pods=json.loads(out(k+['get','pods','-o','json']))['items']
  for p in pods:
   if p['metadata'].get('labels',{}).get('app') not in ('cell-agent','do-runtime','user-runtime'):continue
   assert not p['metadata'].get('deletionTimestamp'),p['metadata']['name']
   for c in p['status']['containerStatuses']:
    assert c['ready'] and digest in c['imageID'],(p['metadata']['name'],c['imageID'])
    print('POD',p['metadata']['name'],'ready',c['ready'],'restarts',c['restartCount'],flush=True)
  root=base64.b64decode(json.loads(out(k+['get','secret','cellhive-secrets','-o','json']))['data']['CELLHIVE_ROOT_KEY']).decode()
  rest,admin,public=port(),port(),port()
  for target,maps in [('pod/cellhive-'+region+'-cell-agent-0',[f'{rest}:7001',f'{admin}:8082']),('service/cellhive-'+region+'-user-runtime',[f'{public}:8081'])]:
   p=subprocess.Popen(k+['port-forward',target]+maps,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);procs.append(p)
 env=os.environ.copy();env['CELLHIVE_ROOT_KEY']=root;env['CELLHIVE_ADMIN_URL']=f'http://127.0.0.1:{admin}'
 token=subprocess.check_output(['/opt/cellhive/bin/cellhive','creds','admin'],env=env,text=True).strip()
 ah={'x-cellhive-admin-token':token,'content-type':'application/json'}
 for i in range(100):
  try:
   if call(rest,'GET','/readyz')[0]==200:break
  except OSError:pass
  time.sleep(.2)
 else:raise RuntimeError('agent not ready')
 def api(path,data):
  s,b=call(admin,'POST','/v1/control/'+path,json.dumps(data),ah);assert s==200,(path,s,b[:200]);return json.loads(b)
 api('app',{'namespace':ns});created=True
 api('resource',{'namespace':ns,'kind':'kv','name':'KV','scope':'main'})
 scope=subprocess.check_output(['/opt/cellhive/bin/cellhive','token','--ns',ns,'--kind','kv','--name','KV'],env=env,text=True).strip();kh={'x-cellhive-scope-token':scope}
 for i in range(20):
  val=f'probe-{i}'.encode();s,b=call(rest,'POST',f'/v1/kv/put?ns={ns}&key=k{i}'+('&expiration_ttl=300' if i%2 else ''),val,kh);assert s==200,(s,b[:200])
  s,b=call(rest,'GET',f'/v1/kv/get?ns={ns}&key=k{i}',headers=kh);assert s==200 and b==val,(s,b[:200])
 print('PASS direct KV 20 PUT/GET pairs (10 TTL)',flush=True)
 def cli(*args):subprocess.run(['/opt/cellhive/bin/cellhive',*args],env=env,stdout=subprocess.DEVNULL,check=True)
 host=ns+'.test';cli('domain','add',ns,host)
 source=open('/opt/cellhive/scripts/e2e/fixtures/kv-performance.js','rb').read()
 s,b=call(admin,'POST','/v1/control/bundle',source,{'x-cellhive-admin-token':token});assert s==200,(s,b[:200]);sha=json.loads(b)['sha']
 api('deploy',{'namespace':ns,'worker':'probe','bundle_sha':sha,'bindings':[{'type':'kv','name':'KV','id':'KV'}]});cli('route','add',ns,host,'probe')
 for i in range(60):
  s,b=call(public,'GET','/perf-ready',headers={'Host':host})
  if s==200 and b==b'ok':break
  time.sleep(.5)
 else:raise RuntimeError(('route readiness',s,b[:200]))
 for i in range(10):
  val=f'worker-{i}'.encode();s,b=call(public,'POST',f'/put?key=w{i}',val,{'Host':host});assert s==200,(s,b[:200]);s,b=call(public,'GET',f'/get?key=w{i}',headers={'Host':host});assert s==200 and b==val,(s,b[:200])
 print('PASS Worker pinned binding 10 PUT/GET pairs',flush=True)
finally:
 if created:
  s,b=call(admin,'DELETE','/v1/control/app?namespace='+ns,headers=ah);print('cleanup',s,flush=True);assert s==200,(s,b[:200])
 for p in procs:p.terminate();p.wait(timeout=10)
