// Closed-loop, persistent HTTP connections; latency includes complete response body.
const http = require('node:http');
const crypto = require('node:crypto');
const {performance} = require('node:perf_hooks');
const value = 'x'.repeat(1024);
const results = [];
async function measure(layer, mode, concurrency, n, round, label='warm') {
  const agent = new http.Agent({keepAlive:true, maxSockets:concurrency});
  let index=0, failures=0; const times=[], errors=[];
  const before=process.env.PERF_PEER?await commitCounts():null;
  const start=performance.now();
  await Promise.all(Array.from({length:concurrency}, async()=>{
    for (;;) {
      const i=index++; if(i>=n) break;
      const base=layer==='worker'?process.env.PERF_RUNTIME:layer==='forwarded'?process.env.PERF_PEER:process.env.PERF_AGENT;
      const path=layer!=='worker'?`/v1/kv/${mode}?ns=${encodeURIComponent(process.env.PERF_NS)}&key=k${i%32}`:`/${mode}?key=k${i%32}`;
      const headers=layer!=='worker'?{'x-cellhive-scope-token':process.env.PERF_SCOPE_TOKEN}:{host:process.env.PERF_HOST};
      const trace=crypto.randomBytes(16).toString('hex');
      if(process.env.PERF_TRACE || process.env.PERF_TAIL_DIAG) headers.traceparent=`00-${trace}-${crypto.randomBytes(8).toString('hex')}-01`;
      if(mode==='put') headers['content-length']=Buffer.byteLength(value);
      const t=performance.now();
      try {
        await new Promise((resolve,reject)=>{
          const req=http.request(base+path,{agent,method:mode==='put'?'POST':'GET',headers},res=>{
            let body='';res.on('data',b=>body+=b);res.on('error',reject);
            res.on('end',()=>res.statusCode===200&&(mode!=='get'||body===value)?resolve():reject(new Error(`status=${res.statusCode} body-valid=${mode!=='get'||body===value}`)));
          });
          req.setTimeout(30000,()=>req.destroy(new Error('timeout')));
          req.on('error',reject);req.end(mode==='put'?value:undefined);
        });
        const ms=performance.now()-t;
        times.push(ms);
        if((process.env.PERF_TRACE || process.env.PERF_TAIL_DIAG) && ms>=50) console.log(JSON.stringify({event:'slow-request',layer,mode,round,index:i,concurrency,ms,trace,time:new Date().toISOString()}));
      } catch(e) { failures++;if(errors.length<3)errors.push(e.message); }
    }
  }));
  const elapsed=performance.now()-start;agent.destroy();times.sort((a,b)=>a-b);
  const p=q=>times.length?+times[Math.min(times.length-1,Math.ceil(times.length*q)-1)].toFixed(3):null;
  const result={layer,mode,label,round,concurrency,n,failures,p50_ms:p(.5),p95_ms:p(.95),p99_ms:p(.99),max_ms:p(1),rps:+((n-failures)*1000/elapsed).toFixed(2),elapsed_ms:+elapsed.toFixed(2),errors};
  if(before) { const after=await commitCounts(); result.commit_delta=Object.fromEntries(Object.keys(after).map(k=>[k,after[k]-(before[k]||0)])); }
  results.push(result);console.log(JSON.stringify(result));
  if(failures)throw new Error(`${layer}/${mode}: ${failures} failures`);
}
async function read(url,headers={}) {
 return new Promise((resolve,reject)=>{http.get(url,{headers},res=>{let data='';res.on('data',b=>data+=b);res.on('end',()=>res.statusCode===200?resolve(data):reject(new Error(`read status=${res.statusCode}`)));res.on('error',reject);}).on('error',reject);});
}
async function commitCounts() {
 const out={fleet:0};
 for(const base of [process.env.PERF_AGENT,process.env.PERF_PEER]) {
  if(!base)continue;
  const text=await read(base+'/metrics');
  for(const match of text.matchAll(/cellhive_commit_total\{mode="([^"]+)"\} (\d+)/g)) out[match[1]]=(out[match[1]]||0)+Number(match[2]);
 }
 return out;
}
(async()=>{
  await measure('direct','put',1,1,0,'first-write');
  if(process.env.PERF_PEER) {
    const resolution=JSON.parse(await read(process.env.PERF_AGENT+'/v1/internal/resolve?scope='+encodeURIComponent(process.env.PERF_NS+'/__kv__/KV'),{'x-cellhive-internal-token':process.env.PERF_INTERNAL_TOKEN}));
    if(!resolution.owned) throw new Error('initial node did not own KV cell');
    console.log(JSON.stringify({event:'owner-verified',owner:resolution.owner,entry:process.env.PERF_AGENT,peer:process.env.PERF_PEER}));
    let ready=false;
    for(let i=0;i<90;i++) {
      const before=await commitCounts();
      await measure('direct','put',1,1,0,'peer-warmup');
      const after=await commitCounts();
      if(after.fleet>(before.fleet||0)) {ready=true;break;}
      await new Promise(r=>setTimeout(r,1000));
    }
    if(!ready) throw new Error('no fleet acknowledgement after peer discovery');
  }
  await measure('direct','put',1,32,0,'preload');
  await measure('worker','get',1,1,0,'first-worker-read');
  if(process.env.PERF_TAIL_DIAG) {
    for(let round=1;round<=6;round++) {
      const c=round%2?1:8;
      await measure('direct','put',c,64,round);
      await new Promise(r=>setTimeout(r,2200));
      await measure('forwarded','put',c,64,round);
      await measure('forwarded','get',c,64,round);
      await measure('worker','put',c,64,round);
      await measure('worker','get',c,64,round);
    }
    await new Promise(r=>setTimeout(r,6000));
    return;
  }
  for(let round=1;round<=2;round++) {
    for(const concurrency of [1,8,32]) {
      const n=concurrency===1?60:concurrency===8?200:400;
      for(const layer of (process.env.PERF_PEER?['direct','forwarded','worker']:['direct','worker'])) {
        for(const mode of ['put','get']) await measure(layer,mode,concurrency,mode==='get'?(concurrency===1?5000:10000):n,round);
      }
    }
  }
  if(process.env.PERF_TRACE) await new Promise(r=>setTimeout(r,6000));
})().catch(e=>{console.error(e.message);process.exitCode=1;});
