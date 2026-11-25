import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
const source = name => readFileSync(new URL(name, import.meta.url), 'utf8').replace(/^import .*?;\n/gms, '').replace(/^export /gm, '');
const globals = {WorkerEntrypoint: class {constructor(ctx, env){this.ctx=ctx;this.env=env;}}, RpcTarget: class {}, TextEncoder, TextDecoder, Request, Response, Headers, URL, Blob, ReadableStream, atob, btoa, crypto};
function bindingContext() {
 const ctx=vm.createContext({currentTrace:()=>({traceparent:"",requestId:""}),runWithTrace:(_c,fn)=>fn(),...globals,mintJWT:async()=> 'jwt', internalHeaders:async()=>({})});
 vm.runInContext(source('bindings.js')+'\nglobalThis.api={R2Bucket,Assets};',ctx);return ctx;
}
for (const workers of [1,100,220]) test(`Tail retained records obey shared budget across ${workers} workers`,()=>{
 const ctx=vm.createContext({currentTrace:()=>({traceparent:"",requestId:""}),runWithTrace:(_c,fn)=>fn(),...globals});
 vm.runInContext(source('log-tail.js')+`\nglobalThis.probe=()=>{for(let i=0;i<400;i++)storeRecords('ns','w'+(i%${workers}),[{logs:[{message:['x'.repeat(250000)]}]}],{});const retained=new Set([...globalItems,...[...rings.values()].flatMap(r=>r.items)].filter(Boolean));return [...retained].reduce((sum,item)=>sum+item.size,0);};`,ctx);
 assert.ok(ctx.probe()<=8*1024*1024,'all strong references must fit the shared record budget');
});
test('R2 get returns before upstream EOF; body consumption is single-use',async()=>{
 const ctx=bindingContext();let finish;
 const upstream=new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('first'));finish=()=>{c.enqueue(new TextEncoder().encode('last'));c.close();};}});
 const bucket=new ctx.api.R2Bucket({props:{ns:'n',name:'b',authorization:{}}},{CELL_URL:'http://agent',PLATFORM:{fetch:async()=>new Response(upstream)}});
 let timer;const result=await Promise.race([bucket.get('key'),new Promise(r=>timer=setTimeout(()=>r(null),100))]);clearTimeout(timer);
 if(!result){finish();assert.fail('get waited for EOF');}
 const reader=result.body.getReader();assert.equal(new TextDecoder().decode((await reader.read()).value),'first');finish();await reader.read();await reader.read();reader.releaseLock();
 if(result.text) await assert.rejects(()=>result.text());
});
test('Assets HEAD never requests or reads upstream body',async()=>{
 const ctx=bindingContext();let method;
 const assets=new ctx.api.Assets({props:{}},{CELL_URL:'http://agent',PLATFORM:{fetch:async(_,opts)=>{method=opts.method||'GET';return {status:200,ok:true,headers:new Headers({'content-length':'999'}),body:null,arrayBuffer(){throw Error('read body');}};}}});
 const res=await assets.fetch(new Request('http://tenant/file',{method:'HEAD'}));assert.equal(method,'HEAD');assert.equal(res.body,null);assert.equal(res.headers.get('content-length'),'999');
});
test('Assets GET forwards stream without aggregating and propagates cancellation',async()=>{
 const ctx=bindingContext();let cancelled=false;
 const body=new ReadableStream({cancel(){cancelled=true;}});
 const assets=new ctx.api.Assets({props:{}},{CELL_URL:'http://agent',PLATFORM:{fetch:async()=>({status:200,ok:true,headers:new Headers(),body,arrayBuffer(){throw Error('read body');}})}});
 const res=await assets.fetch('http://tenant/file');await res.body.cancel();assert.equal(cancelled,true);
});

// Exercise both the legacy local facade and the RPC receiver-side facade.
for (const kind of ['local', 'rpc']) test(`R2 ${kind} shares one stream across body and consumption helpers`, async () => {
 const { AsyncLocalStorage } = await import('node:async_hooks');
 const ctx = vm.createContext({...globals, AsyncLocalStorage});
 vm.runInContext(source('facades.js')+'\nglobalThis.api={makeR2,wrapR2Metadata};',ctx);
 let controller;
 const body = new ReadableStream({start(c){controller=c;}}, {highWaterMark:0});
 const upstream = new Response(body);
 const bucket = kind === 'local'
   ? ctx.api.makeR2({url:'http://agent',fetcher:async()=>upstream},'n','b')
   : ctx.api.wrapR2Metadata({get:async()=>({body:upstream.body,httpMetadata:{contentType:'text/plain'}})});
 let timer;
 const obj = await Promise.race([bucket.get('k'),new Promise(r=>timer=setTimeout(()=>r(null),100))]); clearTimeout(timer);
 if (!obj) {controller.close();assert.fail('get waited for body');}
 assert.equal(obj.bodyUsed,false);
 const consuming=obj.text();controller.enqueue(new TextEncoder().encode('hello'));controller.close();
 assert.equal(await consuming,'hello');assert.equal(obj.bodyUsed,true);
 await assert.rejects(()=>obj.json());
});

test('Assets fetch accepts HEAD through RequestInit',async()=>{
 const ctx=bindingContext();let method;
 const assets=new ctx.api.Assets({props:{}},{CELL_URL:'http://agent',PLATFORM:{fetch:async(_,opts)=>{method=opts.method;return new Response(null);}}});
 const res=await assets.fetch('http://tenant/file',{method:'HEAD'});
 assert.equal(method,'HEAD');assert.equal(res.body,null);
});
