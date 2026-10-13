// Plain-data caches and lifecycle primitives; capabilities stay request-local.
export class ByteLRU {
  constructor(max, maxBytes, sizeOf, maxEntry = maxBytes) {
    this.max=max; this.maxBytes=maxBytes; this.sizeOf=sizeOf; this.maxEntry=maxEntry;
    this.m=new Map(); this.weights=new Map(); this.bytes=0;
  }
  has(k){return this.m.has(k);}
  get(k){const v=this.m.get(k);if(v===undefined)return;this.m.delete(k);this.m.set(k,v);return v;}
  delete(k){if(!this.m.has(k))return false;this.bytes-=this.weights.get(k);this.weights.delete(k);return this.m.delete(k);}
  set(k,v){
    this.delete(k);const size=this.sizeOf(v)+k.length*2;
    if(!Number.isFinite(size)||size<0||size>this.maxEntry||size>this.maxBytes)return v;
    this.m.set(k,v);this.weights.set(k,size);this.bytes+=size;
    while(this.m.size>this.max||this.bytes>this.maxBytes)this.delete(this.m.keys().next().value);
    return v;
  }
}
export const textWeight = v => typeof v==='string' ? v.length*2 : 0;
export const jsonWeight = v => JSON.stringify(v).length*2;

export class Lifetime {
  constructor(){this.active=0;this.pinned=false;}
  hold(){this.active++;let held=true;return ()=>{if(held){held=false;this.active--;}};}
  idle(){return this.active===0&&!this.pinned;}
  background(p){const release=this.hold();return Promise.resolve(p).finally(release);}
  context(ctx){
    // Preserve the actual native ExecutionContext identity: WorkerEntrypoint
    // constructors unwrap it and would lose a JS Proxy's waitUntil override.
    const original=ctx.waitUntil.bind(ctx);
    Object.defineProperty(ctx,'waitUntil',{value:p=>original(this.background(p)),configurable:true});
    return ctx;
  }
  response(res,onDone=()=>{}){
    if(res.webSocket||res.status===101){this.pinned=true;onDone();return res;}
    if(!res.body){onDone();return res;}
    const release=this.hold();const reader=res.body.getReader();let finished=false;
    const finish=()=>{if(!finished){finished=true;release();onDone();}};
    const body=new ReadableStream({
      async pull(c){try{const r=await reader.read();if(r.done){finish();c.close();}else c.enqueue(r.value);}catch(e){finish();c.error(e);}},
      async cancel(reason){try{await reader.cancel(reason);}finally{finish();}},
    },{highWaterMark:0});
    return new Response(body,{status:res.status,statusText:res.statusText,headers:res.headers});
  }
}

export class RuntimeCapacity {
  constructor(max,maxBytes){this.max=max;this.maxBytes=maxBytes;this.entries=new Map();this.bytes=0;}
  reserve(key,bytes){
    if(this.entries.has(key))return true;
    if(this.entries.size>=this.max||this.bytes+bytes>this.maxBytes)return false;
    this.entries.set(key,bytes);this.bytes+=bytes;return true;
  }
  release(key){const n=this.entries.get(key);if(n!==undefined){this.bytes-=n;this.entries.delete(key);}}
}

// This source runs INSIDE the dynamic Worker. Never call abortIsolate in the
// static platform host. Nonces are incarnation identifiers, not credentials.
export function managedModules(code, target, nonce){
  const main=code.mainModule;
  return {
    '__cellhive-state.js': `export ${Lifetime.toString()}; export const life=new Lifetime();`,
    '__cellhive-workers.js': `export * from 'cloudflare:workers'; import {waitUntil as original} from 'cloudflare:workers'; import {life} from '__cellhive-state.js'; export const waitUntil=p=>original(life.background(p));`,
    '__cellhive-managed.js': `
      import {WorkerEntrypoint,abortIsolate} from 'cloudflare:workers';
      import {life} from '__cellhive-state.js';
      import * as target from ${JSON.stringify(main)};
      const nonce=${JSON.stringify(nonce)};
      export class CellHiveManagedFetch extends WorkerEntrypoint {
        async fetch(req){
          const release=life.hold();
          try{
            const ctx=life.context(this.ctx), T=target[${JSON.stringify(target)}];
            const handler=typeof T==='function'?new T(ctx,this.env):T;
            const res=await handler.fetch(req,this.env,ctx);
            return life.response(res);
          }finally{release();}
        }
      }
      export class CellHiveLifecycle extends WorkerEntrypoint {
        status(){return {nonce,idle:life.idle(),pinned:life.pinned};}
        retire(expected){if(expected!==nonce||!life.idle())return false;abortIsolate('cellhive: retired');}
      }
    `,
  };
}
