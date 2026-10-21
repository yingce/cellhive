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
  // Bounded read-ahead for response bodies that are normally delivered whole
  // (JSON/text payloads): a fully consumed small body is returned as a
  // memory-backed Response, so the RPC boundary does not need a per-chunk JS
  // stream. Everything else (SSE, octet streams, bodies over the cap) keeps the
  // tracking wrapper, so the isolate is never retired while a response body is
  // still in flight. Buffering is bounded by BODY_BUFFER_CAP per response.
  static BODY_BUFFER_CAP = 128 * 1024;
  static SMALL_BODY_TYPE = /^(application\/json|application\/[a-z0-9.+-]*\+json|text\/plain)/i;
  static concatChunks(chunks){
    let total=0;for(const c of chunks)total+=c.byteLength;
    const out=new Uint8Array(total);let off=0;
    for(const c of chunks){out.set(c,off);off+=c.byteLength;}
    return out;
  }
  async response(res,onDone=()=>{}){
    if(res.webSocket||res.status===101){this.pinned=true;onDone();return res;}
    if(!res.body){onDone();return res;}
    const release=this.hold();const reader=res.body.getReader();let finished=false;
    const finish=()=>{if(!finished){finished=true;release();onDone();}};
    const replay=(initial)=>{
      let pending=initial,pos=0;
      return new ReadableStream({
        async pull(c){try{
          if(pending&&pos<pending.length){c.enqueue(pending[pos++]);if(pos===pending.length)pending=null;return;}
          const r=await reader.read();if(r.done){finish();c.close();}else c.enqueue(r.value);
        }catch(e){finish();c.error(e);}},
        async cancel(reason){try{await reader.cancel(reason);}finally{finish();}},
      },{highWaterMark:0});
    };
    const contentType=String(res.headers.get("content-type")||"").toLowerCase();
    if(Lifetime.SMALL_BODY_TYPE.test(contentType)){
      const chunks=[];let total=0,oversize=false;
      try{
        for(;;){
          const r=await reader.read();
          if(r.done)break;
          if(!(r.value instanceof Uint8Array)){oversize=true;break;}
          chunks.push(r.value);total+=r.value.byteLength;
          if(total>Lifetime.BODY_BUFFER_CAP){oversize=true;break;}
        }
      }catch(e){finish();throw e;}
      if(!oversize){
        finish();
        if(chunks.length===0)return new Response(null,{status:res.status,statusText:res.statusText,headers:res.headers});
        const body=chunks.length===1?chunks[0]:Lifetime.concatChunks(chunks);
        return new Response(body,{status:res.status,statusText:res.statusText,headers:res.headers});
      }
      return new Response(replay(chunks),{status:res.status,statusText:res.statusText,headers:res.headers});
    }
    return new Response(replay(null),{status:res.status,statusText:res.statusText,headers:res.headers});
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
export function managedModules(code, target, nonce, specs = {}){
  const main=code.mainModule;
  return {
    '__cellhive-state.js': `export ${Lifetime.toString()}; export const life=new Lifetime();`,
    '__cellhive-quota.js': quotaSource(code.limits?.subRequests, specs),
    '__cellhive-workers.js': `export * from 'cloudflare:workers'; import {waitUntil as original} from 'cloudflare:workers'; import {life} from '__cellhive-state.js'; export const waitUntil=p=>original(life.background(p));`,
    '__cellhive-managed.js': `
      import {WorkerEntrypoint,abortIsolate} from 'cloudflare:workers';
      import {runInvocation} from '__cellhive-quota.js';
      import {life} from '__cellhive-state.js';
      import * as target from ${JSON.stringify(main)};
      const nonce=${JSON.stringify(nonce)};
      export class CellHiveManagedFetch extends WorkerEntrypoint {
        async fetch(req){
          const release=life.hold();
          try{
            const ctx=life.context(this.ctx), T=target[${JSON.stringify(target)}];
            const handler=typeof T==='function'?new T(ctx,this.env):T;
            const res=await runInvocation(()=>handler.fetch(req,this.env,ctx));
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

export function quotaSource(max, specs) {
  if (!max) return 'export const runInvocation=fn=>fn();';
  return `
    import {AsyncLocalStorage} from 'node:async_hooks';
    import {env} from 'cloudflare:workers';
    const contexts=new AsyncLocalStorage(), limit=${JSON.stringify(max)};
    function consume(){const state=contexts.getStore();if(!state)return;if(++state.count>limit){const e=new Error('subrequest limit exceeded');e.code='subrequest_limit_exceeded';throw e;}}
    const fetchOriginal=globalThis.fetch.bind(globalThis);
    globalThis.fetch=(...args)=>{consume();return fetchOriginal(...args);};
    const localMethods=new Set(['prepare','bind','get','getByName','idFromName','idFromString','newUniqueId','resumeMultipartUpload']);
    function wrap(value,kind){
      if(!value||typeof value!=='object')return value;
      return new Proxy(value,{get(target,key){
        if(key==='then')return undefined;
        const v=Reflect.get(target,key,target);
        if(typeof v!=='function')return v;
        return (...args)=>{
          const local=(kind==='d1'&&(key==='prepare'||key==='bind'))||(kind==='do'&&localMethods.has(key))||(kind==='workflow'&&key==='get')||(kind==='r2'&&key==='resumeMultipartUpload');
          if(!local)consume();
          const result=v.apply(target,args);
          if(local&&result&&typeof result.then==='function')return result.then(x=>wrap(x,kind));
          if(local)return wrap(result,kind);
          return result;
        };
      }});
    }
    const specs=${JSON.stringify(Object.fromEntries(Object.entries(specs).map(([k,v])=>[k,v.kind])))};
    for(const [name,kind] of Object.entries(specs))if(env[name])Object.defineProperty(env,name,{value:wrap(env[name],kind),writable:true,configurable:true,enumerable:true});
    export const runInvocation=fn=>contexts.run({count:0},fn);
  `;
}

// Executed inside the tenant isolate. AsyncLocalStorage associates console
// output with overlapping asynchronous Queue/Cron invocations, never a mutable
// module-global current invocation. No platform credentials enter this module.
export function invocationSource() {
  return `
    import { AsyncLocalStorage } from 'node:async_hooks';
    const contexts = new AsyncLocalStorage();
    // Capture the native sink before tenant module evaluation can replace
    // console methods. Only the wrapper owns the per-loaded-worker Tail key.
    export const dispatchDebug = console.debug.bind(console);
    export function trackBackground(p,delegate) {
      const state=contexts.getStore(), promise=Promise.resolve(p);
      if(state){if(state.background.length>=256)throw new Error('too many invocation background tasks');state.background.push(promise);}
      return delegate(promise);
    }
    function render(value) {
      if (typeof value === 'string') return value;
      try { return JSON.stringify(value, (_key,v)=>typeof v==='bigint'?String(v)+'n':v) ?? String(value); }
      catch { try { return String(value); } catch { return '[unprintable]'; } }
    }
    for (const level of ['log','info','warn','error','debug','trace','assert']) {
      const original = typeof console[level]==='function' ? console[level].bind(console) : ()=>{};
      console[level] = (...args) => {
        const state=contexts.getStore();
        if (state && (level!=='assert'||!args[0])) {
          let message=(level==='assert'?args.slice(1):args).map(render).join(' ');
          const remaining=Math.max(0,8192-state.chars);
          if(state.logs.length>=256||remaining===0) state.truncated=true;
          else {
            if(message.length>remaining){message=message.slice(0,remaining);state.truncated=true;}
            state.chars+=message.length;
            state.logs.push({level,message,at_ms:Date.now()});
          }
        }
        return original(...args);
      };
    }
    export async function loggedInvocation(fn,ctx) {
      const state={logs:[],truncated:false,chars:0,background:[]};
      return contexts.run(state,async()=>{
        const background=state.background;
        const original=ctx.waitUntil.bind(ctx);
        Object.defineProperty(ctx,'waitUntil',{configurable:true,value:p=>trackBackground(p,original)});
        try {
          const value=await fn();
          for(let i=0;i<background.length;){const pending=background.slice(i);i=background.length;await Promise.all(pending);}
          return {value,logs:state.logs,logs_truncated:state.truncated};
        }
        catch(error) {
          for(let i=0;i<background.length;){const pending=background.slice(i);i=background.length;await Promise.allSettled(pending);}
          let message,stack='';try{message=String(error);}catch{message='[unprintable exception]';}
          try{if(error&&typeof error.stack==='string')stack=error.stack.slice(0,4096);}catch{}
          return {error:message.slice(0,2048),error_stack:stack,logs:state.logs,logs_truncated:state.truncated};
        }
        finally {Object.defineProperty(ctx,'waitUntil',{configurable:true,value:original});}
      });
    }
  `;
}
