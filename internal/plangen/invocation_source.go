package plangen

// invocationSource is copied from lifecycle.js invocationSource(); static workers
// have no Tail sink, so the wrapper only needs console capture/waitUntil
// bookkeeping. Keep it byte-identical to the platform generator.
const invocationSource = `
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
  `
