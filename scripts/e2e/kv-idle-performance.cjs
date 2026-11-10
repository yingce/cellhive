// One shared keep-alive client per entry path. Never retry the measured read;
// report socket reuse/connection timing separately from response latency.
const http = require('node:http');
const crypto = require('node:crypto');
const {performance} = require('node:perf_hooks');
const {token} = require('./jwt.cjs');

module.exports = async function idlePerformance(value) {
  const rounds = Number(process.env.PERF_IDLE_ROUNDS || 2);
  if (!Number.isInteger(rounds) || rounds < 1 || rounds > 5) throw new Error('PERF_IDLE_ROUNDS must be1..5');
  for (const layer of ['direct', ...(process.env.PERF_PEER ? ['forwarded'] : []), 'worker']) {
    const agent = new http.Agent({keepAlive:true, maxSockets:1});
    const base = layer === 'worker' ? process.env.PERF_RUNTIME : layer === 'forwarded' ? process.env.PERF_PEER : process.env.PERF_AGENT;
    const path = layer === 'worker' ? '/get?key=k0' : `/v1/kv/get?ns=${encodeURIComponent(process.env.PERF_NS)}&key=k0`;
    async function probe(round, gap, phase, actualGapMs = 0) {
      const trace = crypto.randomBytes(16).toString('hex');
      const headers = layer === 'worker' ? {host:process.env.PERF_HOST} : {authorization:'Bearer '+token('scope',{ns:process.env.PERF_NS,kind:'kv',name:'KV'})};
      headers.traceparent = `00-${trace}-${crypto.randomBytes(8).toString('hex')}-01`;
      const start = performance.now();
      let connectMs = null, headersMs = null, reused = false;
      await new Promise((resolve,reject) => {
        const req = http.get(base+path, {agent,headers}, res => {
          headersMs = performance.now()-start;
          reused = req.reusedSocket;
          let body = '';
          res.on('data',b=>body+=b);res.on('error',reject);
          res.on('end',()=>res.statusCode===200 && body===value ? resolve() : reject(new Error(`idle ${layer}: status=${res.statusCode} valid=${body===value}`)));
        });
        req.on('socket',socket=>{
          if (socket.connecting) socket.once('connect',()=>{connectMs=performance.now()-start;});
        });
        req.setTimeout(30000,()=>req.destroy(new Error('idle request timeout')));
        req.on('error',reject);
      });
      console.log(JSON.stringify({event:'idle-request',layer,round,gap_seconds:gap,phase,actual_gap_ms:+actualGapMs.toFixed(3),elapsed_ms:+(performance.now()-start).toFixed(3),headers_ms:+headersMs.toFixed(3),connect_ms:connectMs===null?null:+connectMs.toFixed(3),socket_reused:reused,trace}));
    }
    try {
      for (let round=1;round<=rounds;round++) {
        for (const gap of [0,1,5,10,30]) {
          await probe(round,gap,'prime');
          const last=performance.now();
          await new Promise(resolve=>setTimeout(resolve,gap*1000));
          await probe(round,gap,'after-gap',performance.now()-last);
          await probe(round,gap,'immediate-repeat');
        }
      }
    } finally { agent.destroy(); }
  }
};
