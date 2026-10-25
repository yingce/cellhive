import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
const source = readFileSync(process.env.WORKFLOW_WRAPPER_SOURCE || new URL('./workflow-wrapper.js', import.meta.url), 'utf8');
function harness(bridge) {
  const c = vm.createContext({ Date, JSON, btoa, atob, encodeURIComponent, decodeURIComponent, escape, unescape, setTimeout, clearTimeout });
  vm.runInContext('class SleepSignal {}; class StopSignal {}; class WorkflowTimeoutError extends Error {}; class NonRetryableError extends Error {};'+source.slice(source.indexOf('function b64encode('),source.indexOf('// WorkflowEntrypoint receives'))+source.slice(source.indexOf('function call('), source.indexOf('async function finish(')),c);
  return c.makeStep(bridge,'i');
}
test('early redispatch of memoized sleep re-parks the current lease',async()=>{
  const wake=Date.now()+60000,calls=[];
  const step=harness({async workflowStep(op,params){calls.push({op,params});return {status:200,body:JSON.stringify(op==='step.get'?{found:true,result:btoa(JSON.stringify({wake_at_ms:wake}))}:{status:'running'})};}});
  await assert.rejects(step.sleep('nap',60000));
  assert.equal(calls.at(-1).op,'sleep');
  assert.equal(calls.at(-1).params.wake_at_ms,wake);
});
test('expired memoized sleep proceeds without extending the deadline',async()=>{
  const calls=[];
  const step=harness({async workflowStep(op){calls.push(op);return {status:200,body:JSON.stringify(op==='step.get'?{found:true,result:btoa(JSON.stringify({wake_at_ms:Date.now()-1}))}:{status:'running'})};}});
  await step.sleep('nap',60000);assert.deepEqual(calls,['state.get','step.get']);
});
