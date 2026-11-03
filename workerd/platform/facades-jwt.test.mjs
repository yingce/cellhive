import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mintJWT, verifyJWT } from './jwt.js';

const key = Buffer.alloc(32, 7).toString('base64url');
function facades() {
  const source = readFileSync(new URL('./facades.js', import.meta.url), 'utf8')
    .replace(/import\s*\{[^}]+\}\s*from\s*"[^"]+";/g, '').replace(/^export /gm, '');
  const context = vm.createContext({ Request, Response, Headers, URL, TextEncoder, TextDecoder, Uint8Array, btoa, atob });
  vm.runInContext(source + '\nglobalThis.api = {buildBindings, makeKV, makeDOTransport};', context);
  return context.api;
}

for (const kind of ['kv', 'd1', 'r2', 'queue', 'workflow', 'vectorize', 'do']) {
  test(`${kind} facade refreshes binding JWT per call from claims, not a startup token`, async () => {
    let now = 1700000000;
    const sent = [];
    const authorization = { ns: 'acme', kind, name: 'DATA' };
    if (kind === 'kv') Object.assign(authorization, { worker: 'web', version: 2, resource: 'acme/__kv__/original' });
    const platform = {
      url: 'http://agent',
      authorization: async claims => {
        assert.deepEqual(claims, authorization);
        return 'Bearer ' + await mintJWT(key, 'token/scope', { type: 'binding', sub: 'workerd', ...claims }, now);
      },
      fetcher: async (url, init) => {
        sent.push({ url, ...init });
        return Response.json({ results: [{ rows: [] }], id: 'one' });
      },
    };
    const binding = facades().buildBindings(platform, { DATA: { ns: 'acme', kind, name: 'DATA', worker: 'web', class: 'Room', version: 2, authorization } }).DATA;
    const invoke = () => ({ kv: () => binding.get('key'), d1: () => binding.exec('select 1'), r2: () => binding.head('key'), queue: () => binding.send('hello'), workflow: () => binding.create(), vectorize: () => binding.describe(), do: () => binding.get('one').fetch(new Request('http://tenant/', { headers: { authorization: 'Bearer tenant-secret' } })) })[kind]();
    await invoke(); now += 600; await invoke();
    for (const [i, request] of sent.entries()) {
      const headers = new Headers(request.headers);
      const claims = await verifyJWT(key, headers.get('authorization').slice(7), 'token/scope', 'binding', 1700000000 + i * 600);
      assert.equal(claims.iat, 1700000000 + i * 600);
      assert.equal(claims.name, 'DATA');
      assert.equal(claims.resource, authorization.resource);
      assert.equal(headers.has('x-cellhive-scope-token'), false);
      assert.equal(headers.has('x-cellhive-internal-token'), false);
      assert.equal(JSON.stringify(request).includes(key), false);
      if (kind === 'do') assert.equal(JSON.parse(request.body).request.headers.authorization, 'Bearer tenant-secret');
    }
  });
}

test('facade rejects static startup token rather than sending it as Bearer', async () => {
  const binding = facades().makeKV({ url: 'http://agent', fetcher: () => { throw new Error('must not send'); } }, 'acme', 'old-token');
  await assert.rejects(binding.get('key'), /authorization.*claims/i);
});

test('DO direct owner ticket remains outer Bearer and tenant Authorization stays in envelope', async () => {
  const calls = [];
  const platform = { url: 'http://agent', authorization: async () => 'Bearer binding-jwt',
    fetcher: async (url, init) => {
      calls.push({ url, ...init });
      return new Response('ok', { headers: { 'x-cellhive-do-owner': 'http://owner', 'x-cellhive-do-owner-ticket': 'owner-jwt', 'x-cellhive-do-owner-exp': String(Date.now() + 60000) } });
    } };
  const transport = facades().makeDOTransport(platform, { ns: 'acme', worker: 'web', class: 'Room', authorization: { ns: 'acme', kind: 'do', name: 'ROOM' } });
  for (let i = 0; i < 2; i++) await transport.__call('one', new Request('http://tenant/path', { headers: { authorization: 'Bearer tenant-secret' } }));
  assert.equal(new Headers(calls[0].headers).get('authorization'), 'Bearer binding-jwt');
  assert.equal(new Headers(calls[1].headers).get('authorization'), 'Bearer owner-jwt');
  assert.equal(calls[1].url, 'http://owner/v1/do/invoke');
  for (const call of calls) assert.equal(JSON.parse(call.body).request.headers.authorization, 'Bearer tenant-secret');
});

test('DO WebSocket transport uses ticket Bearer and protected tenant auth envelope', async () => {
  let sent;
  const platform = { url: 'http://agent', authorization: async () => 'Bearer binding-jwt',
    fetcher: async (url, init) => {
      if (url.startsWith('http://agent')) return Response.json({ owner: 'http://owner', ticket: 'owner-jwt' });
      sent = new Headers(init.headers);
      return new Response('upgrade');
    } };
  const transport = facades().makeDOTransport(platform, { ns: 'acme', worker: 'web', class: 'Room', authorization: { ns: 'acme', kind: 'do', name: 'ROOM' } });
  await transport.__call('one', new Request('http://tenant/', { headers: { upgrade: 'websocket', authorization: 'Bearer tenant-secret', 'x-cellhive-app-authorization': 'spoof' } }));
  assert.equal(sent.get('authorization'), 'Bearer owner-jwt');
  assert.equal(sent.get('x-cellhive-app-authorization'), 'Bearer tenant-secret');
  assert.equal(sent.has('x-cellhive-do-ticket'), false);
});
