export default {
  async queue(batch, env) {
    if ((batch.queue || '') === 'q1' && batch.messages.some((m) => m.body && m.body.fail === true)) {
      for (const m of batch.messages) { try { m.retry({ delaySeconds: 0 }); } catch (e) {} }
      return;
    }
    await env.BUCKET.put('qmeta', 'v', { httpMetadata: { contentType: 'text/x-queue' } });
    const obj = await env.BUCKET.get('qmeta');
    const h = new Headers();
    obj.writeHttpMetadata(h);
    await env.KV.put('qmeta', h.get('content-type') || 'missing');
    for (const m of batch.messages) { try { m.ack(); } catch (e) {} }
  },
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    if (p === '/send') { await env.QUEUE.send({ ok: 1 }); return new Response('sent'); }
    if (p === '/fail') { await env.QUEUE.send({ fail: true }); return new Response('retried'); }
    if (p === '/status') return new Response((await env.KV.get('qmeta')) || 'pending');
    return new Response('q-ok');
  },
};
