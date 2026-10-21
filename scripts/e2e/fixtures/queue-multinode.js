export default {
  async queue(batch, env) {
    if ((batch.queue || '') === 'q1' && batch.messages.some((m) => m.body && m.body.fail === true)) {
      for (const m of batch.messages) { try { m.retry({ delaySeconds: 0 }); } catch (e) {} }
      return;
    }
    const n = Number((await env.KV.get('consumed')) || 0) + batch.messages.length;
    await env.KV.put('consumed', String(n));
    for (const m of batch.messages) { try { m.ack(); } catch (e) {} }
  },
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    if (p === '/send') { await env.QUEUE.send({ ok: 1 }); return new Response('sent'); }
    if (p === '/fail') { await env.QUEUE.send({ fail: true }); return new Response('retried'); }
    if (p === '/status') return Response.json({ n: Number((await env.KV.get('consumed')) || 0) });
    return new Response('qm-ok');
  },
};
