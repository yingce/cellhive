export default {
  async fetch(req, env) {
    await env.KV.put('k', 'v');
    const kv = await env.KV.get('k');
    const row = await env.DB.prepare('SELECT 1 AS ok').first();
    await env.BUCKET.put('r', 'v', { httpMetadata: { contentType: 'text/x-r2' } });
    const obj = await env.BUCKET.get('r');
    const h = new Headers();
    obj.writeHttpMetadata(h);
    return Response.json({ kv, d1: row && row.ok, r2: h.get('content-type') || 'missing' });
  },
};
