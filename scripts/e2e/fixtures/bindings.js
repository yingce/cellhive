export default {
  async fetch(req, env) {
    await env.KV.put('k', 'v');
    const kv = await env.KV.get('k');
    // A second binding name for the same KV resource id must observe the first
    // write; addressing by the env key instead of the id would use two cells.
    const kvAlias = await env.KV_ALIAS.get('k');
    const row = await env.DB.prepare('SELECT 1 AS ok').first();
    await env.R2A.put('r', 'v', { httpMetadata: { contentType: 'text/x-r2' } });
    // A second binding to the same bucket_name must observe the first write;
    // addressing by the env key instead of bucket_name would use two buckets.
    const obj = await env.R2B.get('r');
    const h = new Headers();
    obj.writeHttpMetadata(h);
    return Response.json({ kv, kvAlias, d1: row && row.ok, r2: h.get('content-type') || 'missing' });
  },
};
