export default {
  async fetch(req, env) {
    const row = await env.DB.prepare('SELECT COUNT(*) AS n FROM store_inspections').first();
    return Response.json({ n: row && row.n });
  },
};
