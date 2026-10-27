export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/perf-ready') return new Response('ok');
    const key = url.searchParams.get('key') || 'k0';
    if (url.pathname === '/put') {
      await env.KV.put(key, await req.text());
      return new Response('ok');
    }
    if (url.pathname === '/get') {
      const value = await env.KV.get(key);
      return new Response(value, {status: value === null ? 404 : 200});
    }
    return new Response('not found', {status:404});
  },
};
