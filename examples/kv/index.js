// kv: KV put/get/delete/list + TTL + metadata。
export default {
  async fetch(request, env) {
    const key = new URL(request.url).pathname.slice(1);
    if (!key) return new Response("Use /KEY\n", { status: 400 });
    switch (request.method) {
      case "PUT":
      case "POST": {
        await env.KV.put(key, request.body, { expirationTtl: 3600, metadata: { at: Date.now() } });
        return new Response(null, { status: 204 });
      }
      case "DELETE":
        await env.KV.delete(key);
        return new Response(null, { status: 204 });
      default: {
        const v = await env.KV.getWithMetadata(key);
        const list = await env.KV.list({ prefix: "", limit: 10, include: ["metadata"] });
        return Response.json({ key, value: v.value, metadata: v.metadata, keys: list.keys.length });
      }
    }
  },
};
