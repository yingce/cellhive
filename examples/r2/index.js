// r2: put（含 http/custom metadata）/ get / head / list / delete。
export default {
  async fetch(request, env) {
    const key = new URL(request.url).pathname.slice(1);
    if (!key) return new Response("Use /KEY\n", { status: 400 });
    if (request.method === "PUT") {
      await env.FILES.put(key, request.body, {
        httpMetadata: { contentType: request.headers.get("content-type") || "application/octet-stream" },
        customMetadata: { uploadedBy: "r2-example" },
      });
      return new Response(null, { status: 204 });
    }
    if (request.method === "DELETE") { await env.FILES.delete(key); return new Response(null, { status: 204 }); }
    if (new URL(request.url).pathname === "/") {
      const list = await env.FILES.list({ limit: 20, include: ["httpMetadata"] });
      return Response.json({ objects: list.objects.map((o) => ({ key: o.key, size: o.size })), truncated: list.truncated });
    }
    const obj = await env.FILES.get(key);
    if (!obj) return new Response("not found\n", { status: 404 });
    const headers = new Headers();
    obj.writeHttpMetadata(headers);
    headers.set("etag", obj.httpEtag);
    headers.set("x-size", String(obj.size));
    return new Response(obj.body, { headers });
  },
};
