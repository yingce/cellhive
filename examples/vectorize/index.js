// vectorize: 注册索引 + facade insert/query/queryById。
export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/seed") {
      await env.VEC.insert([
        { id: "a", values: [1, 0, 0], metadata: { name: "red" } },
        { id: "b", values: [0, 1, 0], metadata: { name: "green" } },
      ]);
      return new Response("seeded\n");
    }
    const q = (u.searchParams.get("v") || "1,0,0").split(",").map(Number);
    const r = await env.VEC.query(q, { topK: 1, returnMetadata: true });
    return Response.json(r);
  },
};
