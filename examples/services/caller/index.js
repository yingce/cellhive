// services/caller: 通过 service binding 调同一命名空间的目标 Worker：
//   env.API.fetch(...)          普通 fetch
//   await env.API.greet("bob")  原生 JSRPC 到入口类（结构化克隆）
export default {
  async fetch(request, env) {
    const r = await env.API.fetch(new Request("http://api/"));
    const hi = await env.API.greet("bob");
    const total = await env.API.sum(2, 3);
    return Response.json({ fetched: await r.text(), rpc: hi, sum: total });
  },
};
