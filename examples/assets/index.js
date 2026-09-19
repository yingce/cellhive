// assets: 静态资产 + Worker 回退（env.ASSETS 绑定）。
export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/api") return Response.json({ from: "worker", path: u.pathname });
    return env.ASSETS.fetch(request); // 其余交给资产（含 SPA / _headers / _redirects）
  },
};
