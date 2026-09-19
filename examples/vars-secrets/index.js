// vars-secrets: env 里的 vars 与 secret（secret 用 `cellhive secret put` 注入）。
export default {
  async fetch(request, env) {
    return Response.json({
      mode: env.MODE,
      hasToken: typeof env.TOKEN === "string" && env.TOKEN.length > 0,
    });
  },
};
