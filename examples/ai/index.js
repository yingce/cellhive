// ai: BYO AI 端点（平台设置 CELLHIVE_AI_URL/_KEY），env.AI.run(model, input)。
export default {
  async fetch(request, env) {
    if (!env.AI) return new Response("AI binding not configured\n", { status: 503 });
    const prompt = new URL(request.url).searchParams.get("prompt") || "say hi";
    const out = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
      messages: [{ role: "user", content: prompt }],
    });
    return Response.json(out);
  },
};
