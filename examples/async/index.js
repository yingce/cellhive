// async: 异步 DO 存储 + setTimeout + ctx.waitUntil。
export class AsyncCounter {
  constructor(state) { this.state = state; }
  async fetch() {
    await new Promise((r) => setTimeout(r, 50));
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    return Response.json({ n });
  }
}
export default {
  async fetch(request, env, ctx) {
    ctx.waitUntil(Promise.resolve("background work"));
    return env.COUNTER.get(env.COUNTER.idFromName("counter")).fetch(request);
  },
};
