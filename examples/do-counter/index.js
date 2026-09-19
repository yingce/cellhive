// do-counter: 经典 DO 类（constructor(state, env)），SQLite storage 持久计数。
export class Counter {
  constructor(state, env) { this.state = state; this.env = env; }
  async fetch(request) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    return Response.json({ name, n });
  }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "default";
    return env.COUNTER.get(env.COUNTER.idFromName(name)).fetch(request);
  },
};
