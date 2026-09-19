// do-rpc: DO RPC（extends DurableObject），env.NS.getByName("x").method(...)。
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  async increment(amount = 1) {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + amount;
    await this.ctx.storage.put("n", n);
    return n;
  }
  async value() { return (await this.ctx.storage.get("n")) ?? 0; }
}

export default {
  async fetch(request, env) {
    const counter = env.COUNTER.getByName("main");
    if (request.method === "POST") return Response.json({ n: await counter.increment(1) });
    return Response.json({ n: await counter.value() });
  },
};
