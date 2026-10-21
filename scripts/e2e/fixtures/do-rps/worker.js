import { DurableObject } from "cloudflare:workers";

export class Bench extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
  }

  async fetch(request) {
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") return this.ws();
    const path = new URL(request.url).pathname;
    if (path === "/inc") {
      this.ctx.storage.sql.exec("INSERT INTO bench(id, v) VALUES(1, 1) ON CONFLICT(id) DO UPDATE SET v=v+1");
      const row = [...this.ctx.storage.sql.exec("SELECT v FROM bench WHERE id=1")][0];
      return Response.json({ v: Number(row.v) });
    }
    if (path === "/get") {
      const row = [...this.ctx.storage.sql.exec("SELECT v FROM bench WHERE id=1")][0];
      return Response.json({ v: Number(row?.v || 0) });
    }
    return new Response("use /inc /get or WebSocket", { status: 404 });
  }

  ws() {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(ws, message) {
    ws.send(message);
  }
}

export default {
  async fetch(request, env) {
    const shard = new URL(request.url).searchParams.get("shard") || "0";
    return env.BENCH.get(env.BENCH.idFromName("bench-" + shard)).fetch(request);
  },
};
