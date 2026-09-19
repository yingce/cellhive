// do-websocket: 可休眠 WebSocket（hibernation API）+ 按消息持久计数。
export class Echo {
  constructor(state) { this.state = state; }
  async fetch(request) {
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return new Response("websocket upgrade required\n", { status: 426 });
    }
    const pair = new WebSocketPair();
    this.state.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[1] });
  }
  async webSocketMessage(ws, msg) {
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    ws.send(JSON.stringify({ echo: msg, n }));
  }
}
export default {
  async fetch(request, env) { return env.ECHO.get(env.ECHO.idFromName("w")).fetch(request); },
};
