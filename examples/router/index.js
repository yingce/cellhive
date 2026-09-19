// router: 一个 Worker 把不同路径路由到不同 DO 对象（按名字分片）。
export class Room {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    return Response.json({ room: this.state.id.name, n });
  }
}
export default {
  async fetch(request, env) {
    const room = new URL(request.url).pathname.split("/")[1] || "lobby";
    return env.ROOMS.get(env.ROOMS.idFromName(room)).fetch(request);
  },
};
