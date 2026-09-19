// do-alarm: DO alarm（平台 shim；由统一 timer + waker 派发）。
export class Alarm {
  constructor(state) { this.state = state; }
  async fetch(request) {
    if (new URL(request.url).pathname === "/arm") {
      await this.state.storage.setAlarm(Date.now() + 2000);
      return Response.json({ armed: true, at: await this.state.storage.getAlarm() });
    }
    const fires = (await this.state.storage.get("fires")) ?? 0;
    return Response.json({ fires, pending: await this.state.storage.getAlarm() });
  }
  async alarm(info) {
    const fires = (await this.state.storage.get("fires")) ?? 0;
    await this.state.storage.put("fires", fires + 1);
    await this.state.storage.put("lastRetry", info?.retryCount ?? 0);
  }
}
export default {
  async fetch(request, env) { return env.ALARM.get(env.ALARM.idFromName("a1")).fetch(request); },
};
