// cron: scheduled 触发器（triggers.crons）。
export default {
  async fetch() { return new Response("see scheduled(); runs every minute\n"); },
  async scheduled(controller, env, ctx) {
    console.log("cron fired", controller.cron, controller.scheduledTime);
    ctx.waitUntil(Promise.resolve());
  },
};
