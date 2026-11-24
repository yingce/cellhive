import { DurableObject } from 'cloudflare:workers';
export class AlarmProbe extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/arm') {
      await this.ctx.storage.put('failOnce', url.searchParams.get('fail') === '1');
      await this.ctx.storage.setAlarm(Date.now() + Number(url.searchParams.get('delay') || 1500));
    }
    if (url.pathname === '/cancel') await this.ctx.storage.deleteAlarm();
    return Response.json({ attempts: (await this.ctx.storage.get('attempts')) || 0,
      completed: (await this.ctx.storage.get('completed')) || 0, due: await this.ctx.storage.getAlarm() });
  }
  async alarm() {
    await this.ctx.storage.put('attempts', ((await this.ctx.storage.get('attempts')) || 0) + 1);
    if (await this.ctx.storage.get('failOnce')) {
      await this.ctx.storage.put('failOnce', false);
      throw new Error('intentional scheduling E2E first attempt failure');
    }
    await this.ctx.storage.put('completed', ((await this.ctx.storage.get('completed')) || 0) + 1);
  }
}
export default {
  async scheduled(controller, env) {
    await env.KV.put('cron', JSON.stringify({ scheduledTime: controller.scheduledTime, cron: controller.cron }));
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      if (message.body.fail && message.attempts === 1) { message.retry({ delaySeconds: 1 }); continue; }
      await env.KV.put(message.body.key || (message.body.fail ? 'queueRetry' : 'queue'), JSON.stringify({ attempts: message.attempts }));
      message.ack();
    }
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/send') {
      await env.QUEUE.send({ fail: url.searchParams.get('fail') === '1', key: url.searchParams.get('key') }, { delaySeconds: Number(url.searchParams.get('delay') || 0) });
      return new Response('sent');
    }
    if (url.pathname === '/status') return Response.json({
      queue: JSON.parse(await env.KV.get('queue') || 'null'),
      queueRetry: JSON.parse(await env.KV.get('queueRetry') || 'null'),
      outage: JSON.parse(await env.KV.get('outage') || 'null'),
      cron: JSON.parse(await env.KV.get('cron') || 'null'),
    });
    return new Response('scheduling-ok');
  },
};
