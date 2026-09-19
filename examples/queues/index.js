// queues: 生产者 send + 消费者 batch（ack/retry）+ DLQ。
export default {
  async fetch(request, env) {
    const n = Number(new URL(request.url).searchParams.get("n") || "1");
    await env.JOBS.send({ n, at: Date.now() });
    return Response.json({ sent: n });
  },
  async queue(batch, env) {
    for (const m of batch.messages) {
      if (typeof m.body?.n !== "number") { m.retry({ delaySeconds: 30 }); continue; }
      m.ack();
    }
  },
};
