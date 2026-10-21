import { WorkflowEntrypoint } from 'cloudflare:workers';

export class SleepWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    await step.sleep('nap', '3 seconds');
    return { slept: true };
  }
}

export class TimeoutWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    await step.waitForEvent('never', { timeout: '2 seconds' });
    return { got: true };
  }
}

export class RetryWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.do('flaky', { retries: { limit: 5, delay: '1 second' } }, async () => {
      const n = Number((await this.env.KV.get('retry-count')) || 0) + 1;
      await this.env.KV.put('retry-count', String(n));
      if (n < 3) throw new Error('flaky attempt ' + n);
      return { attempts: n };
    });
  }
}

export class R2Workflow extends WorkflowEntrypoint {
  async run(event, step) {
    await this.env.BUCKET.put('wmeta', 'v', { httpMetadata: { contentType: 'text/x-workflow' } });
    const obj = await this.env.BUCKET.get('wmeta');
    const h = new Headers();
    obj.writeHttpMetadata(h);
    return { ct: h.get('content-type') || 'missing' };
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/sleep') return Response.json({ id: String((await env.WF_SLEEP.create({ params: {} })).id) });
    if (url.pathname === '/timeout') return Response.json({ id: String((await env.WF_TIMEOUT.create({ params: {} })).id) });
    if (url.pathname === '/retry') return Response.json({ id: String((await env.WF_RETRY.create({ params: {} })).id) });
    if (url.pathname === '/r2') return Response.json({ id: String((await env.WF_R2.create({ params: {} })).id) });
    if (url.pathname === '/status') {
      const kind = url.searchParams.get('kind');
      const id = url.searchParams.get('id');
      const b = kind === 'sleep' ? env.WF_SLEEP : kind === 'timeout' ? env.WF_TIMEOUT : kind === 'r2' ? env.WF_R2 : env.WF_RETRY;
      return Response.json(await (await b.get(id)).status());
    }
    return new Response('w-ok');
  },
};
