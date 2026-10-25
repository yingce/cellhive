import { WorkflowEntrypoint } from 'cloudflare:workers';
export class EventWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.waitForEvent('ready', { type: 'ready', timeout: '5 minutes' });
  }
}
export class ParallelWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    await step.sleep('collect', '15 seconds');
    return await Promise.all(Array.from({ length: 16 }, (_, i) =>
      step.waitForEvent('event-' + i, { type: 'ready', timeout: '5 minutes' })));
  }
}
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === '/ping') return new Response('ok');
    const binding = u.searchParams.get('kind') === 'parallel' ? env.PARALLEL : env.EVENT;
    const id = u.searchParams.get('id');
    if (u.pathname === '/create') return Response.json(await binding.create({ id, params: {} }));
    const instance = await binding.get(id);
    if (u.pathname === '/event') { await instance.sendEvent({ type: 'ready', payload: { n: Number(u.searchParams.get('n')) } }); return new Response('ok'); }
    if (u.pathname === '/restart') { await instance.restart(); return new Response('ok'); }
    return Response.json(await instance.status());
  },
};
