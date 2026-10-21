export default {
  async queue(batch, env) {
    const start = Date.now();
    await env.DB.batch(batch.messages.map((m) =>
      env.DB.prepare('INSERT OR IGNORE INTO seen (id, started_ms) VALUES (?, ?)').bind(m.id, start)));
    await new Promise((resolve) => setTimeout(resolve, 100));
    const end = Date.now();
    await env.DB.batch(batch.messages.map((m) =>
      env.DB.prepare('UPDATE seen SET ended_ms = ? WHERE id = ?').bind(end, m.id)));
    for (const m of batch.messages) { try { m.ack(); } catch (e) {} }
  },
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/init') {
      await env.DB.exec('CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY, started_ms INTEGER, ended_ms INTEGER)');
      return new Response('init');
    }
    if (url.pathname === '/send') {
      const n = Number(url.searchParams.get('n') || 200);
      const t0 = Date.now();
      for (let i = 0; i < n; i++) await env.QUEUE.send({ seq: i });
      return Response.json({ sent: n, send_ms: Date.now() - t0 });
    }
    if (url.pathname === '/send-parallel') {
      const n = Number(url.searchParams.get('n') || 20);
      const t0 = Date.now();
      await Promise.all(Array.from({ length: n }, (_, i) => env.QUEUE.send({ seq: i })));
      return Response.json({ sent: n, send_ms: Date.now() - t0, parallel: true });
    }
    if (url.pathname === '/stats') {
      const rows = (await env.DB.prepare('SELECT id, started_ms, ended_ms FROM seen').all()).results || [];
      const events = [];
      let missing = 0;
      for (const r of rows) {
        events.push([r.started_ms, 1]);
        if (r.ended_ms === null || r.ended_ms === undefined) { missing++; continue; }
        events.push([r.ended_ms, -1]);
      }
      events.sort((a, b) => a[0] - b[0] || b[1] - a[1]);
      let cur = 0, max = 0;
      for (const e of events) { cur += e[1]; if (cur > max) max = cur; }
      return Response.json({ count: rows.length, max_concurrent: max, missing_end: missing });
    }
    return new Response('ec-ok');
  },
};
