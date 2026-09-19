// d1: SQL 建表 / 参数化插入 / 查询 / batch / meta.last_row_id。
const SCHEMA = `CREATE TABLE IF NOT EXISTS notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL, at INTEGER NOT NULL)`;

export default {
  async fetch(request, env) {
    await env.DB.exec(SCHEMA);
    if (request.method === "POST") {
      const { body } = await request.json();
      const r = await env.DB.prepare("INSERT INTO notes (body, at) VALUES (?, ?)").bind(body, Date.now()).run();
      return Response.json({ id: r.meta.last_row_id, changes: r.meta.changes });
    }
    const n = await env.DB.prepare("SELECT count(*) AS n FROM notes").first("n");
    const { results } = await env.DB.prepare("SELECT id, body FROM notes ORDER BY id DESC LIMIT 20").all();
    return Response.json({ count: n, notes: results });
  },
};
