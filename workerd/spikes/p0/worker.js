// Minimal Durable Object worker for the P0.7 workerd spike.
// It writes through ctx.storage.sql so we can observe the actor SQLite WAL.
export class Counter {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    state.blockConcurrencyWhile(async () => {
      state.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, n INTEGER)"
      );
    });
  }

  async fetch() {
    const sql = this.state.storage.sql;
    sql.exec(
      "INSERT INTO counter (id, n) VALUES (1, 0) ON CONFLICT(id) DO UPDATE SET n = n + 1"
    );
    const rows = [...sql.exec("SELECT n FROM counter WHERE id = 1")];
    const n = rows.length ? rows[0].n : null;
    return new Response(JSON.stringify({ n }), {
      headers: { "content-type": "application/json" },
    });
  }
}

export default {
  async fetch(req, env) {
    const id = env.COUNTER.idFromName("a");
    const stub = env.COUNTER.get(id);
    return stub.fetch(req);
  },
};
