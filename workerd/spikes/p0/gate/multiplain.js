// workerd multi-DO control worker: same routing + SQL write, NO output gate.
// Used to separate workerd's multi-actor throughput from the ADR-051 gate cost.
export class A {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.name = null;
    state.blockConcurrencyWhile(async () => {
      state.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, n INTEGER)"
      );
    });
  }

  async fetch(req) {
    const u = new URL(req.url);
    if (this.name === null) {
      this.name = decodeURIComponent(u.pathname.slice(1)) || "a";
    }
    const sql = this.state.storage.sql;
    sql.exec("INSERT INTO t (id,n) VALUES (1,0) ON CONFLICT(id) DO UPDATE SET n=n+1");
    const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
    return new Response(JSON.stringify({ n: rows[0].n, actor: this.name }));
  }
}

export default {
  async fetch(req, env) {
    const name = decodeURIComponent(new URL(req.url).pathname.slice(1)) || "a";
    return env.A.get(env.A.idFromName(name)).fetch(req);
  },
};
