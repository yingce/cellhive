// workerd multi-DO output-gate worker (ADR-051 scaling test).
// Routes GET /<name> to a distinct Durable Object actor (idFromName(name)).
// Each actor does a SQL write, then waits for the cell-agent fleet proof via the
// GATE service binding. The actor's state.id == the on-disk DB filename, so the
// supervisor can locate the actor -wal directly from the id sent in ?id=.
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
    const q =
      "http://gate/sync?actor=" + encodeURIComponent(this.name) +
      "&id=" + this.state.id.toString();
    const r = await this.env.GATE.fetch(q, { method: "POST" });
    if (!r.ok) return new Response("gate failed: " + r.status, { status: 502 });
    const proof = await r.json();
    const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
    return new Response(
      JSON.stringify({ n: rows[0].n, proof: proof.mode, actor: this.name })
    );
  }
}

export default {
  async fetch(req, env) {
    const name = decodeURIComponent(new URL(req.url).pathname.slice(1)) || "a";
    return env.A.get(env.A.idFromName(name)).fetch(req);
  },
};
