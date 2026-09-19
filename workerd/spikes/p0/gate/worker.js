// workerd output-gate spike worker (ADR-051).
// Each DO request: SQL write -> wait for the cell-agent durability proof via the
// GATE service binding -> return. workerd is not modified; the gate is an
// external service (cmd/cell-supervisor).
export class A {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    state.blockConcurrencyWhile(async () => {
      state.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, n INTEGER)"
      );
    });
  }

  async fetch() {
    const sql = this.state.storage.sql;
    sql.exec("INSERT INTO t (id,n) VALUES (1,0) ON CONFLICT(id) DO UPDATE SET n=n+1");
    // Output gate: block the response until the actor WAL commit is proven
    // durable by the cell-agent fleet.
    const r = await this.env.GATE.fetch("http://gate/sync", { method: "POST" });
    if (!r.ok) return new Response("gate failed: " + r.status, { status: 502 });
    const proof = await r.json();
    const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
    return new Response(JSON.stringify({ n: rows[0].n, proof: proof.mode }));
  }
}

export default {
  async fetch(req, env) {
    return env.A.get(env.A.idFromName("a")).fetch(req);
  },
};
