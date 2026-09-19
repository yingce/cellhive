// Batched output-gate worker (ADR-051 / fsync experiment).
// GET /batch?k=K[&gate=0] -> K SQL writes in one event (workerd coalesces them
// into one commit), then ONE gate fetch for the whole batch.
// GET /read               -> current counter (no write, no gate).
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

  async fetch(req) {
    const u = new URL(req.url);
    const sql = this.state.storage.sql;
    if (u.pathname === "/read") {
      const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
      return new Response(JSON.stringify({ n: rows[0] ? rows[0].n : 0 }));
    }
    const k = parseInt(u.searchParams.get("k") || "1", 10);
    const useGate = u.searchParams.get("gate") !== "0";
    const stmt = "INSERT INTO t (id,n) VALUES (1,0) ON CONFLICT(id) DO UPDATE SET n=n+1";
    for (let i = 0; i < k; i++) sql.exec(stmt);
    let proof = "none";
    if (useGate) {
      const r = await this.env.GATE.fetch("http://gate/sync", { method: "POST" });
      if (!r.ok) return new Response("gate failed: " + r.status, { status: 502 });
      proof = (await r.json()).mode;
    }
    const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
    return new Response(JSON.stringify({ n: rows[0].n, k, proof }));
  }
}

export default {
  async fetch(req, env) {
    return env.A.get(env.A.idFromName("a")).fetch(req);
  },
};
