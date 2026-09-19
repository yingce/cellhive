// Platform-side group-commit wrapper for Durable Objects (CellHive do-runtime).
//
// Why: workerd auto-coalesces all writes of ONE DO event into one commit, so a
// workload of "one write per HTTP request" is capped at ~900 commits/s per
// workerd process (fsync bound). Putting more writes into the same event
// amortizes the fsync.
//
// This wrapper buffers concurrent submit(op) calls, applies a whole batch inside
// one DO event (one commit), takes ONE output-gate proof for the batch, then
// resolves every submitter. Tenant code only declares its mutation as
// apply(op); it does not need to know about batching or the gate.
//
// Usage (inside a DO class):
//
//   this.gc = new GroupCommit({
//     state, env,
//     gate: env.GATE,                 // optional service binding with /sync
//     windowMs: 3, maxBatch: 512,
//     apply: (op) => { /* one mutation, e.g. sql.exec(...) */ },
//   });
//   async fetch(req) {
//     await this.gc.submit(opFromRequest(req));  // durable + gated when it resolves
//   }
//
// Semantics: submit() resolves only after the batch containing this op has been
// applied AND (if gate is set) proven durable by the cell-agent fleet. All ops
// in a batch share one commit, so they are all-or-nothing — the same guarantee
// workerd gives for writes within one event.

export class GroupCommit {
  constructor({ state, env, apply, gate = null, windowMs = 3, maxBatch = 512 } = {}) {
    if (typeof apply !== "function") {
      throw new Error("GroupCommit: apply(op) is required");
    }
    this.state = state;
    this.env = env;
    this.apply = apply;
    this.gate = gate;
    this.windowMs = windowMs;
    this.maxBatch = maxBatch;
    this.pending = [];
    this.flushing = false;
    this.stats = { batches: 0, ops: 0, commits: 0, maxObserved: 0 };
  }

  // submit buffers op and resolves with the size of the batch it committed in.
  submit(op) {
    return new Promise((resolve, reject) => {
      this.pending.push({ op, resolve, reject });
      this._kick();
    });
  }

  _kick() {
    if (this.flushing) return;
    this.flushing = true;
    this._flush().catch(() => {});
  }

  async _flush() {
    try {
      while (this.pending.length > 0) {
        if (this.windowMs > 0 && this.pending.length < this.maxBatch) {
          await delay(this.windowMs);
        }
        const batch = this.pending.splice(0, this.maxBatch);
        try {
          for (const b of batch) this.apply(b.op);
          if (this.gate) {
            const r = await this.gate.fetch("http://gate/sync", { method: "POST" });
            if (!r.ok) throw new Error("gate failed: " + r.status);
            await r.json();
            this.stats.commits++;
          }
          this.stats.batches++;
          this.stats.ops += batch.length;
          if (batch.length > this.stats.maxObserved) this.stats.maxObserved = batch.length;
          for (const b of batch) b.resolve(batch.length);
        } catch (e) {
          for (const b of batch) b.reject(e);
        }
      }
    } finally {
      this.flushing = false;
      if (this.pending.length > 0) this._kick();
    }
  }
}

function delay(ms) {
  if (typeof scheduler !== "undefined" && typeof scheduler.wait === "function") {
    return scheduler.wait(ms);
  }
  if (typeof setTimeout === "function") {
    return new Promise((r) => setTimeout(r, ms));
  }
  return Promise.resolve();
}
