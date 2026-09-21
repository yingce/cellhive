// Bounded per-request log tail for loaded workers.
//
// Imported FIRST by the platform wrapper (before the tenant module) so tenant
// module-level console output is captured too. It patches console.*, buffers
// lines, and ships them to cell-agent's bounded log buffer via the PLATFORM
// service binding. Non-durable; the LOG_TOKEN only authorizes this endpoint.
import { env } from "cloudflare:workers";

const levels = ["log", "info", "warn", "error", "debug"];
const MAX_BATCH = 64;
const MAX_LINE = 4096;

function fmt(a) {
  if (typeof a === "string") return a;
  try {
    return JSON.stringify(a);
  } catch (e) {
    return String(a);
  }
}

export function installLogTail() {
  // LOG_TOKEN arrives via the module-scope __cellhivePlatform const (ADR-074):
// role credentials never enter the tenant env. LOG_NS/LOG_WORKER stay in env
// (plain labels, not secrets).
  // The sink is a platform-side entrypoint stub (fixed path + internal token);
  // the tenant isolate holds no transport or role credential (ADR-074).
  const sink = env && env.CH_LOG_SINK;
  if (!sink || typeof sink.send !== "function") {
    return;
  }
  if (globalThis.__cellhiveLogTail) return;
  globalThis.__cellhiveLogTail = true;

  const orig = {};
  for (const lvl of levels) orig[lvl] = console[lvl] ? console[lvl].bind(console) : () => {};
  let buf = [];

  const flush = (ctx) => {
    if (buf.length === 0) return;
    const batch = buf;
    buf = [];
    try {
      const done = Promise.resolve(sink.send(JSON.stringify(batch))).catch(() => {});
      // Bind the send to the request lifetime: workerd cancels un-awaited work
      // once the response is returned (the ADR-115 lesson), which would drop logs.
      if (ctx && typeof ctx.waitUntil === "function") ctx.waitUntil(done);
    } catch (e) {
      /* never take down the worker for logging */
    }
  };
  globalThis.__cellhiveLogFlush = flush;

  for (const lvl of levels) {
    console[lvl] = (...args) => {
      try { orig[lvl](...args); } catch (e) { /* ignore */ }
      let msg = "";
      try { msg = args.map(fmt).join(" ").slice(0, MAX_LINE); } catch (e) { msg = "<unserializable>"; }
      buf.push({ level: lvl, message: msg, traceparent: globalThis.__cellhiveTraceparent || "" });
      if (buf.length >= MAX_BATCH) {
        flush();
      } else {
        // Flush on the next microtask so Durable Object facets (which may not run
        // timers while idle) still ship their logs within the request.
        try { queueMicrotask(() => flush()); } catch (e) { flush(); }
      }
    };
  }
  try {
    setInterval(() => flush(), 500);
  } catch (e) {
    /* no timers */
  }
}

installLogTail();
