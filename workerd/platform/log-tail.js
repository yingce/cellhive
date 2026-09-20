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
  const logToken = (typeof __cellhivePlatform !== "undefined" && __cellhivePlatform.logToken) || env.LOG_TOKEN || "";
  if (!logToken || !env || !env.CELL_URL || !env.LOG_NS || !env.LOG_WORKER) {
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
      const url =
        env.CELL_URL.replace(/\/$/, "") +
        "/v1/internal/logs?ns=" + encodeURIComponent(env.LOG_NS) +
        "&worker=" + encodeURIComponent(env.LOG_WORKER);
      const init = {
        method: "POST",
        headers: { "content-type": "application/json", "x-cellhive-internal-token": logToken },
        body: JSON.stringify(batch),
      };
      const sent = env.PLATFORM && typeof env.PLATFORM.fetch === "function"
        ? env.PLATFORM.fetch(url, init)
        : fetch(url, init);
      const done = sent.catch(() => {});
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
