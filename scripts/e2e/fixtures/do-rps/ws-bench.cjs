// WebSocket echo load generator. One in-flight message per socket measures
// round-trip message throughput, not just fan-out.
const WebSocket = require("ws");

const n = Number(process.argv[2] || 32);
const seconds = Number(process.argv[3] || 8);
const url = process.argv[4] || "ws://127.0.0.1:8081/";
const host = process.argv[5] || "do-bench.local";
const shards = Number(process.argv[6] || 1);
let connected = 0, closed = 0, errors = 0, messages = 0;
const sockets = [];
const started = Date.now();
const deadline = started + seconds * 1000;

for (let i = 0; i < n; i++) {
  const shard = Math.floor(Math.random() * shards);
  const wsUrl = url + (url.includes("?") ? "&" : "?") + "shard=" + shard;
  const ws = new WebSocket(wsUrl, { headers: { Host: host } });
  sockets.push(ws);
  ws.on("open", () => {
    connected++;
    ws.send(`probe-${i}`);
  });
  ws.on("message", () => {
    messages++;
    if (Date.now() < deadline) ws.send(`probe-${i}`);
  });
  ws.on("error", () => { errors++; });
  ws.on("close", () => { closed++; });
}

setTimeout(() => {
  const elapsed = (Date.now() - started) / 1000;
  for (const ws of sockets) { try { ws.close(); } catch {} }
  setTimeout(() => {
    console.log(JSON.stringify({
      sockets: n, connected, closed, errors, messages, seconds: elapsed,
      messages_per_sec: messages / elapsed,
    }, null, 2));
    process.exit(0);
  }, 250);
}, seconds * 1000);
