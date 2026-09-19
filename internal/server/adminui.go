package server

import (
	"net/http"
)

// handleAdminUI serves a minimal, dependency-free admin console (ADR-036: a
// single control plane for a trusted operator, not a tenant portal). The page is
// static and calls the existing JSON admin endpoints with the operator token.
func (s *Server) handleAdminUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(adminHTML))
}

const adminHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CellHive admin</title>
<style>
  :root { color-scheme: dark; }
  body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; background:#0b0e14; color:#e6edf3; margin:0; padding:24px; }
  h1 { font-size:18px; margin:0 0 16px; }
  label { color:#8b949e; }
  input, button { font: inherit; background:#111827; color:#e6edf3; border:1px solid #30363d; border-radius:6px; padding:6px 10px; }
  button { cursor:pointer; background:#1f6feb; border-color:#1f6feb; }
  section { margin-top:20px; }
  pre { background:#111827; border:1px solid #30363d; border-radius:8px; padding:12px; overflow:auto; max-height:50vh; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
</style>
</head>
<body>
<h1>CellHive admin</h1>
<div class="row">
  <label>Operator token</label>
  <input id="tok" type="password" placeholder="admin token" style="min-width:320px">
  <button onclick="loadAll()">Refresh</button>
</div>

<section><h2>Status</h2><pre id="status">—</pre></section>
<section><h2>Capacity (autoscaler signal)</h2><pre id="capacity">—</pre></section>
<section><h2>Release log</h2>
  <div class="row">
    <input id="ns" placeholder="namespace">
    <input id="worker" placeholder="worker">
    <button onclick="loadReleases()">Load releases</button>
  </div>
  <pre id="releases">—</pre>
</section>

<script>
async function call(path) {
  const token = document.getElementById('tok').value;
  const r = await fetch(path, { headers: { 'x-cellhive-admin-token': token } });
  return r.status + ' ' + (await r.text());
}
async function show(id, path) {
  try { document.getElementById(id).textContent = await call(path); }
  catch (e) { document.getElementById(id).textContent = String(e); }
}
function loadAll() {
  show('status', '/v1/control/status');
  show('capacity', '/v1/control/capacity');
}
function loadReleases() {
  const ns = encodeURIComponent(document.getElementById('ns').value);
  const worker = encodeURIComponent(document.getElementById('worker').value);
  show('releases', '/v1/control/releases?namespace=' + ns + '&worker=' + worker);
}
</script>
</body>
</html>
`
