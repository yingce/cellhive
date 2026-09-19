# P0.7 loader config: workerLoader dynamically loads a tenant module that calls
# cell-agent's KV API. Run with `--experimental` (workerLoader is gated).
#
#   workerd serve --experimental workerd/spikes/p0/loader.capnp
#   curl -X POST 'localhost:18789/?key=k' -d hello
#   curl 'localhost:18789/?key=k'

using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [
        (name = "host.js", esModule = embed "host.js"),
        (name = "bindings.js", esModule = embed "../../platform/bindings.js"),
      ],
      compatibilityDate = "2026-04-24",
      bindings = [
        (name = "LOADER", workerLoader = (id = "p0-loader")),
        (name = "OUTBOUND", service = "outbound"),
        (name = "CELL_URL", text = "http://127.0.0.1:7001"),
        (name = "CELL_TOKEN", text = "local-internal-token"),
        (name = "SCOPE_SECRET", text = "local-internal-token"),
      ],
      globalOutbound = "outbound",
    )),

    (name = "outbound", network = (
      allow = ["public", "private"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],

  sockets = [
    (name = "http", address = "*:18789", http = (), service = "main"),
  ],
);
