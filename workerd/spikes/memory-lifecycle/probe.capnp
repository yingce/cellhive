using W = import "/workerd/workerd.capnp";
const config :W.Config = (
  services = [(name = "host", worker = (
    modules = [(name = "host.js", esModule = embed "host.js")],
    compatibilityDate = "2026-06-15",
    bindings = [(name = "LOADER", workerLoader = (id = "lifecycle-probe"))]
  ))],
  sockets = [(name = "http", address = "*:8099", http = (), service = "host")]
);
