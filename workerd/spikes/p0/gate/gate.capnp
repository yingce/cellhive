# workerd output-gate spike config (ADR-051).
# The DO worker binds GATE to an external service (cmd/cell-supervisor) and
# waits for it before returning. No workerd changes.
#
#   cell-supervisor -db <do-storage>/gate/<hash>.sqlite ...
#   workerd serve --experimental workerd/spikes/p0/gate/gate.capnp
#   curl localhost:18805/

using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "worker", esModule = embed "worker.js")],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [(className = "A", uniqueKey = "gate", enableSql = true)],
      durableObjectStorage = (localDisk = "do-storage"),
      bindings = [
        (name = "A", durableObjectNamespace = "A"),
        (name = "GATE", service = "gate"),
      ],
    )),

    (name = "gate", external = (address = "127.0.0.1:18900", http = ())),

    (name = "do-storage", disk = (path = "/tmp/cellhive-gate/do", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18805", http = (), service = "main"),
  ],
);
