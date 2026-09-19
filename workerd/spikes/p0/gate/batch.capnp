# Batched output-gate config, port 18805 (disk /tmp/cellhive-batch/do).
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "worker", esModule = embed "batch.js")],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [(className = "A", uniqueKey = "gate", enableSql = true)],
      durableObjectStorage = (localDisk = "batch-storage"),
      bindings = [
        (name = "A", durableObjectNamespace = "A"),
        (name = "GATE", service = "gate"),
      ],
    )),

    (name = "gate", external = (address = "127.0.0.1:18900", http = ())),

    (name = "batch-storage", disk = (path = "/tmp/cellhive-batch/do", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18805", http = (), service = "main"),
  ],
);
