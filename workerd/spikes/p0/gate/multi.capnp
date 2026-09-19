# workerd multi-DO output-gate config (ADR-051 scaling test).
# One workerd, one DO namespace, many actors (GET /<name> -> idFromName(name)).
# All actors gate through the same external supervisor (cmd/cell-supervisor
# -actor-dir ...), which locates each actor's -wal by the id the worker sends.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "worker", esModule = embed "multi.js")],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [(className = "A", uniqueKey = "gate", enableSql = true)],
      durableObjectStorage = (localDisk = "multi-storage"),
      bindings = [
        (name = "A", durableObjectNamespace = "A"),
        (name = "GATE", service = "gate"),
      ],
    )),

    (name = "gate", external = (address = "127.0.0.1:18900", http = ())),

    (name = "multi-storage", disk = (path = "/tmp/cellhive-multi/do", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18805", http = (), service = "main"),
  ],
);
