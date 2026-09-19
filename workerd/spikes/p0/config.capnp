# Minimal stock-workerd config for the P0.7 spike.
# Validates a native Durable Object with SQLite + localDisk (WAL observability).
#
# Run:
#   workerd serve --experimental workerd/spikes/p0/config.capnp
#   curl localhost:18788/            # increments the counter
#   walscan <do-storage>/.../*.sqlite-wal

using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [
        (name = "worker", esModule = embed "worker.js"),
      ],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [
        (className = "Counter", uniqueKey = "p0-counter", enableSql = true),
      ],
      durableObjectStorage = (localDisk = "do-storage"),
      bindings = [
        (name = "COUNTER", durableObjectNamespace = "Counter"),
      ],
    )),

    (name = "do-storage", disk = (path = "/tmp/cellhive-workerd/do", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18788", http = (), service = "main"),
  ],
);
