using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "worker", esModule = embed "plain.js")],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [(className = "A", uniqueKey = "plain", enableSql = true)],
      durableObjectStorage = (localDisk = "plain-storage"),
      bindings = [
        (name = "A", durableObjectNamespace = "A"),
      ],
    )),

    (name = "plain-storage", disk = (path = "/tmp/cellhive-gate/plain", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18806", http = (), service = "main"),
  ],
);
