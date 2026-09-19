# workerd multi-DO control config (no output gate), port 18806.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "worker", esModule = embed "multiplain.js")],
      compatibilityDate = "2026-04-24",
      durableObjectNamespaces = [(className = "A", uniqueKey = "gate", enableSql = true)],
      durableObjectStorage = (localDisk = "plain-storage"),
      bindings = [
        (name = "A", durableObjectNamespace = "A"),
      ],
    )),

    (name = "plain-storage", disk = (path = "/tmp/cellhive-multiplain/do", writable = true)),
  ],

  sockets = [
    (name = "http", address = "*:18806", http = (), service = "main"),
  ],
);
