# SUPERSEDED (ADR-077): the do-runtime config is rendered at startup by
# `cmd/do-runtime` (internal/doruntime) with the real pinned-workerd fields
# (durableObjectNamespaces + durableObjectStorage localDisk + disk service).
# This file is kept only for historical reference; do not use it directly.

# SKELETON — not yet validated against a pinned workerd.
# See docs/durable-objects.md and workerd/README.md.
#
# do-runtime: native Durable Object host actor + localDisk.
# A Go supervisor (PID 1) spawns this workerd and captures WAL -> reports to cell-agent.

using Workerd = import "/workerd/workerd.capnp";

const doRuntime :Workerd.Config = (
  services = [
    ( name = "do-host",
      worker = (
        compatibilityDate = "2026-04-24",
        bindings = [
          (name = "LOADER", workerLoader = ()),
        ],
        globalOutbound = "private-network",
      ),
    ),

    # Durable Object namespace backed by the host actor.
    ( name = "do-namespace",
      durableObjectNamespace = (
        # TODO: point at the DO host actor worker + SQLite storage; requires
        # `durableObjectStorage = (localDisk = ...)` per pinned workerd.
      ),
    ),

    ( name = "private-network",
      network = (
        allow = ["private"],
        deny = ["public"],
      ),
    ),
  ],

  sockets = [
    ( name = "do", address = "*:8788", service = "do-host" ),
  ],
);
