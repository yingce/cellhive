# SKELETON — not yet validated against a pinned workerd.
# See docs/workerd-integration.md and workerd/README.md.
#
# user-runtime: tenant loader (public :8081) + internal privileged dispatch (:8088).

using Workerd = import "/workerd/workerd.capnp";

const userRuntime :Workerd.Config = (
  services = [
    # Public, gateway-facing tenant loader.
    ( name = "loader",
      worker = (
        # TODO: embed wrapper + host adapters (platform-owned JS modules).
        # modules = [ (name = "index.js", esModule = embed "loader/index.js") ],
        compatibilityDate = "2026-04-24",
        bindings = [
          # Dynamic tenant bundle loading. Requires `workerd --experimental`.
          (name = "LOADER", workerLoader = ()),
          # Tenant loaded workers may only reach the public internet.
          (name = "PUBLIC_NETWORK", service = "public-network"),
        ],
        globalOutbound = "public-network",
      ),
    ),

    # Internal privileged dispatch (scheduled/queue/workflow). Private only.
    ( name = "internal",
      worker = (
        compatibilityDate = "2026-04-24",
        # TODO: privileged dispatch handlers.
      ),
    ),

    ( name = "public-network",
      network = (
        allow = ["public"],
        deny = ["private"],
      ),
    ),
  ],

  sockets = [
    ( name = "public", address = "*:8081", service = "loader" ),
    ( name = "internal", address = "*:8088", service = "internal" ),
  ],
);
