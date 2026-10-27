# CellHive Helm

## Agent-first upgrades

For ADR-302 upgrades, first set `cellAgent.image.digest` to the new immutable digest while retaining the global `image.digest`. Wait for every cell-agent to be Ready on that digest. Then set the global digest to the new version and remove the agent override to roll out the runtimes. The optional agent image map inherits unspecified fields from the global image.
