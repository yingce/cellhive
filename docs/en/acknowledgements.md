# Acknowledgements and Design References

CellHive's state model, runtime integration approach, and some engineering techniques reference the **designs and contracts** of several open-source projects in the same domain. What is borrowed is design, not implementation: except for parts that explicitly belong to third parties and are reused under their licenses, the code in this repository is self-developed.

## celld (`denoland/celld`)

A self-hosted distributed Durable Objects platform (an all-in-one daemon with V8 included, with long-term state stored in your own bucket).

- **Designs referenced**: cell = a state unit backed by one SQLite database; **bucket conditional writes determine the owner**; **epoch fencing**; committed writes captured as **LTX**; **RPO=0** (single node waits for the bucket; in multi-node mode, held first by a peer); durable addressing and replaceable nodes; node leases as a scaling signal; **graceful handoff**; and the discipline that "**indexes are always non-authoritative and repairable**" (CellHive's `wake/` index and repair loop come from this).
- **Not adopted**: celld **comes with its own V8 execution engine**—CellHive insists on using **stock workerd**, integrating only through `workerLoader`/bindings/capnp; it also **does not fork or modify** the celld binary.
- For comparison, see [`decisions.md`](decisions.md) (related ADRs for the cell protocol, LTX, capture, durability posture, handoff, etc.).

## WDL (`wdl-dev/wdl`)

A self-hosted multi-tenant Workers platform based on stock workerd (a family of independent services + external state storage).

- **Patterns and engineering techniques referenced**: `workerLoader` **multi-tenant dynamic loading** of immutable versions; **binding host adapter** (platform worker exports entrypoint classes, and `ctx.exports.X({props})` generates RPC stubs bound with props); versions/rollback; **dual-socket privilege separation**; the DO **host actor + facets** organization and supervisor lifecycle; the **alarm shim** idea (stock workerd does not implement native alarms for the SQLite facet); and the documentation style of **organizing by module + providing a reading path** (this repository's `docs/modules/` and `docs/contributing.md`).
- **Not adopted**: WDL's **Redis/Valkey state model** (CellHive uses bucket + SQLite + conditional writes), **separate gateway component** (CellHive uses Traefik + user-runtime loader), and splitting **scheduler / workflows into separate Rust services** (CellHive unifies timing and dispatch inside `cell-agent`).

## LiteFS / `superfly/ltx`

An **implementation reference** for SQLite replication and the LTX format on the Go side: `internal/ltx`, `internal/replica`, `internal/compaction`, etc. follow LTX's page mapping/snapshot/compaction approach (we do not directly port the Rust implementation).

## Licensing and Attribution

- When reusing any third-party code or design licensed under **Apache-2.0**, the corresponding `LICENSE`/`NOTICE` and attribution must be retained (see the copy/license-related ADRs in [`decisions.md`](decisions.md)).
- This document only describes **design sources and acknowledgements** and does not imply affiliation with, sponsorship by, or endorsement from the projects above. Thanks to the authors and communities of these projects for making their designs and experience public.

_Last updated: 2026-09-19_