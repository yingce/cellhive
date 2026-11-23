# Queue Discovery and Control Read Cost Implementation Plan

User approved implementation on 2026-11-23. No deployment or commit requested.

**Goal:** Reduce repeated control authority reads without weakening resource or owner fencing.

**Architecture:** Cache queue resource discovery in the scheduling caller for at most five seconds, coalesce refreshes, and bind each result to the identity returned by its successful control read. Expired or known-invalid views cannot fall back after failure. Local control reads reuse the initial authoritative owner sample and retain the final sample and local read fence; remote calls retain both caller and receiver checks.

**Tech Stack:** Go, existing control reader and queue runner; no new dependencies.

## Constraints

- No cache of authority validation for destructive operations. Actual queue access retains resource deletion, owner and durability gates.
- Discovery may lag resource changes by at most five seconds; known authority changes/errors invalidate earlier.
- Strict control reads remain uncached. No deployment, bucket changes or changes to unrelated temporary directories.

## Tasks

- [x] Add queue discovery cache in cmd/cell-agent, wire Queues callback, and expose resource results with their authority identity from ControlReader.
- [x] Share the initial local owner check with the local read helper; retain final owner/session/epoch, ACK and ReadFence checks. Keep remote caller checks intact.
- [x] Test expiry, failure without fallback, cancellation, single-flight, deleted resource/changed storage ID, observed authority invalidation and logical request counts. Test local and remote owner read counts and owner change during local/remote read.
- [x] Run focused tests with race, then make test/build/vet; review diff and document measured versus unverified production reductions.
