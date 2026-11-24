# Redis recovery discovery implementation plan

Goal: Redis-only pending-session discovery with COS recovery authority preserved.

- [x] Add an optional nodelog pending-index interface; register before COS Open, remove only on sealed observation, use indexed hint bodies for discovery. Test crash windows with a fake index and counting bucket.
- [x] Implement generation-guarded coordination directory and ready marker, offline seed for both initial migration and existing Redis namespaces, and agent startup gate. Test against isolated Redis including generation loss and real recovery.
- [x] Run focused tests, race and build/vet; record results in docs/known-issues and decisions. Preserve the dirty working branch and do not deploy as part of local verification.

The epoch GC question is answered in the design's follow-up section; no deletion-policy changes belong to this patch.
