# Static asset creation on COS/OSS — 2026-10-27

The VWork production Web canary failed at `POST /v1/control/asset` with
`bucket: not supported`: ProviderBucket only supported selected conditional-create
key families, and `assets/` was missing. The failed Function was inactive and
was removed through the normal management API.

Asset tokens identify a manifest rather than each file's content. The repair
uses native COS/OSS PutObject forbid-overwrite for `assets/`; a check followed by
unconditional Put would allow concurrent replacement. Zero-byte files also need
ordinary conditional creation, because position-zero append remains possible
when an existing object's size is zero. Other key families and authority/fence
protocols are unchanged. Artifact-layer identical-byte replay remains idempotent.

Validation before rollout:
- Native COS probe: empty and nonempty first uploads returned 200; duplicate
  writes returned 409 `FileAlreadyExists`; bytes were preserved and keys deleted.
- Real ProviderBucket COS regression: empty/nonempty, duplicate conflict and
  eight competing writes passed with one winner and unchanged winner content.
  All temporary keys were deleted.
- COS/OSS SDK transport tests verify the native headers and conflict handling;
  bucket and artifact regression suites passed.
- Real Docker CellHive upload/assets-first/worker-first/API/SPA/binding/update/
  rollback acceptance, including exact `/`, passed on the fixed image.

OSS was covered by SDK transport tests; a real OSS account was not exercised.
