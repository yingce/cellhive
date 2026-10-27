# Wrangler compatibility safety fixes — 2026-10-26

## Initial safety patch (superseded below for staged APIs)

Official Wrangler staging requests previously executed atomic deployment. The adapter now rejects POST versions and PATCH worker versions with an explicit Cloudflare-shaped HTTP 501 error, without changing active versions. This is a safety boundary, not implementation of staged versions. Wrangler deploy flows using POST versions are also unsupported; use native `cellhive deploy --config` for atomic deployment.

Multipart metadata now translates AI, Hyperdrive and Vectorize bindings; unsupported types (including JSON variables and inline secret_text) fail instead of disappearing. Existing supported bindings retain native validation.

Settings reads and version details share real configuration serialization. Version secret names are queried from version_secrets without decryption. Ordinary secret lists use name/type. Unsupported settings PATCH returns 501 instead of pretending to save.

## Validation

- Before-fix regression: upload returned 200 and activated version 2; six binding types disappeared; settings returned fixed data. Tests failed as expected.
- Real Wrangler 4.133.0 over local HTTP, real admin handlers, temporary SQLite and FS bucket: PASS. versions upload fails explicitly with active=1; secret put succeeds active=2; ordinary and versioned secret lists show TOKEN; versions secret put fails explicitly with active still 2.
- Additional regression: historical version 1 does not contain the newly added secret name; version 2 and settings do; no plaintext in any response. Unsupported PATCH and missing versions return errors. Binding field mapping checked.
- Full internal/server, internal/control, internal/wrangler, internal/wranglercompat package tests: PASS.
- make build vet: PASS. git diff --check: PASS.
- No production changes, COS E2E or workerd execution E2E in this validation.

Re-run real CLI with CELLHIVE_TEST_WRANGLER_CLI pointing at a Wrangler 4.133.0 cli.js and the standard SQLite build tags, running internal/server TestReviewWranglerRealCLI. Without that variable, only this external CLI integration test skips; HTTP regression tests always run.

Local detailed logs: /tmp/wrangler-fix-cli-final.txt, /tmp/wrangler-fix-packages.txt, /tmp/wrangler-fix-build-vet.txt.


## Follow-up: staged versions (ADR-301)

Implemented real staged upload instead of returning 501. DeploySpec.StageOnly persists the version, bindings, bundle references and secret snapshot in the same transaction, preserving active/previous and omitting deployment events. Server uses the existing capturedWrite durability barrier. First upload remains inactive; an existing live namespace is required. Staging refuses DO migrations and does not recreate routes or restart DOs.

Versioned Secret merge patches clone the latest uploaded version and its ciphertext snapshot in a single transaction; new values are sealed before insertion. Current secrets and active remain unchanged. Promotion and rollback use the existing atomic secret-baseline restoration. Deletion locks and deleted-worker checks protect staging.

Real Wrangler 4.133.0 local HTTP sequence passed: versions upload (active=1), ordinary secret put (active=3), secret list, versions secret list, versions secret put (new version=4, active=3), versions deploy 4@100% (active=4), deploy (active=5). The tests independently read CellHive's active pointer.

Additional control/HTTP coverage: first upload, stage idempotency and conflicting retry mode, bundle reference retention, staged Secret deletion, promotion/rollback snapshots, delete lock/deleted Worker, invalid Secret patch, rejected staged migration without version creation. Four affected packages pass full tests; make build vet passes.

Remaining boundaries: no gradual traffic split; settings PATCH remains rejected; ordinary Wrangler deploy warns about environment tags and prints null for the numeric version ID (CLI expects UUID). Actual deployment was successful. JSON vars and inline secret_text metadata remain explicitly unsupported. No production or COS/workerd execution E2E was run.

Follow-up logs: /tmp/wrangler-staging-final-cli.txt, /tmp/wrangler-staging-final-packages.txt, /tmp/wrangler-staging-build-vet.txt, /tmp/wrangler-staging-race.txt.

Targeted control/server staging and Secret race tests passed with -race -count=3 (real external CLI test skips in the race run and is covered separately above).
