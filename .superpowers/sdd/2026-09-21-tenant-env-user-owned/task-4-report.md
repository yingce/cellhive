# Task 4 report: exhaustive user-owned tenant env coverage

## Delivered

- Added `TestUserRuntimeEnvUserOwned` in `internal/userruntime/userruntime_test.go`.
  It runs on the pinned real workerd and verifies the exact 12 user-declared
  keys and values on public fetch, queue, scheduled, service RPC, and workflow
  constructor surfaces.  Fetch, service RPC, and workflow also inspect the
  importable `cloudflare:workers` env; workflow verifies both its constructor
  parameter and `this.env`.
- Added `TestDoRuntimeEnvUserOwned` in `internal/doruntime/compat_test.go`.
  It runs on the pinned real workerd and verifies exact keys/values in a DO
  constructor parameter, `this.env`, and the importable env.
- No `workerd/` production change was needed.  The current workerLoader env
  builders already materialize tenant env exclusively from declared vars and
  binding stubs; platform values remain module-scoped wrapper data.

## TDD / characterization evidence

The new regression was written before changing any production code.  After
fixing two test-fixture errors (the initial tenant JS syntax typo and a DO test
token mismatch), both real-workerd matrix tests passed without a production
change.  This is characterization evidence that the existing Task 1-3 env
implementation meets the complete Task 4 matrix rather than a fabricated RED.

## Commands and results

All commands used the required fixed workerd:

```sh
CELLHIVE_WORKERD=/opt/cellhive/cli/node_modules/.bin/workerd \
  go test -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' \
  ./internal/userruntime ./internal/doruntime -run 'Test.*Env.*UserOwned' -count=1 -p 1
# PASS: userruntime 0.229s; doruntime 0.225s

CELLHIVE_WORKERD=/opt/cellhive/cli/node_modules/.bin/workerd \
  go test -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' \
  ./internal/userruntime -count=1
# PASS: 16.182s

CELLHIVE_WORKERD=/opt/cellhive/cli/node_modules/.bin/workerd \
  go test -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' \
  ./internal/doruntime -count=1
# PASS: 15.869s
```

`git diff --check` also passed.

## Self-review

- Exact-key assertions reject additions as well as omissions and check every
  declared value, including former system names.
- Tests use the repository SQLite feature tags and stock pinned workerd.
- No platform key/capability was reintroduced into tenant env, and no bucket or
  runtime production behavior changed.
- Tenant platform log capture remains intentionally disabled; this task does
  not depend on dynamic log tails unsupported by the pinned workerd.
