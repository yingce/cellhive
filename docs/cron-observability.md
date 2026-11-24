# Cron invocation results and retry identity

The internal runtime's `event_type=cron` record reports the handler result. A scheduled Tail record can say `ok` after the invocation wrapper catches a business exception and returns an error envelope. That is transport completion, not business success. Both reference Vector configurations map that record to `category=runtime_telemetry`, `outcome=transport_ok`, `outcome_scope=wrapper_transport`. They retain failed Tail records so a crash without a final handler record is not hidden.

Failed invocation normalization accepts `outcome=failed`, `cellhive.error`, and legacy `cellhive.exceptions`. It emits `CELLHIVE_INVOCATION_FAILED`, a sanitized bounded error message, error severity and a failure status. The code identifies the failure boundary, not the party responsible for an underlying RPC or database error. Error messages redact credential assignments, bearer values and URLs; arbitrary business error text still requires care at the application boundary.

Cron logical identity uses namespace, worker and stable timer token. Retry wake times are excluded so one scheduled occurrence remains one logical task. The timestamp remains a separate observation field. Attempts are unknown unless supplied explicitly; do not label every call as attempt 1. This changes telemetry only, not timers, retry backoff, ceilings or handler execution. Old logical IDs cannot safely be retroactively combined with the new IDs across a rollout.

Digest consumers count distinct request IDs within each logical task. Extra failed attempts inside the window are a lower bound on retries: the initial attempt may precede the window, and successful attempts are not in the failure query. Missing identities remain explicitly unknown. A failed-attempt digest cannot assert that the job never subsequently recovered.

Verification:

- `node --test workerd/user-runtime/invocation-otlp.test.mjs`
- `python3 scripts/test-cron-observability.py` runs both shipped remaps in a network-isolated Vector container; set `VECTOR_TEST_IMAGE` to the intended production version.

Rollout requires the runtime image and the actual `cellhive-vector-config` contents; editing reference files alone does not update Kubernetes. The VWork Region direct-SLS path and Platform history receiver have matching changes. Stage and test the Hermes digest scripts separately before replacing the running scripts. No business cleanup RPC should be invoked for acceptance.

## Failure boundary diagnostics

Failed invocations export `cellhive.error_name`, a basename-only `cellhive.error_frame` (line and column), and `cellhive.failure_stage` (`worker_load`, `bundle_fetch`, `binding_setup`, `runtime_capacity`, `handler`, `handler_response`, or `dispatch`). Collectors validate these fields and preserve them in the event payload. Missing or unsupported stages become `unknown`; successful events have null diagnostic fields. Full stacks, URLs and arbitrary diagnostic metadata are not exported.

`error_origin` identifies the observed source: completed failed handlers are user_function, explicit runtime_capacity is platform, otherwise unknown. A handler exception, even a TypeError, does not establish underlying dependency responsibility (`root_cause_origin`). A worker load failure can also involve invalid user artifacts. Use request/trace IDs and upstream evidence before assigning ownership. Application code that discards an upstream response cannot have that response reconstructed by log normalization; it must retain bounded, sanitized operation/status/error-code evidence at the call site. These source changes require runtime and collector rollout before new production events contain the fields.

## Fetch, Queue and Cron coverage

Public Fetch summaries carry diagnostics through file logs, direct OTLP, and the cell-agent request-log fallback. Sampled and unsampled requests share the same classification. A returned HTTP 5xx is `handler_response`, distinct from an exception during the handler call (`handler`), preparation/loading failures, and dispatch errors. Response bodies are never read for diagnostics. Internal service Fetch also emits its invocation outcome.

Queue retains handler exception details; an explicit retry request is `retry`, not a failed invocation. Cron preserves the exception location from both thrown exceptions and `noRetry()` result envelopes. Queue/Cron outer RPC exceptions are `dispatch`; that alone does not prove platform ownership. This change does not change retry, ack, scheduling, or response semantics.

Upstream failures swallowed by tenant code still require call-site instrumentation. The classifier never attributes an arbitrary 500/502, TypeError, or generic INTERNAL_ERROR to user code by itself.

### Function completion and error source

`function_result` is `returned`, `threw`, `not_started`, or `unknown`. `function_completed` is true for a returned/exception-settled handler, false before execution, and null without evidence. Completion is not success and does not assert response streaming or detached background work completed. Failed completed handlers are `error_origin=user_function`; underlying dependency ownership remains `root_cause_origin=unknown`. Wrapper transport success never proves handler completion. Fetch preserves the HTTP status; Queue/Cron failure status 500 is an invocation result, not an upstream HTTP response (`status_kind=invocation`). Explicit Queue retry is not an error. Older records without completion evidence remain unknown.
