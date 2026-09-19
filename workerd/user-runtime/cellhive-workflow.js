// CellHive platform WorkflowEntrypoint base (P2 workflows).
//
// stock workerd exports `WorkflowEntrypoint` but cannot construct it outside its
// own Workflows engine ("constructor parameter 1 is not of type
// 'ExecutionContext'"). So the platform rewrites the tenant bundle's
// `cloudflare:workers` import to this module and runs `run(event, step)` itself,
// mirroring the DurableObject alarm/deleteAll shim (ADR-079/084).
//
// The tenant class only needs `extends WorkflowEntrypoint`; the runner passes a
// platform `step` object implementing the supported subset of step.do/sleep.

export * from "cloudflare:workers";

// NonRetryableError: throwing it from a step aborts the workflow immediately
// (step.do does not retry it), matching Cloudflare Workflows.
export class NonRetryableError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = "NonRetryableError";
  }
}

export class WorkflowEntrypoint {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }
}
