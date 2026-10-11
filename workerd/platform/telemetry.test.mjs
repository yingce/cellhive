import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("./telemetry.js", import.meta.url), "utf8");
const telemetry = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

test("nested platform span uses the supplied parent span", () => {
  globalThis.__cellhiveTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  const invoke = telemetry.startSpan("do.invoke");
  const gate = telemetry.startSpan("do.gate", {}, invoke);

  assert.equal(gate.trace_id, invoke.trace_id);
  assert.equal(gate.parent_span_id, invoke.span_id);
  assert.equal(
    telemetry.traceparentForSpan(gate),
    `00-${gate.trace_id}-${gate.span_id}-01`,
  );
});
