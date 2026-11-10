#!/usr/bin/env python3
"""Join idle request JSON lines to OTLP traces without summing nested spans.

Requires the opentelemetry-proto Python package. Durations are inclusive; spans
may overlap and must not be added to derive request time.
"""
import argparse
import json
from pathlib import Path

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from google.protobuf.message import DecodeError

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--requests', required=True, type=Path)
parser.add_argument('--traces', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
requests = []
for line in args.requests.read_text().splitlines():
    if not line.startswith('{'):
        continue
    try:
        row = json.loads(line)
    except json.JSONDecodeError:
        continue
    if row.get('event') == 'idle-request':
        requests.append(row)
if not requests:
    parser.error('no idle-request records found')
traces = {row['trace']: {} for row in requests}
log_batches = 0
for path in sorted(args.traces.glob('*.pb')):
    data = path.read_bytes()
    try:
        batch = ExportTraceServiceRequest.FromString(data)
    except DecodeError:
        # The shared collector also receives workerd OTLP log exports.
        logs = ExportLogsServiceRequest.FromString(data)
        if not any(s.log_records for r in logs.resource_logs for s in r.scope_logs):
            raise ValueError(f'{path}: neither a trace nor a nonempty log batch')
        log_batches += 1
        continue
    for resource in batch.resource_spans:
        service = next((a.value.string_value for a in resource.resource.attributes
                        if a.key == 'service.name'), '')
        for scope in resource.scope_spans:
            for span in scope.spans:
                trace = traces.get(span.trace_id.hex())
                if trace is None:
                    continue
                trace[span.span_id.hex()] = dict(
                    service=service, name=span.name, span=span.span_id.hex(),
                    parent=span.parent_span_id.hex(), start_ns=span.start_time_unix_nano,
                    inclusive_ms=round((span.end_time_unix_nano-span.start_time_unix_nano)/1e6, 3))
for row in requests:
    row['spans'] = sorted(traces[row['trace']].values(), key=lambda s: s['start_ns'])
    row['trace_present'] = bool(row['spans'])
result = dict(note='Span durations are inclusive and may overlap; do not sum them.',
              log_batches=log_batches,
              request_count=len(requests), missing_trace_count=sum(not r['trace_present'] for r in requests),
              requests=requests)
args.output.write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({k: v for k, v in result.items() if k != 'requests'}))
