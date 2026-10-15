# Worker-to-worker native RPC

This is a focused Service Binding example for native workerd JSRPC and
structured-clone values. It calls an exported `Math` entrypoint and sends a
`Map` through `env.MATH.box()`.

Deploy `api` before `caller`, in the same namespace. Native service RPC is the
default CellHive path. If `CELLHIVE_SERVICE_NATIVE=0` forces the HTTP/JSON
fallback, values such as `Map`, `Date`, and `ArrayBuffer` do not retain full
structured-clone semantics; use the `services` example for fetch plus simple
JSON-compatible RPC behavior.
