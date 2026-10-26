## Phase trace export perf — return Chrome trace events without a serde round trip (PR #2279)

`trace_export::export_chrome_trace` returned `json!(events)`. On a
`Vec<Value>`, that macro serializes the finished tree into a second, deep
copy. It now returns `Value::Array(events)`, which moves the vector. The
output is identical. No event, migration or append-only change.

Evidence: the new `dag_tooling_profile` harness lints, profiles and exports a
2,000-task layered DAG. Total instructions fall from 56.63M to 36.92M
(-34.8%), and allocated blocks from 79,009 to 51,008 (-35.4%). See
`docs/performance-chrome-trace-export.md`.
