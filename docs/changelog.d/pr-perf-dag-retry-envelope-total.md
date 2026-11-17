## Performance — DAG retry envelope without an attempt list

`DagRetryEnvelope::analyze` now uses `retry_envelope::worst_case_without_deadline`.
It builds no attempt list per task. On a 2000-task DAG the harness
`dag_retry_envelope_profile` shows 35.5% fewer instructions and 34% fewer
allocations. Behavior is unchanged: a test compares the new function with
`analyze(..).worst_case_total()` for every jitter mode. See
`docs/performance-dag-retry-envelope.md`.
