## Perf — Intern metrics-rs label values and seed circuit-breaker state (PR #2202)

**Metrics labels.** `MetricsRsRecorder` copied every per-task label value
into a new `String` on every sample. Those values are the activity,
workflow, queue, build id and error type. They are now interned as
`&'static str` behind a thread-local set and a capped shared set. The
shared set holds at most 4,096 values, and a value past the cap is copied
as before. The sets hash with `foldhash`. A first cut with SipHash cost
more instructions than the copies it removed.

**Circuit breaker.** `CircuitBreakerRegistry::new` seeds one state per
policy. The policy-guarded paths look it up with `get_mut` and allocate no
key.

**Measured** with two new valgrind harnesses,
`benches/metrics_rs_labels_profile.rs` and
`benches/circuit_breaker_dispatch_profile.rs`. Per simulated activity
attempt, the labels harness drops from 12 allocations to 4 (instructions
-10.97%). The circuit-breaker harness drops from 3 allocations to 0
(instructions -16.41%). See `docs/performance-metrics-rs-labels.md`.

**Also fixed.** The `autoresearch` example sets
`HistorySnapshot::start_provenance`. The example (#2190) and that field
(#2107) merged independently, so `trunk-dev` did not compile the example.

No new `WorkflowEvent` variant and no migration.
