## Fix — audit export runs on its own task (issue #1269)

Follow-up from the review of #1261 (issue #953). `fire_due_audit_exports` ran
inline inside `timeout::enforce_timeouts_once`, sharing that loop's
connection and cadence with timeout enforcement, SLA checks, session
cleanup, and codec rotation. The sink call was bounded by the claim lease,
but a bound is not cadence: a slow or blackholed sink delayed every other
resident of the loop for up to one lease, and on the multi-shard fan-out
arm a `max_size(1)` shard pool could never export at all, because the
export call asked the pool for a second connection while the checker's own
was still held.

Both hazards were coupling problems, not bugs in the claim/deliver/ack
pipeline itself, so the fix is architectural: give export its own task.

- **New `spawn_audit_export_checker_for_shard`** (`audit_export.rs`): one
  dedicated task per assigned shard, on the worker's existing poll
  interval, registered under the new `Scanner::AuditExport` liveness label.
  It owns its connection lifecycle end to end — never nested inside
  another resident's checkout, which is what made the old failure
  permanent (every tick failed the same way, forever). A cheap
  `is_configured()` check skips the connection checkout entirely on an
  unconfigured deployment (AC8).
- **`export_once_via_pool`**: the task's own tick never holds a pooled
  connection across the network delivery (Codex review on this PR, P1). It
  checks a connection out for the claim transaction, releases it before the
  sink call, then checks one out again for the acknowledgement. A slow or
  hung sink therefore cannot occupy a `max_size(1)` shard pool during
  delivery and block the timeout checker, which a naive "just split the
  task" fix would still have allowed.
- **Liveness registration accounts for the export lease, not just the poll
  interval** (Codex review on this PR, P2): a single tick can legitimately
  run as long as the configured lease allows, which can far exceed the
  worker's poll interval, so `scanner_liveness` registers with
  `poll_interval.max(lease)` to avoid flagging a healthy, still-within-lease
  delivery as `Stale` or `Wedged`.
- **`enforce_timeouts_once` no longer calls `fire_due_audit_exports`.** The
  function, its claim/deliver/ack pipeline, and its `pub` primitive shape
  are unchanged — an embedder driving it by hand still works exactly as
  before — only the caller inside core's own scanner loop is gone.
- **`Scanner::AuditExport`** added to the bounded `scanner` label set
  (`scanner_health.rs`), bringing the total to eight labels / six spawned
  loops.
- Removed `timeout::mark_audit_export_unobserved_for_checker_shard` and its
  two call sites: the timeout checker's own connection failures no longer
  have anything to do with audit export's observability, since the two are
  independent tasks now. The export task marks its own shard unobserved on
  its own connection failure instead.
- **Test evidence:** `audit_export_tests.rs` adds
  `enforce_timeouts_once_no_longer_exports_audit_records` (decoupling, the
  direct pin on the old inline call site being gone),
  `spawn_audit_export_checker_for_shard_exports_independently`,
  `an_unconfigured_dedicated_task_never_attempts_a_connection_checkout`
  (AC8), and
  `a_dedicated_export_task_still_exports_on_a_size_one_pool_shared_with_the_timeout_checker`
  (the fix's replacement property: two independent tasks share a
  one-connection pool without permanent starvation), plus the
  `AuditExport` scanner's own connection-failure-marks-unobserved pair
  (replacing the two tests that pinned the old, now-removed coupling).
  `scanner_tick_db_tests.rs` adds the same register/tick/deregister proof
  the other per-shard loops have. `scanner_liveness_tests.rs` extends the
  bounded label-set and spawn-site-ownership guards to the new variant.
