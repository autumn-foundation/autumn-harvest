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
  delivery as `Stale` or `Wedged`. Re-checked every tick and re-registered on
  a change, so a second runtime publishing a longer lease is picked up
  without restarting the task (follow-up P2).
- **The shipped `harvest_scanner_stalled` alert gives `audit_export` its own
  10m Prometheus window** (follow-up P2), instead of sharing the other
  sub-minute loops' 5m one: a healthy delivery running longer than 5
  minutes under a longer configured `audit_export_lease` would otherwise
  page despite being within its supported timeout.
- **Shutdown cancels an in-flight delivery wait instead of joining it**
  (follow-up P1): both worker shutdown paths await every export task's
  handle, and a bare await on the sink call would have blocked graceful
  shutdown for up to the full lease. The delivery await now races `cancel`;
  a shutdown mid-delivery abandons the wait and leaves the claim exactly
  where it was; the batch is safely redelivered later.
- **`enforce_timeouts_once` no longer calls `fire_due_audit_exports`.** The
  function, its claim/deliver/ack pipeline, and its `pub` primitive shape
  are unchanged — an embedder driving it by hand still works exactly as
  before — only the caller inside core's own scanner loop is gone.
- **`Scanner::AuditExport`** added to the bounded `scanner` label set
  (`scanner_health.rs`), bringing the total to eight labels / seven spawned
  loops.
- **The per-tick liveness re-registration reads the same config snapshot
  the tick itself delivers with** (follow-up P2): the checker previously
  read the global export config twice per tick, once to size the
  registered interval and once inside `export_once_via_pool`, so a swap
  landing between the two reads could register one lease while delivering
  against another. `export_once_via_pool` now takes the snapshot as a
  parameter instead of reading it itself.
- **The delivery deadline reserves `SHARD_ACQUIRE_BOUND` plus the new
  `ACK_QUERY_BOUND` off the claim lease** (follow-up P1 and follow-up P2):
  a delivery finishing right at `lease_until` left zero time for the
  reacquire-and-acknowledge step that follows it, and an initial fix that
  reserved only the checkout bound still left the acknowledgement query
  itself no margin. A second exporter could then reclaim the shard before
  the first one's acknowledgement lands, so a batch delivered under
  sustained near-lease latency was never acknowledged. Reserving both
  bounds guarantees a successful delivery always has that time left to
  reacquire a connection and acknowledge.
- **That reserve is capped at half the configured lease** (follow-up P1):
  the fixed reserve above exceeds leases the builder explicitly supports
  down to one second. Subtracting it unconditionally would place the
  delivery deadline at or before the claim itself, timing out every batch
  on a short lease immediately and backing off forever. The cap instead
  shrinks the delivery window and the reserve together on a short lease,
  so a short lease always keeps some genuine delivery time.
- **The capped reserve is split proportionally between the reacquire and
  the acknowledgement query** (follow-up P1): a capped reserve can already
  be smaller than the fixed `SHARD_ACQUIRE_BOUND`. Reacquiring under that
  fixed bound regardless could let the checkout alone consume the whole
  capped reserve, leaving the acknowledgement query no margin even after
  the cap. `split_reserve` shrinks both bounds together, in their uncapped
  5:2 proportion, and the acknowledgement query is now itself bounded by
  its share instead of running unbounded.
- **The sink-swap fence now runs immediately before the acknowledgement
  write, not right after delivery** (follow-up P1): fencing right after
  delivery could not see a swap landing during the post-delivery reacquire
  wait, so a stale `Advance` outcome from a since-replaced sink could still
  commit. Fencing and classifying right before the write closes that
  window down to the acknowledgement query itself.
- **The `audit_export` Prometheus expression carries the same startup gate
  as `retention`** (follow-up P2): the series starts at zero at
  registration, so a fresh worker's still-in-flight first delivery could
  read `0` over a partial `[10m]` window and page despite being healthy.
  `docs/alerts/starter-pack-v0.1.0.json` ANDs the expression on the
  counter having reached a nonzero value in the same window, mirroring the
  retention expression's own gate.
- **`docs/telemetry.md`** updated to list `audit_export.rs` among the
  per-shard scanner loops, with the eight-label / seven-loop counts and
  the `audit_export` label value.
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
  Follow-up review rounds add
  `an_in_flight_slow_delivery_never_blocks_the_timeout_checker_on_a_size_one_pool`
  (a blocking-until-released sink proves the connection-release property
  directly), `audit_export_checker_re_registers_when_the_configured_lease_grows`,
  `graceful_shutdown_does_not_wait_for_an_in_flight_delivery`,
  `the_delivery_deadline_reserves_time_for_the_acknowledgement` (a
  still-blocked sink proves the effective delivery bound is well short of
  the raw lease), and `a_short_lease_still_keeps_a_positive_delivery_window`
  (an instant sink still succeeds on a four-second lease, proving the cap
  keeps every window positive rather than losing them entirely to the
  fixed reserve). `split_reserve_returns_the_fixed_bounds_at_the_uncapped_reserve`,
  `split_reserve_shrinks_both_bounds_proportionally`, and
  `split_reserve_of_zero_is_zero` pin the split's arithmetic directly.
  `alert_pack_docs.rs` adds
  `scanner_stalled_audit_export_expression_carries_the_same_startup_gate`,
  pinning the `audit_export` expression's startup gate the same way the
  existing test pins the retention expression's.
