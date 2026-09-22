## Phase — select every per-shard monitor's acquisition against cancellation (issue #1426)

Split out of the review on PR #1424 (itself part of #1209): that PR's fix
was scoped to three specific mechanisms (`transition_fleet_status`, the
heartbeat loop, and the schedule-overdue sampler's eager first pass). This
issue tracks the same-shaped, distinct gap that PR deliberately left
unfixed.

**Root cause.** `Worker::run_multi_shard` spawns roughly a dozen per-shard
monitor/sampler background tasks (timeout checker, poison-pill reclaimer,
session-slot reconciler, pause auto-resumer, quota-key reconciler, and the
queue-depth/concurrency/rate-limit/DLQ/queue-pause/history-oversized/
workflow-active/stranded-work/replication samplers). Every one of them
followed the same loop shape:

```rust
loop {
    tokio::select! {
        () = cancel.cancelled() => break,
        () = tokio::time::sleep(interval) => {}
    }
    // pool.get().await, unconditionally, not selected against `cancel`
}
```

Harvest configures no deadpool `Timeouts`, so cancellation was only checked
*between* ticks. Once a tick's `pool.get()` was in flight against a shard
whose pool never yielded a connection, that task was stuck forever — no
later `cancel.cancel()` could rescue it. `shutdown_and_cleanup_monitors`
joins every one of these `JoinHandle`s sequentially and unconditionally, so
a single wedged monitor hung the whole worker's shutdown, reproducing the
"shutdown can hang forever on an exhausted shard pool" symptom #1209 was
filed for.

The #1209 regression test could not catch this: it requests shutdown
~300ms after start, before `spawn_monitoring_tasks` has even run (multi-shard
startup registration against the deliberately-exhausted shard takes several
seconds on its own in that test's setup). Every monitor loop observed the
already-cancelled token at the top of its own loop, before ever calling
`pool.get()`.

**What shipped.** Every affected loop's acquisition is now selected against
its cancellation token, mirroring the pattern PR #1424 already established
for the heartbeat loop and the schedule-overdue sampler:

```rust
let get_result = tokio::select! {
    () = cancel.cancelled() => None, // or `break` for a single-pool loop
    result = pool.get() => Some(result),
};
```

Each call site kept its own existing recovery semantics on a cancelled or
failed acquisition (some `continue` to the next shard, some `break` the
tick, some skip the tick entirely) — cancellation is now handled at least as
conservatively as a genuine acquisition failure already was, nowhere more
loosely.

- `poison_pill.rs::spawn_poison_pill_reclaimer_for_shard`,
  `sessions.rs::spawn_session_slot_reconciler`,
  `quota_reconcile.rs::spawn_quota_key_reconciler_for_shard`,
  `worker.rs::spawn_pause_auto_resumer`,
  `worker.rs::spawn_dlq_depth_sampler` — single-pool loops, fixed with a
  `break`-on-cancel select. `quota_reconcile.rs` was not in the issue's
  original candidate list; it shares the identical shape and was fixed for
  the same reason.
- `worker.rs::spawn_queue_depth_sampler`, `spawn_concurrency_sampler`,
  `spawn_rate_limit_sampler`, `spawn_queue_pause_sampler`,
  `spawn_history_oversized_sampler`, `spawn_workflow_active_sampler` —
  multi-pool-per-tick samplers, fixed per-pool inside their `for pool in
  &pools` loop, mirroring `spawn_schedule_overdue_sampler`'s existing
  pattern.
- `worker.rs::spawn_stranded_work_sampler` — three separate per-shard
  acquisitions inside its per-shard body, each fixed independently.
- `worker.rs::spawn_replication_sampler` / `sample_one_shard` — gained a
  new `ShardSample::Cancelled` variant so the outer loop can distinguish
  "this shard's acquisition was cancelled, stop sampling the remaining
  targets this tick" from `Continue` and `Fenced`.
- `timeout.rs::spawn_timeout_checker_for_shard` — this loop already bounded
  its acquisition to the tick interval via `tokio::time::timeout`, so it
  could not hang forever, only delay shutdown by up to one interval. It now
  also races that bounded wait against `cancel` for immediate
  responsiveness, while keeping the pre-existing "skip a merely slow tick"
  behavior for the non-cancellation case.

`audit_export.rs::spawn_audit_export_checker_for_shard` was already
cancel-aware (via its own bounded `acquire_shard_conn_for_export` helper)
and needed no change.

No new `WorkflowEvent` variant, no migration (AC4). Adding deadpool
`Timeouts` globally remains out of scope, per the same carve-out #1209 and
this issue both already declared.

**Test evidence.**

- New:
  `autumn-harvest/tests/integration/sharded_runtime_tests.rs::shutdown_completes_when_a_shard_pool_is_exhausted_after_monitors_have_ticked`.
  Builds every shard's pool at a generous, uniform size so all three shards
  start genuinely healthy, lets the worker run long enough for every
  monitor loop to tick at least once against shard 0, then drains shard 0's
  pool to simulate it going permanently exhausted mid-run, and only then
  requests shutdown. Asserts the worker's `run()` task completes within a
  bounded timeout and that shards 1 and 2's fleet rows still reach
  `Stopped`. Confirmed to reproduce the hang against the pre-fix code
  (the worker reaches "draining in-flight tasks" and then never returns)
  and to pass in ~19s against the fix (AC1).
- Full `sharded_runtime_tests` module (15 tests, including the #1209 test
  above) and the `autumn-harvest` library's 487 unit tests pass with the
  fix applied.
- `cargo build -p autumn-harvest --features db` and
  `cargo clippy -p autumn-harvest --lib --test integration --features db`
  are clean on the changed files.
- `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` is clean:
  no Tier A findings, no Tier B regressions.
