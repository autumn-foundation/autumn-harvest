## Phase — bound multi-shard worker shutdown against an exhausted shard pool (issue #1209)

Filed from the Codex review on PR #1207 (itself part of #961): that review's
budget was exhausted, so this correctness finding shipped as its own issue.

**Root cause.** `Worker::run_multi_shard`'s shutdown sequence visits every
shard pool sequentially to write the Draining and Stopped fleet-status rows
(`transition_fleet_status`), and that call opened with a bare
`pool.get().await`. Harvest configures no deadpool `Timeouts`, so a shard
whose pool never yields a connection parked both transition loops forever —
the healthy shards' rows were never written and the process never
terminated. The per-shard heartbeat task had the same shape: its own
`pool.get().await` was not selected against its cancellation token, so a
heartbeat already parked in acquisition was joined indefinitely rather than
torn down.

Proving the fix end-to-end (driving the real `Worker::run` shutdown path,
not a narrower unit test) surfaced a third instance of the identical defect:
the schedule-overdue sampler's eager first pass
(`spawn_schedule_overdue_sampler`, issue #696 / Codex round 5 F1) calls
`pool.get().await` on every shard pool *before* its own cancellation check,
so it could also park past shutdown and block the monitor-task join in
`shutdown_and_cleanup_monitors`.

**What shipped.**

- `transition_fleet_status` takes an `acquire_bound: Option<Duration>` and
  routes its acquisition through the existing `acquire_shard_conn` helper
  (from #961). The multi-shard shutdown loops pass
  `shard_acquire_bound(true, poll_interval)`; the single-shard path passes
  `None`, keeping that path byte-for-byte unbounded (AC3 / the #1207 AC7
  posture).
- The per-shard heartbeat loop (`spawn_worker_heartbeat`) selects its
  `pool.get()` against its own `cancel` token, so a heartbeat parked in
  acquisition is torn down by shutdown instead of joined forever.
- `spawn_schedule_overdue_sampler`'s per-shard acquisition inside its eager
  first pass is now selected against `cancel` the same way; a cancelled
  acquisition marks that pass incomplete (the same handling as a real
  connection error) and the loop's own tail check then exits.
- `HarvestRunner::stop` bounds its `worker_handle.await` with
  `shutdown_timeout` plus a fixed margin, as defense in depth: the worker's
  own shutdown is now bounded end to end, so this should never fire, but it
  keeps the runner's own contract intact if a future change reintroduces an
  unbounded await on this path.

No new `WorkflowEvent` variant, no migration (AC5).

**Test evidence.**

- `autumn-harvest/tests/integration/sharded_runtime_tests.rs::shutdown_completes_when_a_shard_pool_is_permanently_exhausted`
  (new): holds shard 0's only connection for the whole test, drives the full
  `Worker::run` multi-shard shutdown path, and asserts it completes within a
  generous bounded timeout (AC1/AC2) while shards 1 and 2's fleet rows still
  reach `Stopped` (AC1). Reverting any of the three acquisition fixes above
  makes this test hang past its timeout — the regression AC4 asks for.
- `autumn-harvest/src/worker.rs::tests::shard_acquire_bound_is_multi_shard_only`
  (pre-existing, unchanged): still asserts the single-shard path returns
  `None` (AC3).
- `autumn-harvest/tests/integration/sharded_runtime_tests.rs::single_shard_deployment_is_unchanged`
  (pre-existing): continues to pass, corroborating AC3 end-to-end.
- Full `sharded_runtime_tests` module (13 tests) and the `autumn-harvest`
  library build pass with the fix applied.

`cargo build -p autumn-harvest -p autumn-harvest-plugin --features db` and
`cargo clippy -p autumn-harvest -p autumn-harvest-plugin --lib --features db`
are clean on the changed files.
