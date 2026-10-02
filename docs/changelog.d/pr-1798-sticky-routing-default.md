## Sticky routing on by default, with shutdown release (issue #1798, step 1)

**Behaviour change.** `WorkerConfig::default()` now turns sticky routing on.
`sticky_timeout` is `DEFAULT_STICKY_TIMEOUT` (5 s), the fallback window that
Temporal uses for its sticky queue. The next decision of a suspended
execution goes to the worker that holds its warm cache. That worker loads
only new events, not the full history. To opt out, pass
`StickyRoutingConfig { lease_ttl: Duration::ZERO }`. This also disables the
warm workflow cache.

**Failover.**

- **Crash.** The pin hides the task from peers for at most one window. A
  peer then claims it.
- **Graceful shutdown.** After the drain, the worker calls the new
  `queue::release_worker_sticky_pins` before it marks itself `Stopped`.
  Without this, a rolling deploy would add up to 5 s to the next decision of
  every execution that the old worker pinned, because a wake re-arms the pin
  of a parked task. The release clears pending and parked rows. It keeps
  rows that the worker still runs and worker-session hard pins (issue #606).
  It is best effort: a failure costs one window. No migration. No new
  `WorkflowEvent` variant. `harvest_events` is not touched.

**Decision-cost baseline.** New `decision_cost` and `decision_wall` groups in
`benches/replay_bench.rs` call `executor::run_workflow` at 1k, 5k and 10k
events. Replay cost grows linearly: 0.10 ms, 0.56 ms and 1.13 ms. A
suspending decision adds a flat 100 ms wait (issue #1797). Step 2 of #1798
(resident workflow state) is blocked by #1797 and is not in this change.

**Tests.** `tests/integration/sticky_default_tests.rs` runs real workers with
`WorkerConfig::default()`:

- Decision 2 lands on the same worker as a cache hit.
- After that worker crashes, a peer finishes the run with a cache miss, and
  only after the sticky window.
- A graceful shutdown clears the pin of the parked task.
- `release_worker_sticky_pins` touches only the pending and parked rows of
  the worker.

Unit tests cover the new default, the effective-config view and the release
SQL.
