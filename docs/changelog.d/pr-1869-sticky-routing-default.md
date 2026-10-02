## Feature — Sticky routing on by default, with shutdown release (issue #1798, step 1)

**Behaviour change.** `WorkerConfig::default()` now turns sticky routing on.
`sticky_timeout` is `DEFAULT_STICKY_TIMEOUT` (5 s), the fallback window that
Temporal uses for its sticky queue. Until the pin expires, only the worker
that holds the warm cache can claim the next decision of a suspended
execution. That worker loads only new events, not the full history. To opt
out, call `with_sticky_routing(StickyRoutingConfig { lease_ttl:
Duration::ZERO })`. This also disables the warm workflow cache.

**Memory.** The warm cache is now on by default. Each of up to
`workflow_cache_size` (1000) entries holds a full decoded history. Lower
`workflow_cache_size` for a worker that runs many long histories.

**Failover.**

- **Crash.** A pin hides the task from peers for up to one window. A wake
  re-arms the pin of a parked task, so each execution pinned to the dead
  worker waits up to one window at its next wake.
- **Graceful shutdown.** The worker calls the new
  `queue::release_worker_sticky_pins` when it starts to drain, and again
  after the drain. A wake during the drain therefore does not re-arm a pin.
  Without the release, a rolling deploy adds up to 5 s to the next decision
  of each pinned execution. The release clears the pins of pending and
  parked rows. It does not touch rows that the worker still runs or worker
  session pins (issue #606). It is best effort: a failure costs one window.
- **Redis dispatch.** The dispatch probe now reports a live pin of another
  worker. That worker hands the reference on after one poll interval, with
  no backoff. Before, the backoff grew past the sticky window, so most
  follow-up decisions on a large fleet waited for the pin to expire.

**Fix: update results in the warm cache.** Two inline paths persisted
`UpdateCompleted`/`UpdateFailed` but did not add them to the in-memory
history: an external-signal-only batch and a mixed external batch. The
in-process re-drive and the warm cache then replayed `UpdateAdmitted` alone,
so the update handler ran a second time and wrote a second result. Both
paths now add the events, through a shared `update_result_events` helper.
This bug already affected workers that enabled sticky routing by hand.

No migration. No new `WorkflowEvent` variant. The change does not touch
`harvest_events`.

**Decision-cost baseline.** New `decision_cost` and `decision_wall` groups
in `benches/replay_bench.rs` call `executor::run_workflow` at 1k, 5k and 10k
events. Replay cost grows linearly: 0.10 ms, 0.56 ms and 1.13 ms. A
suspending decision waits for a fixed 100 ms timeout (issue #1797). Step 2
of #1798 (resident workflow state) depends on #1797 and is not in this
change.

**Tests.** `tests/integration/sticky_default_tests.rs` runs real workers
built from `WorkerConfig::default()`:

- Decision 2 lands on the same worker as a cache hit.
- After that worker crashes, a peer finishes the run, only after the sticky
  window.
- A graceful shutdown clears the pin of a parked task.
- A draining worker clears its pins before the drain ends.
- An update result batched with an external signal runs its handler once.
- `release_worker_sticky_pins` touches only the pending and parked rows of
  the worker.

Unit tests cover the new default, the effective-config view, the release
SQL, the dispatch probe SQL and the dispatch outcome for a pinned row.
