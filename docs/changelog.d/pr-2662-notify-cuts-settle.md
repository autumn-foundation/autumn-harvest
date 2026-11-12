## A NOTIFY cuts the settle once (issue #2653, PR #2662)

The leader claim loop sleeps a 50 to 75 ms settle after an empty poll that a
NOTIFY caused. A NOTIFY that arrived in that sleep waited for its end. So a
start, a signal or an activity completion that landed in a settle waited for
the rest of it.

- A NOTIFY or an owner wake now cuts a fresh settle, and the leader polls at
  once. The rest of the settle stays owed, up to the original end. No NOTIFY
  cuts the rest. So each settle has one NOTIFY cut at most, and a row that a
  host clock stamped early still gets its poll at the end.
- A settle that sleeps to its end takes the NOTIFYs queued then, and owes a
  fresh settle for them. The poll at the end covers each of them. Before,
  the next idle wait polled again at once, and in a fleet that poll was
  empty on each worker.
- The multi-shard leader removes a shard listener whose channel closed, as
  it removes one that fails. A closed channel returns at once, so a kept one
  made each wait spin.
- Kernel: `notify::SettleState`, `take_settle`, `after_notify_cut` and
  `after_slept_settle`. `formal/verus/src/notify_settle.rs` proves that the
  cuts never exceed the fresh settles, and that each cut owes its rest.
- Measured at 1 shard on one 4-core box: the `signal_roundtrip` p99 falls
  from 73.7 ms to 14.9 ms, and the `dispatch_latency` p99 from 10.3 ms to
  8.2 ms. The capacity cells with 4 worker processes show a median paired
  difference of zero ladder steps. Empty polls rise 17% to 23%. See
  `docs/performance-notify-cuts-settle.md`.
- Tests: the settle tests in `worker::tests` and `notify::tests`.
- No new `WorkflowEvent` variant. No migration. No schema change. No claim
  statement changes.
