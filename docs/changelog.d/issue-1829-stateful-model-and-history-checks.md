## Testing — Stateful lifecycle model and crash-history checks (issue #1829)

Two new test layers check client-visible guarantees. Neither changes engine
code. There is no new `WorkflowEvent` variant and no migration.

**Stateful lifecycle model.** `tests/integration/lifecycle_model_props.rs`
runs random sequences of start, claim, heartbeat, park, complete, signal,
cancel, worker death, worker revival and orphan reclaim. Each operation runs
against a real Postgres and against a reference model. After each step the
test compares the result, the rows of the case, and the run state change
against `lifecycle::TRANSITIONS`. A coverage check fails a run that misses a
branch. CI runs 128 cases through a `linux` manifest row. The new nightly
workflow `proptest-nightly.yml` runs 100000 cases of the `property` target
and 100000 lifecycle cases in 8 shards.

The model found one undocumented ordering rule. A fresh enqueue and a wake
backdate `scheduled_at` by `IMMEDIATE_SCHEDULE_SKEW_SECS` (5 s). An orphan
requeue stamps `clock_timestamp()`. A requeued orphan is therefore claimed
after any start made in the next 5 seconds. The model encodes this, and
`a_requeued_orphan_sorts_behind_a_fresh_start` pins it.

**Crash-history checks.** `tests/integration/history_checker.rs` is a
Porcupine-style linearizability checker with Jepsen `ok`, `fail` and `info`
outcomes. It ships two models: `StartIdempotency` and `ExactlyOnceFire`.
`tests/integration/history_crash_tests.rs` drives concurrent idempotent
starts and scheduler replicas while it drops request futures and terminates
backends. The two #350 chaos reproducers now also check their histories.

**Evidence.** Three engine mutations each fail the lifecycle model with a
short shrunk sequence: no `attempt` increment on claim, no crash strike on
requeue, and no `wake_requested` fallback. A broken idempotency window fails
the start-history check. A per-fire unique schedule id fails the schedule
history check in about one random run of four. The post-start #350
reproducer catches it on every run, through its history check.
