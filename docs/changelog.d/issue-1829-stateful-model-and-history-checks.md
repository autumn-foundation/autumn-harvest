## Testing — Stateful lifecycle model and crash-history checks (issue #1829)

Two new test layers check client-visible guarantees. Neither changes engine
code. There is no new `WorkflowEvent` variant and no migration.

**Stateful lifecycle model.** `tests/integration/lifecycle_model_props.rs`
runs random sequences of start, claim, heartbeat, park, complete, signal,
cancel, worker death, worker revival and orphan reclaim. Each operation runs
against a real Postgres and against a reference model. After each step the
test compares the result, the rows of the case, the lifecycle events and
the dead-letter count. It checks each run state change against
`lifecycle::TRANSITIONS`, and the claim order against the claim-order due
time. A coverage check fails a run that misses a
branch. CI runs 128 cases through a `linux` manifest row. The new nightly
workflow `proptest-nightly.yml` runs 100000 cases of the `property` target
and 100000 lifecycle cases in 8 shards. A failed scheduled run opens an
issue.

The model found one undocumented ordering rule. A fresh enqueue and a wake
backdate `scheduled_at` by `IMMEDIATE_SCHEDULE_SKEW_SECS` (5 s). An orphan
requeue stamps `clock_timestamp()`. A claim therefore took any start made
in the next 5 seconds before the requeued orphan. Issue #1824 then changed
the claim order: a new start sorts 30 seconds later, and a requeued orphan
is a continuation. The model now follows that order, and
`a_requeued_orphan_sorts_ahead_of_a_fresh_start` pins it.

**Crash-history checks.** `tests/integration/history_checker.rs` is a
Porcupine-style linearizability checker with Jepsen `ok`, `fail` and `info`
outcomes. It ships two models: `StartIdempotency` and `ExactlyOnceFire`.
A test bounds its crashed operations once nothing they started can still
run, so a late effect is a violation.
`tests/integration/history_crash_tests.rs` drives concurrent idempotent
starts and scheduler replicas while it drops request futures and terminates
backends. The two #350 chaos reproducers now also check their histories.

**Evidence.** Three engine mutations each fail the lifecycle model with a
short shrunk sequence. The mutations are: no `attempt` increment on claim,
no crash strike on requeue, and no `wake_requested` fallback. A broken
idempotency window fails the start-history check on every run. A per-fire
unique schedule id fails the random schedule-history check in about one
run of three. The post-start #350 reproducer catches it on every run,
through its history check.
