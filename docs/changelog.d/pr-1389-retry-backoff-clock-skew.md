## Phase — Retry deadlines computed on Postgres's own clock (issue #1389)

A Codex review round on PR #1386 (issue #1227) found that
`queue::requeue_for_retry` computes its retry deadline on the **host**
clock (`Utc::now() + delay`), but `claim_task` checks eligibility
(`scheduled_at <= NOW()`) on **Postgres's** clock. When the host clock
trails Postgres's, the written deadline can already be due by the time
`claim_task` checks it — silently defeating the backoff. This directly
undermined issue #1227's own fix: its 500ms-3s quota-retry backoff is
smaller than the pre-existing 5s tolerated skew.

Two siblings share the identical defect: `requeue_workflow_task_nd_blocked`
and `requeue_workflow_task_after_panic` compute their deadlines the same
way.

**Fix.** All three now compute `scheduled_at` as `clock_timestamp() +
make_interval(secs => $N)` inside the `UPDATE` statement itself, stamping
it on Postgres's own clock — the same clock `claim_task` later checks it
against. This mirrors the pre-existing `release_task_for_capability_miss_query`
precedent in the same file, rather than introducing a new pattern.

An earlier version of this fix padded the host-computed deadline by a
skew allowance instead (mirroring `IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE`
applied in the opposite direction). A code-review pass found three real
costs to that approach: it added a flat 5s to every retry even with zero
clock skew, inflating author-configured `RetryPolicy` backoffs; it could
disagree with the host-clock `schedule_to_close_deadline_exceeded` gate
near a task's deadline; and it needed a special zero/negative-delay
carve-out to avoid stranding a queue-pause reset (confirmed by observing
`queue_pause_tests::pause_holds_dispatch_and_resume_releases_it` fail
against an unconditionally-padded version). Computing the deadline on
Postgres's own clock removes all three at once: there is no padding to
inflate a normal retry, no host-vs-DB gate mismatch, and no carve-out,
since `make_interval(secs => 0)` is naturally a no-op.

`PendingRequeueChangeset` no longer carries `scheduled_at`; each of the
three requeue functions sets it via a bound SQL expression instead, and
reads the actual persisted value back via `RETURNING` for the
[issue #1312] dispatch hint.

**Tests, red → green.** New integration suite
`tests/integration/retry_clock_skew_tests.rs` covers all three requeue
functions plus a zero-delay case: each compares the written `scheduled_at`
against a query using the database's own `NOW()` (not the host clock),
asserting the held duration lands within a tight tolerance of `delay` —
a two-sided check that fails equally on a dropped delay or a re-inflated
one. Confirmed each test fails against the pre-fix code and against a
deliberately re-inflated version of the fix, and passes with the fix
restored. Unit-level shape tests
(`pending_requeue_changeset_nulls_out_none_fields_in_generated_sql`,
`requeue_after_panic_query_resets_and_unpins_the_task_row`) pin the exact
generated SQL.

Also fixed: the new integration suite was initially missing from
`.github/ci/integration-suites.txt`, so it compiled but never actually
ran in CI — caught by `ci_run_coverage`'s own guard test before merge.

**Second review round found two more clock-consistency gaps in the DB-clock
fix itself.**

1. `schedule_to_close_deadline_exceeded` — the gate deciding whether a retry
   still fits before `schedule_to_close_at`, short-circuiting to a terminal
   timeout otherwise — still evaluated the host clock, while
   `requeue_for_retry` now stamps `scheduled_at` from Postgres's clock.
   Under real host/DB skew the gate could pass while the written
   `scheduled_at` actually lands past the deadline, stranding the row until
   the timeout scanner sweeps it. Its in-transaction recheck in
   `record_schedule_to_close_activity_timeout` had the identical gap. Both
   now read a new `queue::db_clock_now` helper instead.
2. `NOW()` is frozen at the enclosing transaction's start, not the current
   time. `requeue_workflow_task_nd_blocked` runs inside a transaction that
   already did other work (a row lock, a search-attrs write), so a slow
   prior step could understate the backoff, or erase it outright. All three
   requeue functions (and `db_clock_now`) now use `clock_timestamp()`
   instead, which reads the real time at execution — the same distinction
   `queue_pause::resume_shift_scheduled_at_query`'s own doc comment already
   documents for this exact pitfall.

New test `requeue_workflow_task_nd_blocked_uses_the_live_clock_inside_a_transaction`
reproduces the transaction-frozen-clock path with `pg_sleep` standing in for
real prior work. Confirmed red against `NOW()`, green against
`clock_timestamp()`.

**Third review round: sub-millisecond delays were truncated to zero.**
`delay_secs` converted `delay` to seconds via `num_milliseconds()`, which
truncates. `JitterPolicy::Full` picks a uniform value between zero and the
base interval, so a sub-millisecond positive delay is reachable for a
short-interval retry policy — truncation rounded it down to zero, an
immediate retry instead of a brief one. Switched to `num_microseconds()`.
New test `delay_secs_preserves_sub_millisecond_precision` pins it.

A fourth finding — the in-process/Redis dispatch hint's `due` time compares
`scheduled_at` (now DB-clock) against the dispatcher's host clock — was
investigated and left out of this PR. `queue::due_dispatch_hints_page`, the
reconcile sweep backing that hint, queries `scheduled_at <= NOW()` directly
against Postgres on its own 1-second default interval, so a stale hint
self-corrects within that bound regardless of the dispatcher's own clock.
Real, but bounded, self-healing, and in a separate subsystem from this
issue's own scope.

No `WorkflowEvent` variant, no migration, no replay impact — the change is
confined to how `scheduled_at` is computed before the write, and to the two
retry-vs-timeout gates that must now agree with it.
