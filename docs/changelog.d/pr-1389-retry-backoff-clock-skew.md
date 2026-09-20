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

**Fix.** All three now compute `scheduled_at` as `NOW() +
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

No `WorkflowEvent` variant, no migration, no replay impact — the change is
confined to how `scheduled_at` is computed before the write.
