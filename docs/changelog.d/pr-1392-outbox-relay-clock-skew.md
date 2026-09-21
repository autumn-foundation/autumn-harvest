## Phase — Outbox relay backoff deadlines computed on Postgres's own clock (issue #1392)

A Codex review round on PR #1386 (issue #1227), past that PR's 5-round
review budget, found that the completion-trigger outbox's backoff
deadlines (`next_attempt_at`) are computed and checked on each scanner
replica's own **host** clock, not Postgres's. In a multi-replica
deployment, host-to-host clock skew between replicas can collapse the
intended 5-second cadence into repeated immediate retries: a deadline
stamped by a slow replica can already look due to a faster one.

This is the outbox's analogue of issue #1389 (task-queue retry deadlines
vs. host/DB skew), specific to the two writers and two readers PR #1386's
Finding 4 introduced:

- `relay_gate_checked_start`'s `FOR UPDATE SKIP LOCKED` claim re-check now
  compares `next_attempt_at <= NOW()` in the SQL text itself, not against
  a host-sampled `chrono::Utc::now()` bound as a parameter.
- Its `QuotaExceeded` arm now stamps `next_attempt_at` as
  `clock_timestamp() + make_interval(secs => $N)`, computed inside the
  same claim transaction rather than `chrono::Utc::now() +
  QUOTA_REDEFER_BACKOFF`. `clock_timestamp()`, not `NOW()`: this
  transaction already did prior work (the claim, plus the cross-shard
  admission attempt that raised the error), and `NOW()` is frozen at the
  transaction's start.
- `stamp_outbox_relay_backoff` (the backoff stamped when a scan cannot
  even attempt a relay — a missing target-shard pool or a connection
  failure) uses the same `clock_timestamp()` pattern.
- `enforce_completion_triggers_outbox_with_codecs`'s retry-tier batch
  filter now compares against SQL `NOW()` instead of a bound host
  timestamp, so eligibility agrees with the clock the backoff was stamped
  on.

**Also fixed, found while validating this change.** `queue.rs`'s
`requeue_workflow_task_for_quota_retry` (added by issue #1391's fix, PR
#1658) did not compile: it called `PendingRequeueChangeset::new` with the
signature that predates issue #1389's DB-clock refactor
(`new(next_run, previous_error)`, instead of the current
`new(previous_error)`), and its `UPDATE`'s `.set(...)` clause never
actually set `scheduled_at` at all — its own quota-retry backoff was
silently never written to the database row. Brought in line with its
siblings `requeue_workflow_task_nd_blocked` and
`requeue_workflow_task_after_panic`, which already compute `scheduled_at`
as `clock_timestamp() + make_interval(secs => ...)`.

**Tests.** Unit tests in `completion_trigger.rs` pin the generated SQL
text for all four sites (`RELAY_CLAIM_QUERY`,
`OUTBOX_RETRY_ELIGIBLE_PREDICATE`, `OUTBOX_RELAY_BACKOFF_STAMP_QUERY`, and
a `quota_blocked_backoff_query()` shape helper mirroring the
`queue::requeue_after_panic_query` precedent), confirming each uses
`NOW()`/`clock_timestamp()` rather than a bound host timestamp. A new
integration test,
`quota_blocked_outbox_backoff_lands_on_the_database_clock` in
`quota_enforcement_tests.rs`, drives a genuine `QuotaExceeded` outbox
relay outcome against a real Postgres and asserts the written
`next_attempt_at` lands within a tight tolerance of the database's own
`NOW()` plus the backoff — never the test process's `chrono::Utc::now()`.
`queue.rs`'s existing
`requeue_workflow_task_for_quota_retry_query_clears_sentinel_and_wake`
test was updated to assert `scheduled_at` is present in the generated SQL,
closing the gap that let the missing `.set(...)` entry ship silently.

No `WorkflowEvent` variant, no migration, no replay impact — confined to
how `next_attempt_at` is computed and checked before the outbox relay's
`UPDATE`/`SELECT` statements.
