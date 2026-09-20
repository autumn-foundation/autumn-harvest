## Phase — Retry backoff holds under host/DB clock skew (issue #1389)

A Codex review round on PR #1386 (issue #1227) found that
`queue::requeue_for_retry` computes its retry deadline on the **host**
clock (`Utc::now() + delay`), but `claim_task` checks eligibility
(`scheduled_at <= NOW()`) on **Postgres's** clock. When the host clock
trails Postgres's by up to the pre-existing `IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE`
(5s), the written deadline can already be due by the time `claim_task`
checks it — silently defeating the backoff. This directly undermined issue
#1227's own fix: its 500ms-3s quota-retry backoff is smaller than that
tolerated skew.

Two siblings share the identical defect: `requeue_workflow_task_nd_blocked`
and `requeue_workflow_task_after_panic` compute their deadlines the same
way.

**Fix.** All three now go through a new shared `retry_scheduled_at(delay)`
helper. A positive delay is padded by `RETRY_SCHEDULE_SKEW_ALLOWANCE` (the
same 5s magnitude as `IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE`, applied in the
opposite direction), so the deadline holds `delay` past Postgres's own
clock even under that much skew.

A zero (or negative) delay is deliberately left unpadded: it carries no
backoff to protect, and padding it would make an immediate "no backoff"
reset look like a still-in-progress backoff to the queue-pause resume's
credited-wait formula (`queue_pause::resume_shift_scheduled_at_query`),
which only shifts a row that is already due. Confirmed by running the
existing `queue_pause_tests::pause_holds_dispatch_and_resume_releases_it`
test against an earlier, unconditionally-padded version of the fix: it
failed with a padded zero-delay reset landing outside the resume shift's
window and staying unclaimable after resume.

**Tests, red → green.** New unit tests
`queue::tests::retry_scheduled_at_pads_the_delay_by_the_skew_allowance` and
`::retry_scheduled_at_does_not_pad_a_zero_delay` pin both branches.
New integration suite `tests/integration/retry_clock_skew_tests.rs` covers
all three requeue functions: each calls the function with a 2s delay, then
compares the written `scheduled_at` against a query using the database's
own `NOW()` (not the host clock), asserting the deadline still holds delay
plus the skew allowance. Confirmed each new test fails against the
pre-fix code (production code reverted, rebuilt, tests observed to fail
by exactly the missing allowance) and passes with the fix restored.

No `WorkflowEvent` variant, no migration, no replay impact — the change is
confined to how `scheduled_at` is computed before the write.
