## Phase — Ownership guard on ordinary workflow terminal writes (issue #1184)

Issue #1182 (and #804 before it) closed the worker-ownership race for one
narrow branch: `handle_suspended_workflow`'s empty-command-set catch-all,
via `commit_terminal_failure_if_still_claimed` /
`fail_suspended_workflow_if_still_claimed`, reusing
`queue::claim_still_held_for_update` (a `FOR UPDATE SKIP LOCKED` recheck of
`worker_id` + `crash_strikes` under a fresh row lock). Every *other*
terminal write — an ordinary completion, an ordinary failure, and an
operator-pause re-park — had no such check at all: a stale or zombie
dispatcher whose claim had already moved to a new owner (poison-pill
reclaim, operator requeue, or a concurrent claim race) could commit a
decision against a run the new owner was actively driving.

**What's guarded now**, all via the identical `claim_still_held_for_update`
recheck as the first operation inside the write's own transaction:
`persist_workflow_completion`, `persist_workflow_failure`,
`persist_child_workflow_completion`, `persist_child_workflow_failure`,
`check_paused_and_park`'s operator-pause re-park, and every branch of
`fail_task_and_execution_with_history`. Also guarded:
`persist_workflow_continue_as_new`'s own seal transaction — a related gap
of the identical shape found while auditing the call chain, not in the
issue's originally-confirmed list.

**Design deviation from the issue's proposed fix direction, deliberate:**
the issue's sketch was to widen `queue::fail_task`/`queue::complete_task`
themselves with `worker_id`/`crash_strikes` parameters and filter on them
in SQL. That would touch roughly twenty unrelated call sites across
activity/timeout/session code that this issue never analyzed. Instead, the
guard runs once per terminal-write transaction, immediately before that
transaction's `fail_task`/`complete_task` call: since `claim_still_held_for_update`
takes the task row's lock and it is held for the rest of the transaction,
the later unfiltered `fail_task`/`complete_task` call is provably safe
without touching either function — the same reasoning
`commit_terminal_failure_if_still_claimed` already relies on for its own
guarded call. `queue::fail_task`/`complete_task` are untouched.

**A new sibling error variant, not a reuse:** `HarvestError::TerminalWriteClaimAmbiguous`
mirrors `SuspendedClaimAmbiguous`'s full contract (propagate via `?` through
the whole persistence transaction so it rolls back before any release is
attempted, then a standalone release afterward) but is kept distinct
because the two cover different write shapes (empty-command-set suspension
vs. an ordinary completion/failure/park). `queue::release_terminal_workflow_claim`
and `worker::handle_ambiguous_terminal_write_claim` are the matching release
pair, wired into `process_task` next to the #1182 handler; both share
`release_suspended_workflow_claim`'s proven query via one `_inner` impl
(mirroring the existing `park_workflow_task`/
`park_workflow_task_preserving_capability_misses` pattern) rather than
duplicating it.

**A real deadlock found and fixed during self-review, not merely a
theoretical risk:** the first cut of the `fail_task_and_execution_with_history`
guard took the task-row lock before the execution row, on the one caller
path (`handle_session_acquire`/`handle_session_release`, reached via
`fail_task_and_execution`) that opens no prior transaction and so holds no
execution-row lock ahead of it — inverting the documented
`harvest_task_queue` lock order (execution row, then task row) and cycling
against `timeout::force_fail_activity`, which locks the same two rows in
the documented order. Fixed by locking the execution row first (reusing the
existing `lock_workflow_execution_row_only`, already built for exactly this
"ordering only, no history load" need) before any task-row touch, in every
branch that has an execution to lock.

No new `WorkflowEvent` variant, no migration, no schema change — this is a
transaction-ordering and error-handling change over existing tables.

**Tests, red → green → refactor:** `autumn-harvest/tests/integration/
terminal_write_ownership_tests.rs`, nine deterministic tests following the
`capability_miss_tests.rs` pattern for the #1182 sibling — seed a claimed
task, transfer its claim to `"thief"` on a second connection, call the
guarded function with the stale `worker_id`, assert `Err` naming the exact
task id, no terminal event appended, execution state untouched, task row
exactly as `"thief"` left it. One site (`persist_workflow_failure`) was
red-proven by hand (temporarily disabling its guard reproduces the exact
vulnerability); a multi-angle self-review (correctness/concurrency,
adversarial test-quality, and a general code-review pass) then found and
closed: the ABBA deadlock above; two tests whose placeholder parent
execution id meant a removed guard would still fail the test for an
unrelated `NotFound`, not the guard's absence (now seed a real parent); and
a missing test for the `SKIP LOCKED` false-positive recovery path (a claim
that was never actually lost still gets released, mirroring #1182's own
`a_dispatcher_that_still_owns_the_claim_is_released_when_skip_locked_is_a_false_positive`).
All 9 new tests plus the 46 pre-existing #1182/#804 `capability_miss_tests`
pass against a real Postgres 16.
