## Phase — Ownership guard on the pause fast-path park (issue #1347)

Issue #1184 added a worker-ownership recheck
(`queue::claim_still_held_for_update`) to every ordinary terminal-write
path: completion, failure, and `check_paused_and_park`'s own
operator-pause re-park. One site kept the old, unguarded shape:
`process_workflow_task`'s early, non-locking pause fast path, which called
`queue::park_workflow_task` directly. `park_workflow_task` filters only on
`task_id` and `state = 'RUNNING'`, with no `worker_id` check, so a stale
dispatcher whose claim had already moved to a new owner (poison-pill
reclaim, operator requeue, or a concurrent claim race) could still clear
the new owner's claim and sticky affinity on this one path — found on
Codex round 6 review of PR #1346, filed separately per that PR's round
budget.

**Fix**: route the fast path through `check_paused_and_park` instead of
calling `queue::park_workflow_task` directly. The fast path's own
non-locking read stays as a cheap pre-check (avoiding the row lock in the
overwhelmingly common non-paused case); once it observes `PAUSED`, the
actual park now runs inside a transaction that calls
`check_paused_and_park`, which re-checks `PAUSED` under a `FOR UPDATE`
lock on the execution row, then re-derives the task claim under
`claim_still_held_for_update` before parking. A lost claim now surfaces
as `HarvestError::TerminalWriteClaimAmbiguous`, handled by the existing
`handle_ambiguous_terminal_write_claim` release path in `process_task` —
the same mechanism issue #1184 wired up.

**A special case removed, not just a guard added**: the old fast path had
its own handling of `park_workflow_task`'s `had_wake_requested` return
value, working around a documented raced-wake hazard against
`resume_workflow_execution` (PR #901 review) — the fast path's read took
no lock, so a resume racing the park in the gap could have its wake
silently swallowed. Routing through `check_paused_and_park`'s `FOR
UPDATE` lock on the execution row closes that gap by construction:
`resume_workflow_execution` takes the identical row lock and calls
`wake_workflow_task` inside the same locked transaction, so a concurrent
resume always serialises strictly after this park commits and issues its
own fresh wake. The special-case handling and its comment were removed as
dead weight, not merely left beside the new guard.

No new `WorkflowEvent` variant, no migration, no schema change — this
reuses #1184's existing guard and error variant over an existing call
path.

**Tests, red → green**:
`pause_fast_path_makes_no_terminal_decision_when_the_claim_moved` in
`autumn-harvest/tests/integration/pause_tests.rs`, following the shape of
the file's existing `pause_during_inflight_decision_task_discards_pending_commands`:
a real worker runs a handler that sleeps 300ms before its first command,
giving a window to steal the task's claim (`worker_id` -> `"thief"`) and
pause the execution while the decision is mid-flight, before the fast
path runs. Confirmed red by hand against the pre-fix code (the stolen
row's `worker_id` reverted from `"thief"` to `None`, reproducing the
exact clobber the issue describes) and green against the fix (the row is
untouched: state stays `RUNNING`, `worker_id` stays `"thief"`, no
`TimerStarted` is appended). The pre-existing
`terminal_write_ownership_tests.rs` suite (11 tests, including
`check_paused_and_park_makes_no_terminal_decision_when_the_claim_moved`)
and the sibling ordinary-pause regression test both still pass unchanged.
