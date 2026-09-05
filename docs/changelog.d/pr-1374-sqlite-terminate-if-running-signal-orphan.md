## Phase — `TerminateIfRunning` cleans up an orphaned undelivered signal (sqlite backend, issue #1374)

🪝 Snag exploratory QA finding: `autumn-harvest-sqlite`'s `cancel_and_seal_prior`
(the `TerminateIfRunning` reuse-policy cleanup path) already deleted a sealed
prior's PENDING task rows and unfired timers — see
`terminate_if_running_cleans_up_orphan_timer` — so nothing orphaned by the
replace could be claimed or fire against a run that will never be driven
again. It had no equivalent for a staged, undelivered `harvest_signals` row.

An orphaned signal can never mis-fire (the sealed prior is never driven
again), so this was never a correctness hazard on its own. But this backend
documents no retention/GC pass at all as a v0.1 non-goal, unlike the Postgres
core, which reclaims old rows via a separate retention subsystem. So a
signal sent to a still-RUNNING execution that the workflow never reaches a
matching `wait_for_signal`/`wait_for_signal_timeout` for before being
replaced via `TerminateIfRunning` stayed in `harvest_signals` with
`delivered = 0` forever, growing without bound across repeated
send-signal-then-replace cycles against the same
`(workflow_name, workflow_id)` key.

Adds `queue::delete_undelivered_signals_for_execution` (mirroring the
existing `delete_unfired_timers_for_execution`). No new `WorkflowEvent`
variant, no migration — a `harvest_signals` row deletion only. Regression
test:
`reuse_policy::terminate_if_running_orphans_an_undelivered_signal_across_repeated_cycles`
in `autumn-harvest-sqlite/tests/integration/reuse_policy.rs` — RED
pre-fix (3 orphaned rows across 3 cycles), GREEN post-fix (0).

**Review follow-up (same PR):** the first pass only called the new deletion
inside `cancel_and_seal_prior`'s `if prior_state == "RUNNING"` branch. Codex
review correctly flagged that a signal staged while a prior is RUNNING can
outlive it even when the prior reaches COMPLETED/FAILED on its own (the
workflow never awaits that signal name) — that prior is sealed through the
same function's already-terminal path, which skips the cancellation branch
entirely, so the row was just as orphaned by a different trigger. Moved the
deletion outside the `if` so it runs for every prior being sealed, not only
a cancelled RUNNING one. Regression test:
`reuse_policy::terminate_if_running_orphans_an_undelivered_signal_on_an_already_completed_prior`
— RED against the first pass, GREEN after.
