## Fix — durable workflow terminal metrics survive post-commit cancellation (issue #1348)

`process_workflow_task`'s `Persisted` arm computes this cycle's terminal
metrics (`harvest.workflow.duration`, `.history_size`,
`.continue_as_new`, `harvest.workflow.terminal`, canary metrics) before
the persist transaction, then emits them only after that transaction
commits (issue #1184) — so a rolled-back attempt cannot double-count.

**The gap.** The emit call used to run last in the `Persisted` arm,
after several deferred post-commit `.await`s: the dispatch-hint flush,
the schedule-failure counter, unfinished-handler checks, and the
history-bloat read. `run_under_workflow_body_budget` (issue #494) wraps
the whole decision cycle in a `workflow_task_timeout` and drops it,
uncompleted, on a timeout — even when the persist transaction inside it
already committed. A timeout landing on one of those deferred `.await`s
dropped the cycle before the emit call ran, permanently losing that
decision's metrics even though its outcome was durable. Found on Codex
review of PR #1346 (round 7); filed as its own issue, past that PR's
review-round budget.

**The fix.** Move the emit call to the first thing the `Persisted` arm
does, immediately after the correction for a `ContinuedAsNew` outcome
redirected to a terminal failure (issue #1161) and before the
`WORKER_AFTER_OUTER_COMMIT` chaos point. No `.await` separates it from
the persist commit above, so no cancellation can land between them —
this removes the window rather than narrowing it. This reintroduces a
narrower double-count risk (the emit call succeeds, then a later
deferred step in the same cycle fails) that the issue accepts: a
metrics-only failure mode already tolerated elsewhere in this codebase
(`emit_history_bloat_warning_if_crossed`'s at-least-once semantics).

**Test.** New chaos reproducer
`chaos_repro_1348_terminal_metrics_survive_post_commit_cancellation`
and its supporting helper `chaos_drive_one_workflow_task_cancel_at_hold`
(`worker.rs`, `#[cfg(feature = "chaos")]`). The helper drives a decision
cycle and deterministically drops it — no wall-clock race — the instant
it reaches a chaos `HoldHandle`'s rendezvous, mirroring the drop
`run_under_workflow_body_budget` performs on a real timeout. Holding at
the existing `WORKER_AFTER_OUTER_COMMIT` chaos point (already the first
statement in the `Persisted` arm) and cancelling there reproduces the
bug exactly: the persisted outcome is `COMPLETED` in the database
either way, but on the pre-fix ordering the terminal metrics were never
recorded. Fails on the pre-fix ordering, passes on the current one.

**No new `WorkflowEvent` variant, no migration, no replay/determinism
impact** — this only reorders in-process metrics emission relative to
already-durable state.
