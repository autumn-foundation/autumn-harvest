## Phase — Canary replay's completing path now agrees with its suspended path at the frontier (issue #1175)

`executor.rs::run_workflow_canary` had two terminal arms that reach the
frontier — recorded history fully consumed — and disagreed about whether a
new command emitted there is drift. The suspended arm (`Err(_elapsed)`)
already tolerated it, checking only `history_has_unconsumed_events()` before
returning `Suspended`. The completing arm (`Ok(Ok(output))`) additionally
rejected the run whenever `ctx.drain_commands().iter().any
(is_replay_significant_command)` — and `RecordSideEffect`, emitted by
`ctx.system_now()` / `new_uuid()` / `random_*()`, counts as significant. A
workflow that consumed all recorded history, read the wall clock, and then
returned `Ok` (rather than parking on another activity) was reported as
`0 clean, 1 diverged` on completely unchanged code.

Reaching the end of the workflow function with recorded history fully
consumed is exactly as legitimate an outcome here as parking on the next
await point — the candidate build is making forward progress either way,
whether it happens to return or await next. The completing-path rejection is dropped; that
arm now falls into the same `else` branch that already returns `Completed`
for the issue #952 `at_terminal_failure_frontier()` case (the two produced
an identical `WorkflowOutcome::Completed` once the rejection was removed, so
they're one arm with one combined rationale comment rather than two branches
with the same body). `run_strict_with_ctx` (true strict replay, where the
history is genuinely finished and a new command *does* mean the code
changed) is untouched — its identical-looking rejection stays.

Genuine drift on the completing path is unaffected: a command that
mismatches recorded history (wrong activity name, wrong order, …) resolves
to `Diverged`/`NoMatch` in the matcher and fails the cycle long before
reaching this arm.

This fixes all three consumers reached via `replay_canary_snapshot`:
`replay_bundle` (#798 in-flight drift gate), `WorkflowReplayer::run_canary`
(#512 deploy canary), and `replay_diagnosis` (#614 endpoint).

No new `WorkflowEvent` variant, no migration, no `harvest_events` write path
change — this is a pure replay-classification fix confined to
`run_workflow_canary`.

**Tests, red → green:** `autumn-harvest/tests/integration/replay_drift_tests.rs`
gained `a_run_that_completes_after_a_builtin_side_effect_at_the_frontier_is_not_drift`
(the issue's reachable shape: consume `step_one`, read `ctx.system_now()`,
return `Ok` — reproduces `0 clean, 1 diverged` before the fix, `1 clean` after)
and its control, `a_completing_run_with_a_mismatched_activity_is_still_drift`
(a handler that replays a different activity name than recorded still exits
non-zero, proving the relaxation doesn't swallow real divergence). Both run
through `replay_bundle`, the same path all three consumers share (`replay_bundle`,
`WorkflowReplayer::run_canary`, `replay_diagnosis` all resolve to the same
`replay_canary_snapshot_effective` → `run_workflow_canary` call). A new
`strict_replay_still_rejects_a_completing_run_with_a_builtin_side_effect_past_end_of_history`
pairs with the pre-existing `strict_replay_still_rejects_a_builtin_side_effect_past_end_of_history`
(the parking shape) to confirm strict mode's rejection survives unchanged for
the completing shape too. Full crate suite green: 2328 lib unit tests, 1852
integration tests (including all 79 `replay_drift_tests`), `cargo clippy
--all-targets` clean.
