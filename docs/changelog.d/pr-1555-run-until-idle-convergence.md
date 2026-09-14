## Fix — `run_until_idle` capped fleet convergence at one decision cycle per call in the presence of a persistently-broken execution (issue #1555)

`run_until_idle`'s outer loop called `self.poll_once().await?`. The bare `?`
propagated ANY per-pass error immediately, so a single call returned after
exactly one internal `poll_once` pass whenever the fleet contained an
execution that errors every cycle (e.g. an unsupported command through
ordinary dynamic control flow, same shape as #1530). A multi-cycle,
unrelated execution elsewhere in the fleet then advanced only one decision
cycle per external `run_until_idle()` call instead of running to completion
in one call, silently under-delivering the documented "repeat `poll_once`
until quiescent" contract. #1536's own regression test for #1530 did not
catch this residual gap because its `healthy_wf` completes in a single
cycle, and one pass is indistinguishable from full convergence for a
one-cycle workflow.

**The fix separates "did this pass error" from "did this pass make
progress".** A new private `poll_once_pass` helper — shared by `poll_once`
and `run_until_idle` — returns both signals instead of collapsing them into
one `Result`. `poll_once`'s public contract is unchanged (`SqliteResult<bool>`,
same error on the same input). `run_until_idle` now keeps calling passes
as long as ANY execution still makes progress, regardless of a per-pass
error, deferring the FIRST error seen across all passes until the fleet
actually quiesces (or the `MAX_ITERATIONS` safety bound is hit). The broken
execution's error is never dropped — it is still returned to the caller,
just no longer at the cost of stalling the rest of the fleet's convergence.

**Design choice, and what was left out of scope.** The issue named two
possible directions: keep looping and defer the error (chosen), or
quarantine a persistently-failing execution out of `running_executions`
after N consecutive failures. Quarantining changes when/whether an
execution's error is reported at all and is a separate, larger design
decision the issue explicitly left open; this fix stays minimal and changes
nothing about error *visibility*, only about how many internal passes one
external call performs.

**Zero unrelated engine impact:** no new `WorkflowEvent` variant, no
migration, no schema change, no public API change. `poll_once`'s contract
and behavior are unchanged, though its body now delegates to
`poll_once_pass`; `poll_once_as_of` and `run_until_blocked`/
`run_until_blocked_as_of` (the single-execution, fail-fast drivers) are
untouched.

Tests, red -> green: `autumn-harvest-sqlite/tests/integration/fleet_fault_isolation.rs`
gained `run_until_idle_converges_a_multi_cycle_execution_in_one_call_past_a_broken_one`,
mirroring the issue's own repro shape — a `broken_wf` reaching
`signal_external_workflow` through dynamic control flow, and a `two_step_wf`
needing two sequential activity-backed decision cycles, started so it sorts
after the broken execution in `ExecutionId` order. It asserts a single
`run_until_idle()` call both still surfaces `SqliteError::Unsupported` AND
drives the unrelated execution all the way to `Completed`. Confirmed RED
(execution left `Running`) before the fix. Full `autumn-harvest-sqlite`
suite (128 integration tests, 45 lib unit tests) is green with zero
regressions.
