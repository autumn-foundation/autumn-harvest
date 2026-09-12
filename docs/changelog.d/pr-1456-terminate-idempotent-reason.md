## Phase — terminate's idempotent no-op stops claiming a false cancellation (issue #1456)

`terminate_workflow_execution_collect` and `cancel_workflow_execution_collect`
share `CancelledWorkflowExecution::idempotent()` for their already-terminal
no-op path. Its fallback reason — used only when the row's `error` column is
`NULL` — was a single fixed string, `"workflow already cancelled"`, written
for cancel's narrow reuse (reachable there only on an already-`CANCELLED`
row, where `error` is always populated by the original cancellation, so the
fallback was dead code). Terminate reaches the same helper for any
already-terminal state, and `COMPLETED` (and `CONTINUED_AS_NEW`) never
populate `error` — so force-terminating a workflow that had already
completed normally answered `state: "COMPLETED"` with
`reason: "workflow already cancelled"`, asserting a cancellation that never
happened. Follows up on the sibling status-code class of bug fixed in
#1444/#1445.

Found by Snag (exploratory QA): deterministic 10/10 repro against a live
`harvest-dev` instance — start the sample workflow, let it run to
`COMPLETED`, call terminate. `GET /workflows/{id}` confirmed the stored
`error` column was `NULL` and history held no `WorkflowCancelled` event.

Fix: `idempotent()` now falls back to
`default_idempotent_reason(&execution.state)`, which derives the reason
from the row's own state instead of a cancel-specific literal (e.g.
`"workflow already completed"`, `"workflow already timed out"`). Cancel's
call site is unaffected: it only reaches this path on a `CANCELLED` row, and
the derived reason for `CANCELLED` is unchanged —
`"workflow already cancelled"`. A stored `error` still always wins over the
derived default, on both call sites.

`MIGRATED` — the sealed source of a shard migration (issue #964) — is in
`TERMINAL_STATES` and reachable through terminate's own terminal-state
check, but deliberately gets no specific phrase. That row is terminal only
in the sense that nothing more happens on *this* shard; the run itself
stays alive on another shard, so a confident "already migrated" claim
would read as a completed termination it is not. It falls to the generic
`"workflow already in terminal state migrated"` instead, matching the
caution cancel and signal already apply to this state elsewhere.

No new `WorkflowEvent` variant, no migration — a pure read-path fix, no
change to any stored row.

Tests:
- `execution::idempotent_reason_tests` (`autumn-harvest/src/execution.rs`,
  unit, no DB): every named state maps to its exact reason, no state but
  `CANCELLED` claims cancellation, `MIGRATED` and an unrecognised state
  both fall to the generic phrasing, and every state in `TERMINAL_STATES`
  is covered.
- `terminate_idempotent_reason_matches_state`
  (`autumn-harvest-plugin/tests/terminate_integration.rs`, integration,
  real Postgres): a naturally-`COMPLETED` run's terminate response no
  longer claims cancellation; `CONTINUED_AS_NEW` (the other
  never-populates-`error` state) gets its own derived reason; terminate
  against an already-`CANCELLED` row still answers `"workflow already
  cancelled"`; and a stored `error` on a `FAILED` run still passes
  through verbatim.
