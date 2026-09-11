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

No new `WorkflowEvent` variant, no migration — a pure read-path fix, no
change to any stored row.

Tests:
- `execution::idempotent_reason_tests` (`autumn-harvest/src/execution.rs`,
  unit, no DB): every state in `TERMINAL_STATES` gets a reason naming it,
  and `CANCELLED`'s derived reason is unchanged.
- `terminate_completed_reason_does_not_claim_cancellation` and
  `terminate_failed_reason_keeps_stored_error`
  (`autumn-harvest-plugin/tests/terminate_integration.rs`, integration,
  real Postgres): a naturally-`COMPLETED` run's terminate response no
  longer claims cancellation, and a stored `error` on a `FAILED` run still
  passes through verbatim.
