## Phase X.Y — Every reset fork refuses a PII-erased source (issue #1999)

Batch reset could fork a PII-erased `FAILED`, `CANCELLED` or `TIMED_OUT` run.
The fork resumed on `{"_harvest_erased": true}` tombstones. The refusal was an
opt-in flag, and only DAG retry set it.

- The engine now refuses an erased source on every fork path.
  `reset_workflow_execution` checks under the fork's `FOR UPDATE` row lock.
  `preview_workflow_reset` and `resolve_batch_reset_one` check too, so a dry
  run returns the same result as the real fork.
- `WorkflowResetRequest::refuse_erased_source` is removed. It was
  `#[serde(skip)]`, so the wire does not change. An embedder that set the
  field must delete that line. See
  [the 0.8.0 upgrade guide](../upgrading/0.8.0.md), section 1.4.
- `ResetSkipReason::ErasedSource` is new. A batch reports an erased run as
  `skipped` with `skip_reason: {"type": "erased_source"}`. The other runs in
  the cohort still reset.
- `batch_skip_reason` maps a fork error to a batch skip reason. Before, batch
  reported every fork error as `infrastructure_error`, which tells the operator
  to retry. A refusal for erasure now stays typed, also when the erasure
  commits between the batch resolve and the fork lock.
- The shared reset `409` for an erased source no longer says "DAG run". DAG
  retry keeps its own message.
- [`docs/workflow-reset.md`](../workflow-reset.md) is new. It lists the fork
  surfaces, records the decision, and lists the batch skip reasons. A future
  fork path that calls `reset_workflow_execution`, such as issue #2000, gets
  the refusal from the engine.

No migration. No new `WorkflowEvent` variant. No new `harvest_events` mutator.

Tests, red first:

- `workflow_reset_integration`:
  `batch_reset_skips_an_erased_source_and_resets_the_rest`,
  `batch_reset_preview_reports_an_erased_source_as_skipped`,
  `batch_reset_refuses_an_erasure_that_lands_before_the_fork_lock`,
  `reset_refuses_an_erased_source_with_no_opt_in` and
  `preview_refuses_an_erased_source`. The erased runs cover `FAILED`,
  `CANCELLED` and `TIMED_OUT`. The race test holds an erasure open until the
  fork waits on the row lock.
- `reset.rs`: four pure tests for the skip reason, `batch_skip_reason` and
  `skip_reason_to_error`.
- `api.rs`: `erased_source_409_is_generic_for_reset_and_specific_for_dag_retry`.

`reset_without_the_flag_still_forks_an_erased_source` and
`refuse_erased_source_is_not_settable_from_the_wire` are deleted. They pinned
the old opt-in. `reset_refuses_an_erased_source_under_its_own_row_lock`
moved from `dag_retry_integration` to `workflow_reset_integration` as
`reset_refuses_an_erased_source_with_no_opt_in`.
