## Phase — Lifecycle invariant gaps: guarded transitions, one transition table, CI that runs the DAG suites

An audit set every workflow execution, task queue and DAG lifecycle
transition against the checks that enforce it. This change fixes the
defects it found and closes the coverage gaps it can close without a schema
change.

**Transition guards.**

- `enforce_workflow_timeout` (`timeout.rs`) read the locked execution row but
  never checked its state. A workflow task timeout could seal a `PAUSED` run
  `TIMED_OUT` and leave its pause record set. It could also overwrite a run
  that another path had already sealed, if that run still owned an open task.
  The locked state now decides: `RUNNING` or `PAUSED` seals, a terminal state
  only fails the orphan task, and a staged `MIGRATING` copy is left alone.
  `update_workflow_execution_timed_out` now carries its own
  `state IN ('RUNNING','PAUSED')` predicate and clears the pause record.
- `replace_execution` (`execution.rs`) sealed a finished prior
  `CONTINUED_AS_NEW` with an UPDATE by id: no state predicate and no row-count
  check. The new `seal_replaced_execution` re-reads the state under the lock,
  accepts only `COMPLETED`, `FAILED`, `CANCELLED` or `TIMED_OUT`, and writes
  with a compare-and-set.
- A replaced run read back as a success. `terminal_raw_result` maps
  `CONTINUED_AS_NEW` to `Ok(output)`, so a replaced `FAILED` run returned a
  null output. `await_external_workflow` on it waited forever for a
  `WorkflowContinuedAsNew` event that was never written. The seal appends no
  event, so the run's own last lifecycle event still names its outcome.
  `execution::replaced_run_outcome` reads it, and both
  `WorkflowHandle::load_effective_execution` and
  `read_external_await_outcome` now report it. A real continue-as-new is
  unaffected. A run whose workflow task timed out records `WorkflowFailed`, so
  once replaced it reads back as `FAILED`, not `TIMED_OUT`.
- `inline_cancel` appended `WorkflowCancelled` and then ignored a zero-row
  UPDATE. It now rolls back.
- `terminate_workflow_execution_collect` returned an idempotent success for
  a `MIGRATED` seal whose live copy still runs on another shard. It now
  returns `ShardUnavailable`, as cancel already did. A seal whose live copy is
  observed terminal stays an idempotent no-op.
- `reactivate_failed_execution` (DLQ redrive) now refuses a run whose payloads
  were erased. A redrive would otherwise run the failed step on tombstones.

**Retention.**

- The candidate scan required `sticky_worker_id IS NULL`, because retention
  reuses that column as its lease. The worker writes its own id there when it
  seals a run `COMPLETED`, `FAILED` or `CONTINUED_AS_NEW`. So no run a worker
  sealed was ever a retention candidate. The predicate now excludes only a
  live `retention-lease-*` value.
- The delete transaction locked the row but re-checked only legal hold and
  staging. A redrive between the scan and the delete could reopen the run,
  and retention would delete a live run. The locked state is now checked
  against `RETENTION_CANDIDATE_STATES`, and a reopened row is skipped.

**Manual DAG trigger.** `trigger_unified_dag` checked pause and
`max_active_runs` with unlocked reads, then started the run. Two concurrent
manual triggers could both pass the check. The check and the start now run
in one transaction under a `FOR UPDATE` lock on the DAG's schedule rows.
A scheduled tick holds a lease, not that lock, so a tick racing a manual
trigger can still exceed the limit by one.

**DAG definitions.**

- `HarvestBuilder::try_build` skipped every DAG validator for a definition
  that failed to compile, so a cyclic DAG passed and failed only at run time.
  The new `HarvestBuilderError::InvalidDagDefinition` rejects it first.
- Duplicate node names stay legal, as `dag_retry.rs` documents. The new
  `DuplicateNodeNameRule` lint warns that such a DAG cannot be retried from a
  node.
- The `TriggerRule` doc said every rule fires on a root node. Only
  `AllSuccess` and `AllDone` do. The doc and its test now say so, and
  `TriggerRule::Manual` no longer claims a manual trigger exists.

**Append-only invariant.** Continue-as-new patched `last_completion_result`
into the successor's event 0 with an UPDATE after the INSERT: a third
in-place writer of `harvest_events.event_data`. The new
`store::append_events_offloaded_with_codecs_and_patch` applies the patch to
the encoded row before the INSERT. `lifecycle::append_only_guard` fails if
any file other than `erase.rs` and `codec_rotation.rs` writes the column.
`docs/architecture.md` §4 now lists both sanctioned writers.

**One transition table.** The new `lifecycle` module names all ten persisted
states (`WorkflowState`) and every sanctioned transition with its writer
(`TRANSITIONS`). Its tests check that the state set equals the latest `CHECK`
constraint, that the terminal set equals `erase::TERMINAL_STATES`, and that
every named writer exists and names its target state. They also check that
every state is reachable and every open state has an exit, and that only a
DLQ redrive reopens a terminal run. The table is not enforced by a database
trigger. Shard-rebalance operator overrides and test fixtures write `state`
directly by design.

**CI.**

- Four `linux … testing` rows now run `dag_unified_tests`,
  `dag_mapping_tests`, `dag_input_binding_tests` and `dag_signal_gate_tests`.
  No row had enabled both `testing` and `unified-dag-execution`, so the DAG
  walker's semantics never ran in CI.
- `ci_run_coverage::every_feature_gated_core_suite_executes_in_some_row`
  catches that class of gap for any feature-gated suite, DB or not.
- Seven suites leave the `ci_run_coverage` debt allowlist and get `linux`
  rows: `cancellation_tests`, `redrive_tests`, `workflow_task_timeout_tests`
  and `pause_tests`, plus the plugin suites `terminate_integration`,
  `workflow_reset_integration` and `dlq_redrive_integration`. They cover
  cancel, redrive, pause, terminate, reset and the workflow task timeout, and
  no CI step ran them. All pass. `pause_tests` also holds the two new workflow
  task timeout regression tests. `dag_retry_integration` stays allowlisted:
  `reset_without_the_flag_still_forks_an_erased_source` fails in reset-point
  validation, code this change does not touch.
- `specs/scheduler_claim_invariant.rs` is deleted. It was a Verus proof about
  the `harvest_dag_runs` claim, a table dropped in `20260514000000`. It was
  not built anywhere.

No migration. No new `WorkflowEvent` variant. No new `harvest_events` mutator:
this change removes one.
