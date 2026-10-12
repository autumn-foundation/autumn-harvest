## Feature — SQLite: child workflows, and signal, cancel and await between runs (issue #2026)

Before this change, a SQLite workflow that started a child, awaited another
run, or signalled or cancelled another run was sealed `FAILED` as an
unsupported feature. Composition now works on this backend.

**What changed**

- **Children.** `StartChildWorkflow` and `SpawnDetachedChildWorkflow` start
  the child in the parent's decision transaction. The child is a full run with
  a parent link. Its `workflow_id` is its execution id, and
  `ctx.info().parent_execution_id` names the parent.
- **Results.** An awaited child's terminal writes `ChildWorkflowCompleted` or
  `ChildWorkflowFailed` to its `RUNNING` parent in the same transaction. A
  typed failure keeps `error_type`, `details` and `non_retryable`. A cancelled
  child reports `child workflow cancelled: <reason>`.
- **Parent-close policy.** When a parent ends, each running detached child
  gets its policy: `request_cancel` cancels it with reason `parent closed`,
  `terminate` fails it with `ParentClosed`, and `abandon` leaves it. The
  parent records `ChildWorkflowCascadeApplied`. Every terminal path applies
  it, including the unsupported-feature seal, the panic seal and the
  `TerminateIfRunning` seal.
- **Between runs.** `SignalExternalWorkflow`, `RequestCancelExternalWorkflow`
  and `AwaitExternalWorkflow` resolve in the caller's transaction. A cancel is
  a hard seal, as on Postgres: the target ends `CANCELLED` with reason
  `cancelled by external request`. A signal idempotency key dedups on the
  target. An unknown target fails at once with `target_unknown`, because one
  file has no other shard to wait for.
- **Race losers.** A child that loses a `ctx.race()` or a
  `spawn_child_workflow_timeout` is cancelled with reason
  `lost race to a sibling branch`.
- **Abandoned dispatch.** A failing cycle that drops a fresh child dispatch
  records the `ChildWorkflowStarted` and `ChildWorkflowFailed` pair of issue
  #952. No child row is made.
- **Driving.** A run that waits on a `RUNNING` child or await target returns
  the new `RunState::WaitingWorkflow(target)`. `run_until_blocked` drives that
  target in the same call. The fleet drivers treat it as blocked.

**Behavior change.** `RunState` has a new variant, `WaitingWorkflow`. An
exhaustive `match` on `RunState` needs a new arm. The in-tree agent daemon and
`docs/sqlite-backend.md` §7 are updated. Tests that used a child or an
external command as the unsupported example now use a local activity.

**Invariants.** No new `WorkflowEvent` variant. Every event this backend writes
has the Postgres shape. No write to an existing `harvest_events` row. One
additive schema step: `harvest_executions.parent_exec_id`,
`harvest_executions.parent_close_policy` and `harvest_signals.idempotency_key`,
each nullable, with their indexes. An old file gains them on open.

**Known core limit.** A run cancelled while it waits ends in a
`WorkflowCancelled` that its code never saw. Strict core replay verifies a
cancelled history in full, so it reports that run as still parked. Postgres
writes the same bytes and gets the same result. The tests replay the events
before the cancel in canary mode.

**Tests.** `autumn-harvest-sqlite/tests/integration/composition.rs` has 26
tests. They cover attached and detached children, typed and unsupported child
failures, each parent-close policy, signal and cancel by id and by key,
idempotency, await of completed, failed, cancelled and unknown targets, the
child timeout race, a reopen, mutual awaits, and an abandoned dispatch. Each
test replays every history it writes on the core `WorkflowReplayer`.
`schema::tests::migrate_adds_composition_columns_to_an_old_file` covers the
schema step. The design record is `DESIGN-2026.md`.
