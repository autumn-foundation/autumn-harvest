## Feature — SQLite: child workflows, and signal, cancel and await between runs (issue #2026)

Before this change, the SQLite backend rejected composition. A workflow that
started a child, awaited another run, or signalled or cancelled another run
ended `FAILED` as an unsupported feature. Composition now works on this
backend.

**What changed**

- **Children.** `StartChildWorkflow` and `SpawnDetachedChildWorkflow` start
  the child in the parent's decision transaction. The child is a full run with
  a parent link. Its `workflow_id` is its execution id. Both
  `ctx.info().parent_execution_id` and the new `SqliteRuntime::parent_execution`
  name the parent. An update replay and a query replay see the same parent link.
- **Results.** An awaited child's terminal writes `ChildWorkflowCompleted` or
  `ChildWorkflowFailed` to its `RUNNING` parent in the same transaction. A
  typed failure keeps `error_type`, `details` and `non_retryable`. A cancelled
  child reports `child workflow cancelled: <reason>`, and a terminated child
  `child workflow terminated: <reason>`. A due child-timeout
  deadline fires before a late child's result, as on Postgres (issue #779).
  The backend reads the clock again when a body returns. A close and a
  signal to another run use that instant, as Postgres uses `NOW()`.
- **Parent-close policy.** When a parent ends, each running detached child
  gets its policy. `request_cancel` cancels it with reason `parent closed`.
  `terminate` fails it with `ParentClosed`. `abandon` leaves it running. The
  parent records `ChildWorkflowCascadeApplied`. Every terminal path applies the
  policy. The cap check before a terminal event or a cancel seal counts these
  events, as on Postgres. A worklist carries the cascade down a chain of any depth.
- **Between runs.** `SignalExternalWorkflow`, `RequestCancelExternalWorkflow`
  and `AwaitExternalWorkflow` take effect in the caller's transaction. A
  cancel is a hard seal, as on Postgres: the target ends `CANCELLED` with
  reason `cancelled by external request`. A signal idempotency key dedups on
  the target, and by workflow id across every run of that id. A keyed signal
  row outlives the close of its run. An unknown target fails at once with
  `target_unknown`, because one file has no other shard to wait for.
- **Race losers.** The parent cancels a child that loses a `ctx.race()` or a
  `spawn_child_workflow_timeout`. The reason is `lost race to a sibling
  branch`. A losing child that never started is skipped.
- **Abandoned dispatch.** A failing cycle that drops a fresh child dispatch
  records the `ChildWorkflowStarted` and `ChildWorkflowFailed` pair of issue
  #952. The backend makes no child row.
- **Driving.** A run that waits on a `RUNNING` child or await target returns
  the new `RunState::WaitingWorkflow(target)`. `run_until_blocked` drives each
  such target in the same call, with a cycle budget per run. The fleet drivers
  treat it as blocked.
- **Cancel, terminate and continue-as-new (issues #2021, #2023).** The
  cooperative `cancel`, `terminate`, a history cap and every other seal close
  the run, so its parent and detached children get the result. A child that
  continues as new fails with the Postgres text, `continue_as_new is not
  supported in child workflows in this release`. Its last cycle keeps its
  bookkeeping, as any failing cycle does. An await follows a target
  that continues as new to its last run. A `TERMINATED` target gives
  `target_terminated`.
- **Reuse policy.** `AllowDuplicateFailedOnly` replaces a `CANCELLED` prior,
  as core does.
- **Core replay fix.** `HistoryMatcher` now resolves a stashed await whose
  terminal lands past a sibling's child or activity events. Before, a
  `join!(await_external_workflow, spawn_child_workflow)` parked permanently on
  both backends.

**Breaking change.** `RunState` has a new variant, `WaitingWorkflow`. An
exhaustive `match` on `RunState` needs a new arm. This change updates the
in-tree agent daemon and `docs/sqlite-backend.md` §7. Tests that used a child,
an external command, a local activity or a search-attribute upsert as the
unsupported example now use an external activity.

**Invariants.** No new `WorkflowEvent` variant. Every event this backend writes
has the Postgres shape. No write to an existing `harvest_events` row. One
additive schema step adds two nullable columns and an index:
`harvest_executions.parent_exec_id` and
`harvest_executions.parent_close_policy`. An old file gains them on open. A
signal to another run stores its key in `harvest_signals.idempotency_key`,
which issue #2028 added, and dedups through the same helpers as
`send_signal_with_idempotency`.

**With issue #2029.** A run that passes its `execution_timeout` closes like
any terminal run. Its detached children get their policy, and an awaiting
parent records a `ChildWorkflowFailed` with the timeout text, as on Postgres.
A failed run that retries does not close: its successor starts in the same
transaction, and its detached children keep running, as on Postgres. A child
keeps the `execution_timeout` of its type. A detached child also keeps the
retry policy of its type. An awaited child never retries, as on Postgres. A
cancel from another run, a race-loser cancel and a `request_cancel` cascade
record the close time and release the run's mutex, as every terminal path
does.

**Known limits.**

- A run cancelled while it waits ends in a `WorkflowCancelled` that its code
  never saw. Strict core replay verifies a cancelled history in full, so it
  reports that run as still parked. Postgres writes the same bytes and gets
  the same result. The tests replay the events before the cancel in canary
  mode.
- This backend writes a mixed batch in command order. Postgres writes the
  external events first. Each event is the same, and replay accepts both
  orders.

**Tests.** `autumn-harvest-sqlite/tests/integration/composition.rs` has 63
tests. Each test replays every history it asserts on the core
`WorkflowReplayer`, with the run's ids. They cover:

- attached and detached children, and a three-level attached chain;
- typed, uncaught, unsupported and panicking child failures;
- each parent-close policy, a detached chain cascade, and a detached spawn in
  a failing cycle;
- signal and cancel by execution id and by workflow id, keyed signals, and
  dedup after a cancel and across runs;
- await of completed, failed, cancelled, superseded and unknown targets;
- the child timeout race, and a late child, a long terminal cycle, a long
  cancel cleanup, a long final panic or a late signal that loses to its
  deadline;
- a reopen, mutual awaits, a blocked target beside a working sibling, and a
  run that its own cancel seals;
- `join!` of an await and a child in both orders;
- an operator cancel and a terminate of a child, a terminated await target, a
  child that continues as new, and an await across a continue-as-new;
- the terminal, cancel, fallback cancel and final panic cap checks with and
  without cascade events, and an update and
  a query on a child that read its parent;
- a parent that passes its `execution_timeout` and cascades to its detached
  child, a child that times out by its declared timeout, a detached child
  that retries, an awaited child that does not, and a cancelled mutex holder
  that releases the lock.

Unit tests cover the schema step, a race loser with no row, and the panic
strikes of runs that another run seals: by a cancel, a terminate cascade, an
unsupported seal or an error-path cancel. A unit test checks that a hard cancel removes a task
that a dropped drive left `RUNNING`. The core test
`match_external_await_resolves_past_a_sibling_child` pins the replay fix. The
design record is `DESIGN-2026.md`.
