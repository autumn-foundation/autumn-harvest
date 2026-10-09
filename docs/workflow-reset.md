# Workflow reset

A reset forks a run at an event boundary. The fork gets a copy of the source
history up to that boundary, and the engine seals the source `TERMINATED`.
`autumn-harvest/src/reset.rs` holds the engine. This page lists the fork
surfaces and the rules that apply to all of them.

## Fork surfaces

| Surface | Source states | Notes |
|---|---|---|
| `POST /workflows/{id}/reset` (issue #148) | `RUNNING`, `PAUSED` | One run. `?dry_run=true` previews. |
| Vantage "Reset to event N" | `RUNNING`, `PAUSED` | Calls the same engine. See [Vantage UI](vantage-ui.md). |
| `POST /workflows/batch_reset` (issue #538) | `RUNNING`, `PAUSED`, `FAILED`, `CANCELLED`, `TIMED_OUT` | A cohort. Each run gets an outcome. `preview: true` forks nothing. |
| `POST /dags/{dag_name}/runs/{run_exec_id}/retry` (issue #366) | `FAILED`, `CANCELLED`, `TIMED_OUT` | See [DAG retry](runbooks/dag-retry-from-failed-node.md). |

No surface forks a `COMPLETED` or `TERMINATED` run, or a child run.

## A fork never uses a PII-erased source

Erasure (issue #495) replaces each payload field with the
`{"_harvest_erased": true}` tombstone. It applies to terminal runs only.

The engine refuses an erased source on every fork path (issue #1999). The
check runs under the `FOR UPDATE` row lock that the fork takes, before the
fork copies an event. An erasure either commits before that lock, and the fork
refuses, or it waits behind the lock, and the fork copies intact events. A dry
run returns the same refusal.

| Surface | Result for an erased source |
|---|---|
| Single reset | `409`. The state gate usually refuses first, because an erased run is terminal. |
| Batch reset | The item is `skipped` with `skip_reason: {"type": "erased_source"}`. The other runs in the cohort still reset. |
| DAG retry | `409` that tells you to start a fresh DAG run. |

There is no opt-out. To run the work again, start a fresh run with new input.
`POST /workflows/{id}/rerun` does this when you give it an explicit `input`.
It refuses an erased input otherwise. See [`api-contract.json`](api-contract.json).

### Decision record (issue #1999)

Before issue #1999, the refusal was an in-process opt-in,
`WorkflowResetRequest::refuse_erased_source`. Only DAG retry set it. Batch
reset left it off, so a batch could fork an erased `FAILED`, `CANCELLED` or
`TIMED_OUT` run. The recorded reason was scope: issue #780 did not want to
change a shipped endpoint. That reason does not hold for these causes:

- The fork input is the tombstone. Batch reset has no input override.
- The carried-over activity results are tombstones too. The fork resumes on
  data that no code can read.
- A fork makes an erased run live again. Erasure must be final.
- A batch reported the refusal as `infrastructure_error`. That reason tells
  the operator to retry, which never helps.

So the engine now refuses an erased source without a flag, and the flag is
gone. A new fork path, such as the non-destructive fork of issue #2000, gets
the refusal from the engine. If a new path does not call
`reset_workflow_execution`, it must call the same check under its own row lock
and add a row to the tables above.

## Batch skip reasons

A batch reset never drops a run. Each item has `outcome` set to `reset`,
`previewed` or `skipped`. A skipped item has a `skip_reason` with a `type`:

| `type` | Cause | Retry helps? |
|---|---|---|
| `terminal_source` | The run is `COMPLETED`, `TERMINATED` or not found. | No |
| `child_workflow` | The run is a child. Reset the root parent. | No |
| `erased_source` | The run's payloads were erased. | No |
| `empty_history` | The run has no `WorkflowStarted` event. | No |
| `continue_as_new` | The history holds `WorkflowContinuedAsNew`. | No |
| `no_matching_activity` | `first_activity_run` found no matching activity. | No |
| `invalid_boundary` | The resolved event is not a clean boundary. | Use the nearest valid id. |
| `infrastructure_error` | A database or engine failure stopped the run. | Yes |
