## Feature — override a workflow type's chain cap at runtime (issue #2406)

`WorkflowInfo::chain_execution_timeout` (#617) caps a whole continue-as-new
chain. It was fixed at startup. `PATCH
/admin/workflow-types/{workflow_name}/run-defaults` now takes
`chain_execution_timeout_secs`, with the tri-state, the bound and the cache of
the #2387 fields. `GET` shows it in `code`, `override` and `effective`.

**Semantics.** The override applies to the next chain that starts. A
continue-as-new successor copies the stored `chain_deadline_at`, also across a
type change. So a chain in flight keeps its deadline, and
`recompute_in_flight` never moves it. The fleet ceiling
`max_workflow_chain_timeout` still caps the override. Clearing the override
restores the code value. A DAG declares no chain cap, so the route refuses
one for a DAG.

**One resolver.** `HandlerRegistry::chain_execution_timeout` applies the
override. Every start path of a new chain calls it, or
`apply_chain_run_default`. The guard `run_defaults_chain_guard` fails on a
start path that reads the declared field itself.

**Gaps closed.** The outbox start, the Vantage trigger-now of a workflow
schedule and the typed `signal_with_start` stub set no chain cap before. They
now take the chain cap of the type, the override and the fleet chain ceiling.
A child spawn now caps its chain cap with the fleet ceiling. The typed `start`
and `signal_with_start` stubs of `#[workflow]` now apply the run-default
overrides of a client that shares the cache (#2387). A completion-trigger
start still sets no chain cap. Issue #2511 tracks it.

**Migration.** `20261102141324_harvest_run_default_chain_timeout` adds the
nullable column `chain_execution_timeout_secs` to
`harvest_run_default_overrides`, and widens the row check to three columns.
No `WorkflowEvent` variant and no replay impact. Design:
`docs/design/DESIGN-2406.md`.

**Tests.** Unit tests on the fields, the cache and the resolver. DB tests on
a chain-only row, the bound and the table check. HTTP tests for each
acceptance criterion and for the DAG refusal. Worker tests for a same-type
and a cross-type successor. Outbox and Vantage tests for the closed gaps.
