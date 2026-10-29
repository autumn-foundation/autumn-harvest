## Engine — Runtime SLA and execution-timeout overrides per workflow type (issue #2387, epic #2384)

`WorkflowInfo::sla`, `WorkflowInfo::execution_timeout` and their `DagInfo`
counterparts were static, so a change needed a restart.

The new `PATCH /admin/workflow-types/{workflow_name}/run-defaults` sets an
operator override of `sla_secs` and `execution_timeout_secs` for one type. A
DAG uses its own name. The override wins over code for every new run and
survives a restart. `GET` on the same route shows the code value, the
override and the effective value. Each field is tri-state, and
`clear_override` returns the type to its code values.

Every start path reads the override from an in-process cache, so a start
sends no extra query. The cache reloads once it is older than 5 seconds. The
start clamps still hold: an SLA above the effective timeout is clamped to it.

By default a change applies to new runs only. `recompute_in_flight: true` also
moves the SLA deadline of each non-terminal run of the type that has not
breached, on every shard. The hard `deadline_at` never moves.

See [`docs/operations/run-default-overrides.md`](../operations/run-default-overrides.md).

**Migration.** `20261029050916_harvest_run_default_overrides` adds one small
table on the default shard. **Grants:** `SELECT`, `INSERT`, `UPDATE`, `DELETE`
on `harvest_run_default_overrides`; `GET /admin/preflight` checks them. No
`WorkflowEvent` variant, no replay impact.

**Tests.** `run_defaults_integration` (a new start takes the override, the
opt-in recompute moves only in-flight runs and the breach scanner fires on
the new deadline, clear restores code, a timeout override clamps the SLA, bad
requests are refused and audited) and `run_defaults_db_tests` (a warm cache
sends no query, another process sees a change within one TTL).
