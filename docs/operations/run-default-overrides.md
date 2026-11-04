# Runtime SLA, execution-timeout and chain-cap overrides

A workflow type declares three durations in code: its soft `sla`, its hard
`execution_timeout` and its chain cap `chain_execution_timeout`. It declares
them through
`#[workflow(sla = ..., execution_timeout = ..., chain_execution_timeout = ...)]`.
A DAG declares `sla` and `execution_timeout` through `#[dag(...)]` or
`DagInfo`, and its runs take them through its shadow workflow type of the
same name. A DAG declares no chain cap. An operator override changes these
values at runtime, with no restart and no deploy (issues #2387 and #2406,
epic #2384).

## Quick reference

```bash
# Tighten the SLA of new nightly_etl runs to five minutes.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/nightly_etl/run-defaults \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"sla_secs": 300, "reason": "INC-4821"}'

# Also move the SLA deadline of the runs that are in flight now.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/nightly_etl/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"sla_secs": 300, "recompute_in_flight": true}'

# Give new continue-as-new chains of a poller a 7-day cap.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/inbox_poller/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"chain_execution_timeout_secs": 604800, "reason": "INC-4822"}'

# Return the SLA to its code value, and keep any timeout override.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/nightly_etl/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"sla_secs": null}'

# Return every field to its code value.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/nightly_etl/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"clear_override": true}'

# Read the code value, the override and the effective value.
curl https://<host>/api/harvest/admin/workflow-types/nightly_etl/run-defaults
```

Both routes need the admin role. Each `PATCH` writes one
`workflow_type.run_defaults` audit row, a refused request included.

## Fields

| Field | Meaning |
|-------|---------|
| `sla_secs` | The soft SLA in whole seconds, from 1 to 3153600000 (100 years). A breach is reported. It never ends a run. |
| `execution_timeout_secs` | The hard execution timeout in whole seconds, from 1 to 3153600000 (100 years). A run past it is ended. |
| `chain_execution_timeout_secs` | The cap of a new continue-as-new chain in whole seconds, from 1 to 3153600000 (100 years). A chain past it is ended. Refused for a DAG. |
| `clear_override` | `true` deletes the stored override before the other fields merge. |
| `reason` | Free text stored with the override. |
| `recompute_in_flight` | `true` moves the SLA deadline of the runs in flight. Default `false`. |

Each duration is tri-state:

- An absent key keeps the stored override.
- `null` returns the field to its code value.
- A number sets the override.

A merge that sets no field deletes the override.

## Precedence and clamps

The precedence is fixed: override, then code. A field that the override does
not set inherits the code value.

The start clamps still apply after the override:

- An SLA above the effective execution timeout is clamped to that timeout.
  So an override of `execution_timeout_secs: 600` on a type declared
  `sla = 3h` gives new runs a 10-minute SLA.
- The fleet ceiling `max_workflow_execution_timeout` still caps the timeout.
- The fleet chain ceiling `max_workflow_chain_timeout` still caps the chain
  cap. A root run of a type with no chain cap takes the ceiling as its
  default, as before. A child run of such a type stays uncapped.
- A completion-trigger target start and a typed `update_with_start` run take
  the same three values and the same ceilings. A deferred completion-trigger
  start resolves them when it fires. The immediate relay of a cross-shard
  start keeps the SLA of evaluation time.
- A per-start `sla_secs` or `execution_timeout_secs` in the start request
  still wins over both.

`effective` in the response is the override, then code, before these clamps.

## When a change takes effect

The overrides live in `harvest_run_default_overrides` on the default shard.
Each process holds a copy in memory, and a start never queries the table.

- **The process that served the PATCH** applies the change to its next start.
- **Every other process** reloads its copy once it is older than 5 seconds
  (`RUN_DEFAULTS_TTL`). The scheduler tick, each worker and the API gate
  refresh loop all run the reload. So a change reaches the whole fleet within
  about 6 seconds.
- A failed reload keeps the last copy and logs a warning. A process that
  never loaded the table uses the code values. An SLA only reports a breach,
  so this fails open, unlike an admission gate.

## Runs already in flight

A change applies to new runs only, by default. A continue-as-new successor of
the same type keeps the values of its run. A successor of a different type is
a new run of that type, so it takes the override of `sla` and
`execution_timeout`.

A chain cap applies to the next chain that starts. A chain starts at a root
run or a child run. Each continue-as-new successor copies the stored
`chain_deadline_at` of its chain, also a successor of a different type. So a
chain in flight keeps its chain deadline, and no option moves it. A shorter
deadline could end a chain at once. To end a chain early, cancel or terminate
its current run.

`recompute_in_flight: true` moves the SLA deadline only. It moves it on every
shard, for each RUNNING, SUSPENDED or PAUSED run of the type that has not
breached yet:

- The run keeps its anchor. The new deadline is the old deadline minus the
  old SLA, plus the new one. A run with no SLA so far anchors at its start.
- The new SLA is clamped to the run's own execution timeout.
- A breached run does not move, so a breach is never reported twice.
- `deadline_at` never moves. A shorter hard deadline could end a run at once.
  To end a run early, cancel or terminate it.

The response then carries `recompute.moved_runs` and
`recompute.unavailable_shards`. A shard that the recompute could not reach
keeps the old SLA. Send the same request again to retry it.

The recompute updates rows in batches of 500, so it holds a bounded number of
row locks at a time.

Before each batch the recompute reads the stored override again. If a newer
change has replaced its SLA, it stops and reports `recompute.superseded:
true`. So two recomputes of one type never undo each other: the newer change
decides the SLA of the runs in flight. To move them, set
`recompute_in_flight: true` on that newer change.

## Grants

The runtime role needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on
`harvest_run_default_overrides`. `GET /admin/preflight` reports a missing
grant.

## See also

- [`schedule-overrides.md`](schedule-overrides.md): the same idea for
  schedules.
