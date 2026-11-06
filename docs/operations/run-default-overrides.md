# Runtime SLA, execution-timeout, chain-cap, concurrency-limit, quota and retry overrides

A workflow type declares three durations in code: its soft `sla`, its hard
`execution_timeout` and its chain cap `chain_execution_timeout`. It declares
them through
`#[workflow(sla = ..., execution_timeout = ..., chain_execution_timeout = ...)]`.
A DAG declares `sla` and `execution_timeout` through `#[dag(...)]` or
`DagInfo`, and its runs take them through its shadow workflow type of the
same name. A DAG declares no chain cap. An operator override changes these
values at runtime, with no restart and no deploy (issues #2387 and #2406,
epic #2384).

A workflow type can also declare a concurrency policy through
`#[workflow(concurrency(key = ..., limit = ...))]`. An operator override
changes its `limit` at runtime (issue #2407). The key and `on_conflict` stay
as declared.

A workflow type can also declare a per-tenant quota through
`#[workflow(quota(key = ..., max_active_executions = ..., ...))]`. An operator
override changes its three admission caps at runtime (issue #2408). The key
and the LLM caps stay as declared.

A workflow type can also declare a workflow retry policy in
`WorkflowInfo::retry_policy`, for example through
`WorkflowInfo::with_retry_policy`. An operator override changes three of
its fields at runtime: `max_attempts`, `initial_interval` and
`max_interval` (issue #2409). The backoff coefficient, the jitter and
`non_retryable_errors` stay as declared.

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

# Shed load from a downstream: let at most 2 runs of a key run at once.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/tenant_sync/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"concurrency_limit": 2, "reason": "INC-4823"}'

# Return the concurrency limit to its code value.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/tenant_sync/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"concurrency_limit": null}'

# A tenant floods the fleet: refuse new runs of a key past 10 active runs.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/tenant_import/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"quota_max_active_executions": 10, "reason": "INC-4824"}'

# Return that cap to its code value, and keep the other quota caps.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/tenant_import/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"quota_max_active_executions": null}'

# Stop the workflow retries of a type, for runs in flight too.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/billing_sync/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"retry_max_attempts": 1, "reason": "INC-4825"}'

# Slow them instead: start the backoff at 10 minutes. The declared jitter
# still applies.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/billing_sync/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"retry_initial_interval_ms": 600000}'

# Return the retry fields to the code value.
curl -X PATCH https://<host>/api/harvest/admin/workflow-types/billing_sync/run-defaults \
     -H 'Content-Type: application/json' \
     -d '{"retry_max_attempts": null, "retry_initial_interval_ms": null}'

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
| `concurrency_limit` | The concurrency limit of a new start, from 1 to 2147483647. Refused for a type with no concurrency policy, a DAG included, and for a latest-wins type. |
| `quota_max_active_executions` | The quota cap on the active runs of one key, from 1 to 4294967295. |
| `quota_max_history_bytes` | The quota cap on the history bytes of the active runs of one key, from 1 to 9223372036854775807. |
| `quota_max_dead_letters` | The quota cap on the dead letters of one key, from 1 to 4294967295. |
| `retry_max_attempts` | The workflow retry `max_attempts`, the first attempt included, from 1 to 2147483647 and at most the fleet ceiling. `1` stops the retries. |
| `retry_initial_interval_ms` | The base delay before the first retry, in whole milliseconds, from 1 to 31536000000 (365 days). |
| `retry_max_interval_ms` | The upper bound on each retry delay, in whole milliseconds, from 1 to 31536000000 (365 days). When both interval fields are set, at least `retry_initial_interval_ms`. |
| `clear_override` | `true` deletes the stored override before the other fields merge. |
| `reason` | Free text stored with the override. |
| `recompute_in_flight` | `true` moves the SLA deadline of the runs in flight. Default `false`. |

The route refuses a value in a `quota_max_*` field for a type with no quota
admission cap, a DAG included. Such a type stamps and locks no key for a cap.
A `null` sets no cap, so the route accepts it on any type.

The route refuses a value in a `retry_*` field for a type with no code
`retry_policy`, a DAG included. A run of such a type stores no policy, so the
field has nothing to apply to. The route also refuses a `retry_max_attempts`
above the fleet ceiling `max_workflow_attempts`.

Each duration, `concurrency_limit`, each `quota_max_*` field and each
`retry_*` field is tri-state:

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
- A batch start takes the timeout and the SLA, on the immediate path and on
  the throttled path. A throttled item keeps the values of admission time
  when the scanner fires it. A webhook-delivery start takes them too.
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

## Concurrency limit

A start stores its concurrency limit on its workflow task row. The claim
admits a row only while the count of running rows of its key is below that
stored cap. A run in flight keeps one task row for its life. So:

- **A lower limit** applies to the next start. A run in flight keeps its task
  and its cap, and a lower limit cancels no run. The new starts wait until
  the running count of their key drops below the new limit. A start that
  waits from before the change keeps its higher cap. A continue-as-new
  successor or a retry of a run in flight also keeps its higher cap.
- **A higher limit** applies to the next start. The `PATCH` also raises the
  stored cap of each live task row of the type, on every shard: a waiting
  start, and the row of a run in flight. So the next claim admits the
  waiting starts. It never lowers a stored cap.
- **A latest-wins type** (`on_conflict = cancel_running`) cancels the oldest
  runs of a key to keep its limit. Any later drop of an override, a clear
  included, would cancel runs in flight. So the route refuses a limit for
  such a type, and a start of such a type ignores a stored one.
- **`{"concurrency_limit": null}`** clears only the limit and restores the
  code limit. `clear_override` also clears the other fields. Either `PATCH`
  raises the live rows to the code limit, if it is higher.

A deferred start takes the limit that holds when it fires: a debounce, a
throttle, an event batch, a completion trigger and a cross-shard child.

A `PATCH` that names `concurrency_limit` or `clear_override`, for a type with
a policy, adds a `concurrency_raise` report:

| Field | Meaning |
|-------|---------|
| `raised_tasks` | The number of task rows whose cap went up. |
| `unavailable_shards` | The shards that the raise could not reach. Their rows keep the old cap. Send the same request again to retry them. |
| `superseded` | `true` when a newer change to the limit stopped this raise. That change then decides the cap. |

The raise updates 500 rows per transaction. It skips a row that another
transaction holds, for example a row that a worker claims at that moment.
Another process stores the old limit on a start until its cache reloads,
within about 6 seconds. So the route raises the rows again 10 seconds, 1
minute and 5 minutes later. A later raise also lifts a row that an earlier
one skipped, or a start that a caller transaction held uncommitted. Each
raise runs in its own task, so a client that disconnects stops none of them.
A shard that a later raise cannot reach goes to the log as a warning. A start
that stays uncommitted past the last raise keeps the old cap. Send the same
request again to raise it.

On a shard other than the default one, each batch of a raise holds the type
lock of the default shard until it commits. A change of the override takes
the same lock. So a newer, lower limit never meets a stale raise.

`GET /admin/concurrency` reports `code_limit`, `override_limit` and
`effective_limit` for each workflow type with a concurrency policy in a
`workflow` group. It reads the stored override, not a cache.
`max_concurrent` is the largest cap that a live row of the key stores. So it
can differ from `effective_limit` until the older rows finish.

## Quota limits

Each admission reads the quota caps again. No row stores a cap. So:

- **A lower cap** refuses the next admission of a key over it, with the `429`
  of issue #946. Admission is the only place where a cap applies. A run in
  flight continues, and no run ends. A workflow retry is still exempt. A
  continue-as-new successor has no quota check. A child spawn that the cap
  refuses defers its parent with a backoff, as before. The parent waits, and
  its own `execution_timeout` still applies. So a lower cap that stays in
  place for a long time can time out a parent that waits for a child.
- **A higher cap** admits the next admission under it. No pass is needed.
- **A cap that code leaves unset** can be set, on a type with at least one
  admission cap. `null` returns it to uncapped.
- **`clear_override`** restores every code cap.

Each start, child spawn, fork, DLQ redrive and cross-shard child takes the
override, because each one calls `enforce_quota_admission`. A cross-shard
child takes the override of the process on its target shard. Quota stays
shard-local, as in `docs/sharding.md`.

`GET /admin/quotas` shows the source of each cap:

| Field | Meaning |
|-------|---------|
| `limits` | The effective caps: the override, then code. |
| `code_limits` | The caps declared in code. |
| `override` | The stored caps with `reason`, `updated_by` and `updated_at`, or `null`. |

Each usage entry carries the three fields. `override.applies` is `false` when
the type has no admission cap, for example after a deploy removed the caps.
`workflow_types` lists the fields for each registered type with an admission
cap, so an override shows before its type has usage.

The route reads the stored override from the default shard, and then reports
`override_source: "table"`. If that read fails, it shows the cache of the
serving process, with no reason, actor or time, and reports
`override_source: "cache"`.

Admission reads the overrides through the cache of the last
`HandlerRegistry` that the process built. Each worker and API node refreshes
it. A process that starts runs with a bare `HarvestClient` and no registry
admits under the code caps. A process that never loaded the table, for
example because the default shard was down at boot, also admits under the
code caps until a refresh succeeds. Watch for the refresh warning in the
log.

## Retry policy

Each start stores the retry policy of its run on the execution row. The
failure path applies the override over that stored policy when
the run fails. So:

- **A change applies to the next failure of any run of the type**, in flight
  or new. `retry_max_attempts: 1` stops the retries of the runs in flight at
  their next failure. A run whose attempt count already reaches the new value
  does not retry.
- **An attempt that already started is not affected.** A running attempt is
  never cancelled. A retry that a failure already scheduled keeps its row,
  its fire time and its stored policy. Its own failure takes the override.
- **The stored policy never changes.** A retry successor stores the policy of
  its run, not the effective one. So `clear_override`, or a `null`, restores
  the code value at the next failure.
- **The effective `max_interval` is never below the effective
  `initial_interval`** when the override sets an interval. `max_interval`
  caps each delay, so a lower cap would hide the first delay. So a longer
  `retry_initial_interval_ms` alone also raises the cap, and a
  `retry_max_interval_ms` alone below the first delay rises to it.
- **The declared jitter and backoff still apply.** The interval is the base
  of each delay. With the default full jitter, a delay can be shorter than
  the base.
- **365 days is the longest interval.** A retry starts with a delay, and the
  start refuses a delay above 365 days. A longer delay would end the chain.
- **The fleet ceiling** `max_workflow_attempts` still caps `max_attempts`.
  The failure path also clamps an override value to the ceiling of its own
  process.

A schedule's own `retry_policy` wins for the runs that the schedule starts.
The override does not apply to them. Each schedule path marks such
a policy with the key `_harvest_retry_source: "schedule"` in the stored JSON.
A retry, a same-type continue-as-new and a re-run of such a run keep the mark.
A schedule run that inherits the code policy has no mark, so the override
applies to it.

The failure path reads the overrides through the cache of the last
`HandlerRegistry` that the process built. Each worker refreshes it. The
decision runs outside workflow code, so the override does not change replay.
The `WorkflowRetryScheduled` event keeps its shape.

## Mixed versions

Set a limit only after every process runs this release. An older process
refuses `concurrency_limit` with a 400, and it starts runs with the code
limit. Do not send a `PATCH` to an older process once a limit is stored. If
it clears the last other field of the row, it deletes the row, the limit
included.

The quota caps follow the same rule. An older process refuses each
`quota_max_*` field with a 400, and it admits under the code caps. An older
process reads a row with quota caps only as an override that sets nothing.
An older process that serves `clear_override` with another field writes only
its own columns, so the stored quota caps stay in force. Its response does
not show them.

The retry fields follow the same rule. An older process refuses each
`retry_*` field with a 400. An older worker ignores the override, and it
decides each failure with the stored policy. An older process that schedules
a retry of a schedule run drops the schedule mark, so the override then
applies to that chain. A run that an older process started from a schedule
policy has no mark either. If an older process clears the last other field
of a row, it deletes the row, the retry fields included.

To roll back, deploy the older release first. Then run the down migration of
`20261105062317_harvest_run_default_retry_policy`, then that of
`20261105015009_harvest_run_default_quota_limits`. This release reads the
quota and retry columns, so it fails each refresh once they are gone. The
retry down migration deletes each override row that sets only retry fields.

## Grants

The runtime role needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on
`harvest_run_default_overrides`. The concurrency raise also needs `UPDATE` on
`harvest_task_queue`, which the runtime role already has. `GET /admin/preflight` reports a missing
grant.

## See also

- [`schedule-overrides.md`](schedule-overrides.md): the same idea for
  schedules.
