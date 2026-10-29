# DESIGN-2391: operational parameters that need a runtime override

Part of epic #2384. This record lists every field of `WorkflowInfo`, `DagInfo`,
`ActivityInfo` and `WorkflowSchedule`. Each row says where the field is
declared, whether an operator can change it at runtime today, what a change
costs a run that is in flight, and what to do next.

The guard `runtime_parameter_inventory_docs` reads these four structs from the
source. It fails when a field has no row here, and when a row names a field
that no longer exists. So a new field needs a row in the same PR.

## Recommendations

- **override**: an operator needs to change it during an incident, and the
  change is safe for runs in flight. Each one has a child issue of #2384.
- **already runtime**: an admin route already changes it, or its effect,
  without a restart. The row names the route.
- **deploy-only**: identity, code, schema or metadata. A runtime change would
  make a run mean something different, or gains nothing during an incident.

## How a change reaches a run in flight

A run reads most of these values once, at a fixed point, and stores the
result. So a change applies to the next read only:

- An execution stores `deadline_at`, `sla` and `sla_deadline_at` at start.
- A task row stores `retry_policy`, `max_attempts`, `heartbeat_timeout`,
  `schedule_to_start`, `schedule_to_close_at` and `concurrency_cap` when the
  activity is scheduled. A retry reuses the stored row.
- A local activity freezes its retry policy and start-to-close at the first
  schedule, in `LocalActivityScheduled` (issue #620).
- `ActivityScheduled` stores no timeout and no retry policy, so replay never
  compares them. A change to an activity default does not break replay.

A change to a value that a start reads, such as a workflow concurrency limit,
applies to the next start.

## `WorkflowInfo`

Declared with `#[workflow(...)]` and registered with `WorkerBuilder`.

| Field | Runtime today | Cost of a change mid-run | Recommendation |
| --- | --- | --- | --- |
| `WorkflowInfo.name` | no | The type identity. History, schedules and routing key on it. | deploy-only |
| `WorkflowInfo.module` | no | Grouping metadata. | deploy-only |
| `WorkflowInfo.handler` | no | Code. | deploy-only |
| `WorkflowInfo.execution_timeout` | yes, `PATCH /admin/workflow-types/{workflow_name}/run-defaults` (#2387) | New runs only. `deadline_at` of a run in flight never moves. | already runtime |
| `WorkflowInfo.chain_execution_timeout` | no | Read when a chain starts, and stored as its chain deadline. Each continue-as-new copies that deadline, so a chain in flight keeps it. | override (#2406) |
| `WorkflowInfo.sla` | yes, run-defaults route (#2387) | New runs. The opt-in recompute moves the SLA deadline of unbreached runs. | already runtime |
| `WorkflowInfo.concurrency` | no. `GET /admin/concurrency` reads it. | `limit` is read at each start. A lower limit queues or rejects new starts, and never stops a run. `key_expr` and `on_conflict` change what a start means. | override (#2407), `limit` only |
| `WorkflowInfo.debounce` | no. `GET /admin/debounce` reads pending bursts. | A burst in flight holds its window. A new window splits or merges bursts in a way that no caller can predict. | deploy-only |
| `WorkflowInfo.batch` | no | A batch in flight holds its size and wait. A change mid-batch has the same problem as debounce. | deploy-only |
| `WorkflowInfo.throttle` | yes, TTL'd pacing override, `POST /admin/start-throttle/{workflow_name}/override` (#945) | New starts only. | already runtime |
| `WorkflowInfo.quota` | no. `GET /admin/quotas` reads usage. | The limits are read at each start and each admission. A lower limit refuses new work for a tenant, and never ends a run. | override (#2408), limits only |
| `WorkflowInfo.max_input_bytes` | no | A schema-level bound that callers code against. | deploy-only |
| `WorkflowInfo.owner` | no | Metadata for alerts. | deploy-only |
| `WorkflowInfo.runbook_url` | no | Metadata for alerts. | deploy-only |
| `WorkflowInfo.severity` | no | Metadata for alerts. | deploy-only |
| `WorkflowInfo.description` | no | Metadata. | deploy-only |
| `WorkflowInfo.input_schema` | no | Schema, generated from code. | deploy-only |
| `WorkflowInfo.output_schema` | no | Schema, generated from code. | deploy-only |
| `WorkflowInfo.error_schema` | no | Schema, generated from code. | deploy-only |
| `WorkflowInfo.retry_policy` | no | Read when a run fails, to start the next attempt. A change applies to the next failure of any run. | override (#2409) |
| `WorkflowInfo.mcp` | no | Exposure of the type as a tool. A security boundary, so it changes with code review. | deploy-only |
| `WorkflowInfo.declared_activities` | no | Static reachability metadata. | deploy-only |
| `WorkflowInfo.declared_children` | no | Static reachability metadata. | deploy-only |

## `DagInfo`

Declared with `#[dag(...)]`. A unified DAG lowers to a workflow schedule.

| Field | Runtime today | Cost of a change mid-run | Recommendation |
| --- | --- | --- | --- |
| `DagInfo.name` | no | The DAG identity. | deploy-only |
| `DagInfo.module` | no | Grouping metadata. | deploy-only |
| `DagInfo.schedule` | yes, `PATCH /dags/{dag_name}` (#2385) | Re-anchors `next_run_at`. A run in flight is not affected. | already runtime |
| `DagInfo.catchup` | yes, `PATCH /dags/{dag_name}` (#2385) | Next tick. | already runtime |
| `DagInfo.max_active_runs` | yes, `PATCH /dags/{dag_name}` (#2385) | Next fire. A run in flight is not affected. | already runtime |
| `DagInfo.default_queue` | no | Routing of the task graph. A queue with no worker strands new tasks. | deploy-only |
| `DagInfo.builder` | no | Code: the task graph. | deploy-only |
| `DagInfo.workflow_handler` | no | Code. | deploy-only |
| `DagInfo.jitter` | yes, `PATCH /dags/{dag_name}` (#2385) | Next fire. | already runtime |
| `DagInfo.overlap_policy` | yes, `PATCH /dags/{dag_name}` (#2385) | Next fire. | already runtime |
| `DagInfo.buffer_all_max` | yes, `PATCH /dags/{dag_name}` (#2385) | Next fire. | already runtime |
| `DagInfo.owner` | no | Metadata for alerts. | deploy-only |
| `DagInfo.runbook_url` | no | Metadata for alerts. | deploy-only |
| `DagInfo.severity` | no | Metadata for alerts. | deploy-only |
| `DagInfo.mcp` | no | Exposure as a tool. A security boundary. | deploy-only |
| `DagInfo.execution_timeout` | yes, run-defaults route under the DAG name (#2387) | New runs only. | already runtime |
| `DagInfo.sla` | yes, run-defaults route under the DAG name (#2387) | New runs, and the opt-in recompute. | already runtime |

## `ActivityInfo`

Declared with `#[activity(...)]`.

| Field | Runtime today | Cost of a change mid-run | Recommendation |
| --- | --- | --- | --- |
| `ActivityInfo.name` | no | The type identity. History keys on it. | deploy-only |
| `ActivityInfo.module` | no | Grouping metadata. | deploy-only |
| `ActivityInfo.default_retry_policy` | no | Frozen on the task row at schedule. A change applies to activities scheduled after it. Replay is not affected. | override (#2410) |
| `ActivityInfo.default_start_to_close` | no | Frozen at schedule, as above. A slow downstream needs a longer value during an incident. | override (#2410) |
| `ActivityInfo.default_heartbeat_timeout` | no | Frozen at schedule, as above. | override (#2410) |
| `ActivityInfo.default_schedule_to_start` | no | Frozen at schedule, as above. | override (#2410) |
| `ActivityInfo.default_queue` | no | Routing. A queue with no worker strands new tasks. The queue pause covers the incident case. | deploy-only |
| `ActivityInfo.max_concurrent` | no. An adaptive limit (#1836) is a separate cap per worker, which moves on its own. No route sets this fleet-wide cap. | Stored on each task row as `concurrency_cap` at enqueue, and enforced at claim. A change applies to tasks enqueued after it. Pending tasks keep the old cap. | override (#2411) |
| `ActivityInfo.concurrency_key` | no | Changes which tasks share a cap. A change mid-run splits one bucket into several. | deploy-only |
| `ActivityInfo.default_schedule_to_close` | no | Frozen at schedule as `schedule_to_close_at`. | override (#2410) |
| `ActivityInfo.is_local` | no | Changes the execution model and the history events. | deploy-only |
| `ActivityInfo.max_input_bytes` | no | A schema-level bound. | deploy-only |
| `ActivityInfo.max_result_bytes` | no | A schema-level bound. | deploy-only |
| `ActivityInfo.rate_limit_rps` | yes, TTL'd override, `POST /admin/rate-limits/{activity_name}/override` (#945) | Next token. | already runtime |
| `ActivityInfo.rate_limit_burst` | yes, the same override (#945) | Next token. | already runtime |
| `ActivityInfo.rate_limit_key` | no | Changes which calls share a bucket. | deploy-only |
| `ActivityInfo.rate_limit_key_expr` | no | Changes which calls share a bucket. | deploy-only |
| `ActivityInfo.circuit_breaker` | yes, its state: `POST /admin/circuits/{activity_name}/force-open` and `force-close` (#369). The thresholds are fixed. | Next dispatch. | already runtime |
| `ActivityInfo.requires` | no | Worker capability routing. | deploy-only |
| `ActivityInfo.handler` | no | Code. | deploy-only |
| `ActivityInfo.input_schema` | no | Schema, generated from code. | deploy-only |
| `ActivityInfo.output_schema` | no | Schema, generated from code. | deploy-only |

The activity pause (`POST /activities/{activity_name}/pause`, #807) and the
queue pause (`POST /admin/queues/{queue_name}/pause`, #619) are not fields,
but they are the first tool for an activity incident.

## `WorkflowSchedule`

Declared with `WorkerBuilder::workflow_schedule`, or lowered from a unified
DAG. The #771 row edit changes a schedule made with `POST /schedules`. The
#2386 override changes a code-declared one.

| Field | Runtime today | Cost of a change mid-run | Recommendation |
| --- | --- | --- | --- |
| `WorkflowSchedule.workflow_name` | no | The schedule identity. | deploy-only |
| `WorkflowSchedule.dag_name` | no | The schedule identity. | deploy-only |
| `WorkflowSchedule.schedule` | yes, `PATCH /admin/schedules/{id}` (#771, #2386) | Re-anchors `next_run_at`. | already runtime |
| `WorkflowSchedule.input` | no for a code-declared schedule. The #2386 override refuses it, because the input is a payload under the codecs. | The next fire only. | override (#2412) |
| `WorkflowSchedule.catchup` | yes (#771, #2386) | Next tick. | already runtime |
| `WorkflowSchedule.max_active_runs` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.paused` | yes, `POST /admin/schedules/{id}/pause` and `resume` | Next tick. | already runtime |
| `WorkflowSchedule.queue_name` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.jitter` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.overlap_policy` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.buffer_all_max` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.execution_timeout` | yes, in effect. A fire takes the timeout of the workflow type, which the run-defaults route overrides (#2387). | New runs only. | already runtime |
| `WorkflowSchedule.chain_execution_timeout` | not read. A fire takes the chain timeout of the workflow type. | None: the field has no effect. | deploy-only |
| `WorkflowSchedule.calendar` | yes, the field (#771, #2386) and the calendar itself (`PUT /calendars/{name}`) | Next fire. | already runtime |
| `WorkflowSchedule.skip_policy` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.consecutive_failure_limit` | yes (#771, #2386) | Next failure. | already runtime |
| `WorkflowSchedule.end_at` | yes (#771, #2386) | Next tick. | already runtime |
| `WorkflowSchedule.max_runs` | yes (#771, #2386) | Next fire. | already runtime |
| `WorkflowSchedule.catchup_policy` | yes (#771, #2386) | Next recovery tick. | already runtime |
| `WorkflowSchedule.retry_policy` | yes (#771, #2386) | Next run that the schedule starts. | already runtime |
| `WorkflowSchedule.all_writable_shards` | no | Changes on how many shards the schedule has a row. | deploy-only |

## Child issues

| Issue | Fields |
| --- | --- |
| #2406 | `WorkflowInfo.chain_execution_timeout` |
| #2407 | `WorkflowInfo.concurrency` (`limit`) |
| #2408 | `WorkflowInfo.quota` (limits) |
| #2409 | `WorkflowInfo.retry_policy` |
| #2410 | `ActivityInfo.default_retry_policy` and the four activity timeouts |
| #2411 | `ActivityInfo.max_concurrent` |
| #2412 | `WorkflowSchedule.input` |
