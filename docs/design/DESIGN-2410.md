# DESIGN-2410: Override an activity type's retry policy and timeouts at runtime

Issue #2410. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the run-default overrides of #2387
and on the workflow retry override of #2409.

## Problem

`ActivityInfo` declares five defaults: `default_retry_policy`,
`default_start_to_close`, `default_heartbeat_timeout`,
`default_schedule_to_start` and `default_schedule_to_close`. When a
downstream slows down, an operator needs a longer timeout or fewer retries
now. Only a deploy changes them today.

## Facts

- The worker resolves the five values in `build_activity_enqueue_plan`
  when it stores the task row. The order is call site, then activity
  default, then builder default (#620).
- The task row stores the result: `max_attempts`, `retry_policy`,
  `start_to_close`, `heartbeat_timeout`, `schedule_to_start` and
  `schedule_to_close_at`. A retry reuses the row. The failure path reads the
  policy from the row, never from the registry.
- `ActivityScheduled` stores no timeout and no policy. Replay never
  compares them.
- The typed context calls (`execute_activity_with_opts`, `race().activity`,
  `Session::execute_activity` and the typed fan-outs) send the code default
  as the call-site value of the command. So the worker cannot tell a code
  default from a real call-site option.
- The simulator in `testing/sim.rs` reads the retry policy of the command.
  It has no registry.
- A local activity resolves in `execute_local_activity_raw_resolved`. The
  worker freezes the result in `LocalActivityScheduled` with
  `resolved: true` (#620). A crash recovery reads the frozen values.
- The local runner clamps `start_to_close` to the worker cap. It rejects a
  code `schedule_to_close`. The macro rejects a heartbeat timeout, a
  `schedule_to_start` and a `schedule_to_close` on a local activity.
- Each worker and each API process runs a run-defaults refresh loop
  (#2387). The worker builds each decision context from its registry.

## Brainstorm

1. Add activity columns to `harvest_run_default_overrides`.
2. Add a new table keyed by the activity name.
3. Keep a second cache with its own refresh loops.
4. Hold the activity map in `RunDefaultsCache`, so each existing refresh
   loads it too.
5. Resolve the override in the worker only.
6. Resolve it in the context, which then sends the effective value.
7. Mark the source of each command value with a new command field.
8. Store a whole `RetryPolicy` as JSONB.
9. Store `max_attempts`, `initial_interval` and `max_interval`, as #2409.
10. Let the override add a heartbeat timeout or a retry policy that code
    does not declare.
11. A new route `PATCH /admin/activity-types/{activity_name}/defaults`.

Decision: 2, 4, 5 and 6 together, 9 and 11.

- Option 1 keys two kinds of type by one name. An activity and a workflow
  can share a name.
- Option 3 needs a new loop in each process kind. Option 4 reuses the four
  existing refresh paths. Each refresh loads both tables.
- Option 5 alone misses the typed calls, whose command carries the code
  value as a call-site option. Option 7 changes every command constructor
  and still leaves the simulator without a registry. Option 6 lets the
  context send the effective value. The worker also applies the override
  when the command carries no value, which covers the raw and DAG calls.
- Option 8 breaks `Copy` and `Eq`. Option 9 covers both needs: stop
  (`max_attempts = 1`) and slow (a longer interval).
- Option 10 is refused. A heartbeat timeout kills each attempt of an
  activity that does not heartbeat. A retry policy retries an activity
  whose author never declared it safe to retry.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A typed call sends the code default as a call-site value, so the override never applies. | The context resolves call site, then override, then code. A context test checks the command of each typed path. |
| The override changes an activity that is already scheduled. | The override is read only at schedule time. The row keeps its values. A DB test checks the first row after the change. |
| The override changes replay. | `ActivityScheduled` stores no value. A DB test replays a run that spans the change. |
| A recovered local activity takes the new value. | The recovery path reads the frozen event values. A context test sets an override and checks the frozen values. |
| A local activity holds the workflow task too long. | The local runner clamps `start_to_close` to the worker cap. The route refuses fields that a local activity does not support. |
| A heartbeat timeout on an activity that does not heartbeat. | The route refuses `heartbeat_timeout_ms` when code declares none. The resolver ignores it too. |
| A retry override with no code policy. | The route refuses it. The resolver ignores it. |
| A call-site option loses to the override. | The call site wins in each path. A unit test checks the order. |
| The builder default `start_to_close` cuts a long attempt short after the override adds a `schedule_to_close`. | The worker checks the attempt bound on the effective values. |
| A zero or huge value. | The write, the decode and the table `CHECK` refuse zero. A timeout stops at 100 years and an interval at 365 days. |
| A row that the engine cannot read fails the load of every override. | The `CHECK` constraints match the decode bounds. |
| A reserved engine activity takes an override, so the liveness canary reports a false failure. | The route refuses the session and canary activities. The worker skips them. |
| A change is not audited. | Each `PATCH` writes one audit row, refused or not. |
| The override list omits activity overrides. | `GET /admin/overrides` lists them. |

## Six thinking hats

- **White (facts).** One enqueue site for remote activities. Five typed
  context paths. One local resolution site. The row freezes each value.
- **Red (feeling).** An operator in an incident wants one safe knob per
  activity. It must not surprise a run in flight.
- **Black (risks).** The typed calls that hide the code default. A
  heartbeat timeout on a silent activity. A local activity that holds the
  workflow task.
- **Yellow (benefits).** A new small table. No change to a hot table. No
  new event. No replay impact. One read from memory per schedule.
- **Green (ideas).** The `code`, `override` and `effective` views of the
  workflow route, for activities. Durations in milliseconds, so a 500 ms
  code value shows exactly.
- **Blue (process).** Write the failing tests first, then the code, then
  the docs:
  - unit tests on the fields, the merge and the resolver;
  - context tests on the typed calls and the local activities;
  - DB tests on the store, the table checks and each AC through a worker;
  - HTTP tests on the route, each refusal and the audit rows.

## Design

- `activity_defaults::ActivityDefaults` holds the five values of one
  activity. `ActivityDefaultFields` holds the override: `RetryLimits` and
  four durations. `ActivityDefaultFields::apply` returns the effective
  values. An unset field inherits. A retry field and a heartbeat timeout
  apply only over a code value. `ActivityDefaultPatch` is the tri-state
  change.
- `RunDefaultsCache` holds a second map, keyed by the activity name.
  `refresh` loads both tables. A failed load keeps its own map.
- `WorkflowContext::with_activity_overrides` takes the cache. Each typed
  call resolves call site, then override, then code. A local activity
  resolves the same way, then the builder default.
- `build_activity_enqueue_plan` applies the override to the registered
  defaults. A command value still wins.
- Migration `harvest_activity_default_overrides` adds the table.
- `GET` and `PATCH /admin/activity-types/{activity_name}/defaults` read
  and change the override. Each `PATCH` writes one
  `activity_type.defaults` audit row. `GET /admin/overrides` lists the
  activity overrides.

## Acceptance criteria

1. An activity scheduled after the change takes the override. A scheduled
   one keeps its stored values.
2. Replay of a run that spans the change succeeds.
3. A local activity frozen before the change keeps its frozen values.
4. Clearing the override restores the code value. Every change is audited.
