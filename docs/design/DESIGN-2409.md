# DESIGN-2409: Override a workflow type's retry policy at runtime

Issue #2409. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the run-default overrides of #2387,
#2406, #2407 and #2408.

## Problem

`WorkflowInfo::retry_policy` (#523) decides whether a failed run starts
another attempt. During an incident an operator may want to stop the
retries, or to slow them. Only a deploy changes the policy today.

## Facts

- Each start freezes a retry policy on the execution row, in the
  `workflow_retry_policy` JSONB column. The source is the type's code
  policy, or the schedule's own `retry_policy` for a schedule fire. The
  fleet ceiling `max_workflow_attempts_ceiling` clamps `max_attempts` first.
- One function decides a workflow retry: `worker::persist_workflow_failure`.
  It reads the stored policy of the failed run. It compares
  `workflow_attempt` to `max_attempts`, checks `non_retryable_errors` and
  computes the delay.
- A retry successor and a same-type continue-as-new successor copy the stored
  policy. A cross-type continue-as-new takes the code policy of the target
  type. A re-run copies the stored policy of its source.
- Nine schedule paths pick the schedule's own policy, or the code policy when
  the schedule sets none. Four are in `scheduler.rs`, four in the plugin
  `api.rs` and one in `ui.rs`.
- The stored JSON does not say which source chose it.
- `RunDefaultFields` is `Copy` and `Eq`. A `RetryPolicy` holds a `Vec` and an
  `f64`, so it is neither.
- The retry decision runs outside workflow code. Replay never reads it.
- Each worker runs a run-defaults refresh loop (#2387). The failure path has
  no registry, so it reads `GLOBAL_RUN_DEFAULTS`, as admission does (#2408).

## Brainstorm

1. Store a whole `RetryPolicy` as JSONB on the override row.
2. Store three scalar fields: `max_attempts`, `initial_interval` and
   `max_interval`. Keep `backoff_coefficient`, `jitter` and
   `non_retryable_errors` from code.
3. Apply the override at start, into the stored policy of each new run.
4. Apply the override at failure, over the stored policy of the failed run.
5. Tell a schedule's own policy from a code policy by a new column on
   `harvest_workflow_executions`.
6. Tell them apart by a marker key in the stored JSON.
7. Tell them apart at failure by a read of the schedule row.
8. Tell them apart by a comparison of the stored policy with the code policy.
9. Let an override add a retry policy to a type that declares none.
10. Add a new route, such as `PATCH /admin/workflow-types/{name}/retry-policy`.

Decision: 2, 4 and 6, on the existing run-defaults route.

- Option 1 breaks `Copy` and `Eq` on every run-default type. Its JSON shape
  makes the operator restate the whole policy. Option 2 covers the two needs
  of the issue: stop (`max_attempts = 1`) and slow (a longer interval).
- Option 3 misses the runs in flight, so it fails AC 1.
- Option 5 changes a hot table that is at its diesel width tier, and every
  insert path of it. Option 6 needs no migration on that table. An older
  binary ignores an unknown key.
- Option 7 is a cross-shard read on the failure path, and it sees a schedule
  change that the run did not start under. Option 8 treats a run started
  under an older code policy as a schedule run, so it fails AC 1.
- Option 9 retries a type whose author never declared it safe to retry. A
  run with no stored policy also has no base for the three fields. The route
  refuses a retry field for a type with no code policy.
- Option 10 duplicates the cache, the audit and the refresh.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A retry successor stores the override policy. A clear then keeps it. | The successor stores the stored policy and its source, never the effective one. A DB test clears the override mid-chain and checks the next failure. |
| A schedule fire forgets the marker, so the override replaces a schedule policy. | Each schedule path calls `workflow_retry::schedule_retry_policy`. A source guard fails on a direct read of a schedule's `retry_policy` outside the allowed lines. |
| A path decodes the stored JSON to a typed policy and drops the marker. | `StartWorkflowParams` carries `workflow_retry_source`. Debounce, throttle, event batch, re-run and the start macro decode with `workflow_retry::decode_stored`. |
| The override changes an attempt that already started. | The override is read only when a run fails. No pass touches a live or a delayed successor. A DB test checks that a started attempt keeps its row, its fire time and its stored policy. |
| An override raises `max_attempts` past the fleet ceiling. | The route refuses it. The failure path also clamps it to `GLOBAL_MAX_WORKFLOW_ATTEMPTS_CEILING`. |
| A longer `initial_interval` has no effect, because `max_interval` caps each delay. | When the override sets an interval, the effective `max_interval` rises to the effective `initial_interval`. The write refuses a merged `max_interval` below `initial_interval`. |
| An interval above 365 days. The retry start refuses such a delay, so the chain ends instead of slowing. | The write, the decode and the table `CHECK` refuse an interval above 365 days, the start-delay limit of a retry. |
| A zero interval. | The write, the decode and the table `CHECK` refuse zero. |
| A stored row that the engine cannot read fails the load of every override. | The `CHECK` constraints match the decode bounds, upper bounds included. |
| The override changes replay. | The decision runs in `persist_workflow_failure`, outside workflow code. The `WorkflowRetryScheduled` event keeps its shape. |
| A change is not audited. | Each `PATCH` writes one audit row, refused or not. |
| An older process ignores the override. | It selects only its own columns, so it keeps the stored policy. The operations guide says so. |
| A run that an older binary started under its schedule's policy has no marker. | The override applies to it. The upgrade guide says so. |

## Six thinking hats

- **White (facts).** One retry decision site. The stored policy is frozen at
  start. Nine schedule paths. No registry on the failure path.
- **Red (feeling).** An operator in an incident wants a single, safe knob.
  "Stop retrying now" must work for the runs in flight too.
- **Black (risks).** A schedule path that drops the marker. A successor that
  stores the effective policy. A test that leaks a global override.
- **Yellow (benefits).** Three nullable columns on a small table. One read on
  the failure path, from memory. No change to the hot table. No replay
  impact.
- **Green (ideas).** The `code`, `override` and `effective` views show the
  three retry fields. Intervals are in milliseconds, so a code value of
  500 ms shows exactly.
- **Blue (process).** Write the failing tests first. Then the code, then the
  docs. The tests are:
  - unit tests on the limits, the merge, the stored source and the resolver;
  - DB tests on the store, the table checks and each AC through a worker;
  - HTTP tests on the route, each refusal and the audit rows;
  - a source guard over the schedule paths.

## Design

- `workflow_retry::RetryLimits` holds `max_attempts`, `initial_interval`
  and `max_interval`. `RetryLimits::apply` returns a policy with each set
  field replaced. `RetryLimitsPatch` is the tri-state change.
- `workflow_retry::RetryPolicySource` is `Type` or `Schedule`.
  `encode_stored` writes the policy JSON, plus the key
  `_harvest_retry_source: "schedule"` for a schedule policy.
  `decode_stored` reads both back.
- `RunDefaultFields::retry` and `RunDefaultPatch::retry` carry the limits.
  `run_defaults::current_retry_policy` resolves the policy of a failure. A
  schedule policy wins. Otherwise the cached override applies, clamped to
  the fleet ceiling.
- `persist_workflow_failure` decides with the resolved policy. The successor
  stores the stored policy and its source.
- Migration `20261105062317_harvest_run_default_retry_policy` adds
  `retry_max_attempts`, `retry_initial_interval_ms` and
  `retry_max_interval_ms`. Each interval stops at 365 days. The row `CHECK`
  widens to ten columns.
- `PATCH /admin/workflow-types/{workflow_name}/run-defaults` takes the three
  fields. `code`, `override` and `effective` report them.

## Acceptance criteria

1. The override applies to the next failure of any run of the type, in
   flight or new.
2. An attempt that already started is not affected.
3. Clearing the override restores the code value. Every change is audited.
4. A schedule's own `retry_policy` still wins for the runs that it starts,
   as today.
