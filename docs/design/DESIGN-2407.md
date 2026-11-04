# DESIGN-2407: Override a workflow type's concurrency limit at runtime

Issue #2407. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the run-default overrides of #2387
and #2406.

## Problem

`WorkflowInfo::concurrency` caps the runs that share a key. Code declares the
cap. During an incident an operator may need a lower cap, to shed load from a
downstream, or a higher cap, to drain a backlog. `GET /admin/concurrency`
shows the cap, but only a deploy changes it.

## Facts

- A start resolves `(key, limit, on_conflict)` from the declared policy. It
  stores the limit on its workflow task row as `concurrency_cap`.
- The claim admits a row only while the count of RUNNING rows of its key and
  task type is below the row's own `concurrency_cap`.
- A run in flight keeps one workflow task row for its life. The row is parked
  between decisions, and a wake sets it back to PENDING with its stored cap.
- A latest-wins policy (`cancel_running`, #811) cancels the oldest runs of the
  key until the count respects the limit. So any drop of the limit cancels
  more runs. A latest-wins type never queues a start, so a raise drains
  nothing.
- About 20 start paths read `info.concurrency` themselves. They include the
  HTTP routes, the scheduler, the DLQ replay, reset, the completion trigger,
  the typed stubs and a child spawn.
- Some paths copy a limit that they resolved earlier: a debounce, throttle or
  event-batch fire, a cross-shard child relay, a same-type continue-as-new
  and a workflow retry.
- The cache of #2387 holds one `RunDefaultFields` per type. Each process
  reloads it within about 6 seconds. A start reads it from memory.

## Brainstorm

1. Add `concurrency_limit` to `RunDefaultFields`, a column to
   `harvest_run_default_overrides` and a key to the run-defaults route. Reuse
   the cache, the tri-state patch, the audit row and the refresh loops.
2. Make a new table, cache and route for the concurrency limit.
3. Apply the override in the claim SQL, so each claim reads the current cap.
4. Resolve the policy in one registry method. Each start path calls it.
5. On a change, raise the stored cap of the live rows of the type.
6. Let a new start raise the cap of the waiting rows of its key.
7. Let a lower limit lower the cap of the live rows too.
8. Let a latest-wins type take an override, and make the supersede respect
   the caps of the runs that it cancels.

Decision: 1, 4 and 5, plus a second raise after the cache TTL. Option 2
duplicates the cache and the route. Option 3 needs the override on every
shard and changes the hot claim query. Option 6 adds a write to the start
path. Option 7 slows the runs in flight. Option 8 changes the supersede and
its lock order for a type that gains nothing from an override.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A start path reads the code limit and skips the override. | `HandlerRegistry::concurrency_policy` is the one resolver. A source guard fails on a `.concurrency` read that is not an argument of an open `apply_concurrency*` call. An unknown receiver counts as a read. |
| A deferred start fires with the limit of its admission. | Each fire re-resolves the limit: debounce, throttle, event batch, the completion-trigger relay and the cross-shard child relay. |
| A latest-wins type cancels runs after the override drops. | The route refuses every override for a latest-wins type. The resolver also ignores a stored one. |
| A lower limit stops a run in flight. | No pass lowers a stored cap. A successor or a retry takes the larger of its stored cap and the current limit. |
| A raise leaves a waiting start or a parked run behind its old cap. | The `PATCH` raises each PENDING or RUNNING workflow task row of the type, on every shard. |
| A process with a stale cache stores the old cap after the raise. | A second raise runs after twice the cache TTL. It also lifts a row that the first raise skipped. |
| A client disconnects and the raise stops. | Each raise runs in its own task. |
| Two changes race, and an old raise runs after a newer lower limit. | Each raise batch takes the type lock and reads the stored override. It stops when the effective limit changed. |
| A raise holds many row locks on the hot task table. | It updates 500 rows per transaction, with `SKIP LOCKED`. |
| A limit of zero, or one past the `INTEGER` column. | The write refuses both. The table `CHECK` refuses zero. |
| An override on a type with no policy, or on a DAG. | The route refuses it. A start of such a type has no key. |
| An old process reads a row with a limit only. | It decodes no limit field. It uses the code values, as for the chain cap of #2406. |
| Clearing the override drops the other fields. | The tri-state merge keeps each field that the patch does not name. |
| `GET /admin/concurrency` shows a stale value. | It reads the stored override from the default shard, not the cache. |

## Six thinking hats

- **White (facts).** The cap lives on each task row. The claim compares the
  running count to the row's cap. The supersede uses the limit of the start.
- **Red (feeling).** Operators fear that a knob can kill live runs. A lower
  limit must only make new starts wait.
- **Black (risks).** A missed start path. A limit copied before a change. A
  write on the hot task table. A latest-wins type that cancels more runs.
- **Yellow (benefits).** One more field on an existing route, cache and
  table. No new event, no replay impact and no change to the claim query.
- **Green (ideas).** The raise reuses the batch and lock pattern of the SLA
  recompute. One resolver removes 20 copies of the policy lookup.
- **Blue (process).** Write the failing tests first. Then the code, then the
  docs. The tests are:
  - unit tests on the fields, the merge, the cache and each resolver;
  - DB tests on the store and the raise;
  - HTTP tests for each AC, the refusals and the second raise;
  - a source guard over the start paths.

## Design

- `RunDefaultFields` and `RunDefaultPatch` get `concurrency_limit`.
  `RunDefaultFields::apply_concurrency` and `RunDefaultsCache::apply_concurrency`
  return the declared policy with the effective limit. A latest-wins policy
  keeps its code limit.
- `HandlerRegistry::concurrency_policy(name)` returns the effective policy of
  a registered type. The registry and the client have
  `apply_concurrency_run_default`. The completion-trigger path has
  `target_concurrency`, and each deferred fire has
  `fire_time_concurrency_limit`.
- Migration `20261104173743_harvest_run_default_concurrency_limit` adds
  `concurrency_limit INTEGER CHECK (> 0)` and widens the row `CHECK` to four
  columns.
- `PATCH /admin/workflow-types/{workflow_name}/run-defaults` takes
  `concurrency_limit`, from 1 to 2147483647. It refuses the field for a DAG,
  for a type with no policy and for a latest-wins type. `code`, `override`
  and `effective` report it.
- A `PATCH` that names `concurrency_limit` or `clear_override` runs
  `raise_waiting_concurrency_caps` on every shard, now and again after
  `CONCURRENCY_RAISE_SETTLE_DELAY`. The response reports the first raise as
  `concurrency_raise.raised_tasks`, `unavailable_shards` and `superseded`.
- `GET /admin/concurrency` adds `code_limit`, `override_limit` and
  `effective_limit` to each workflow type with a policy in a `workflow`
  group.

## Acceptance criteria

1. A lower limit applies to the next start. It never cancels a run in flight.
2. A higher limit admits waiting starts at the next start or admission.
3. `GET /admin/concurrency` reports the code limit, the override and the
   effective limit.
4. Clearing the override restores the code value. Each change writes an
   audit row.
