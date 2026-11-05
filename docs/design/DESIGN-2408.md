# DESIGN-2408: Override a workflow type's quota limits at runtime

Issue #2408. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the run-default overrides of #2387,
#2406 and #2407.

## Problem

`WorkflowInfo::quota` (#946) declares three per-tenant admission caps:
`max_active_executions`, `max_history_bytes` and `max_dead_letters`.
`GET /admin/quotas` shows usage against them, but only a deploy changes a
cap. A tenant that floods the fleet, or a tenant that needs headroom, waits
for a release.

## Facts

- Every quota check of a new start, a child spawn, a fork, a DLQ redrive
  and a cross-shard child goes through one function,
  `execution::enforce_quota_admission`. It locks the key, reads the usage
  and calls `quota::check_quota`.
- A fork also compares its copied history to `max_history_bytes` after the
  insert, in `fork::check_stored_history`.
- No task row stores a quota cap. Each admission reads the policy again.
- Admission is the only place where a cap applies. A workflow retry is
  exempt. A continue-as-new successor has no quota check. A refused child
  spawn defers its parent with a backoff. So a cap never ends a run.
- `has_any_cap()` of the code policy opens several gates: the key lock of a
  child spawn, the supersede credit and the lock-order scanner.
- The run-default cache of #2387 holds one `RunDefaultFields` per type. A
  process reloads it within about 6 seconds. `GLOBAL_RUN_DEFAULTS` mirrors
  it for code that has no registry.
- The LLM caps of #1997 are not admission caps. The issue names only the
  three admission caps. `key_expr` stays deploy-only.

## Brainstorm

1. Add the three caps to `RunDefaultFields`, three columns to
   `harvest_run_default_overrides` and three keys to the run-defaults
   route. Reuse the cache, the tri-state patch, the audit row and the
   refresh loops.
2. Make a new table, cache and route, such as `PATCH /admin/quotas/{name}`.
3. Apply the override in each start path, as #2407 did for concurrency.
4. Apply the override once, inside `enforce_quota_admission`, from
   `GLOBAL_RUN_DEFAULTS`.
5. Apply the override in `GLOBAL_WORKFLOW_METADATA` itself, when the cache
   refreshes.
6. Let an override add a cap to a type with no admission cap.
7. Accept a cap of zero, to stop all new work of a type.
8. Store an override per tenant key, not per type.

Decision: 1 and 4, plus the fork check. Option 2 duplicates the cache and
the route. Option 3 touches about 10 call sites, and a new one can forget it.
Option 5 mixes code values and operator values in one map, so the views
cannot show both. Option 6 changes `has_any_cap()`, which opens gates in
other paths. A type with no admission cap stamps and locks no key for those
gates, so the route refuses it. Option 7 duplicates the pause of #618.
Option 8 is out of scope: the issue keys the override by type.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A path compares usage to the code cap and skips the override. | `enforce_quota_admission` applies the override. A source guard fails on a `check_quota` call outside it, and on a cap read outside the allowed lines. |
| A fork checks its history against the code `max_history_bytes`. | The fork applies the override before its admission and its stored-history check. |
| A cross-shard child ships the code caps in its spec. | The target process applies its own override by type name in `enforce_quota_admission`. |
| A lower cap ends a run in flight. | Only admission reads a cap. A retry stays exempt. A refused child spawn defers its parent. A DB test lowers the cap under live runs and checks that each run stays `RUNNING`. |
| An override adds a cap to a type with no key gate. | The route refuses each quota field for a type with no admission cap. The resolver also ignores an override for such a type. |
| A cap of zero or past the column range. | The write refuses zero, a count past `u32::MAX` and a byte cap past `i64::MAX`. The table `CHECK` refuses zero. |
| `GET /admin/quotas` shows a stale override. | It reads the stored override from the default shard. Only when that read fails does it read the process cache. |
| An older process reads a row with quota caps only. | It selects only its own columns, so it sees an empty override and keeps the code values. |
| Clearing the override drops the other fields. | The tri-state merge keeps each field that the patch does not name. |
| A change is not audited. | Each `PATCH` writes one audit row, refused or not. |

## Six thinking hats

- **White (facts).** One function applies every admission cap. No row
  stores a cap. A cap applies at admission only.
- **Red (feeling).** Operators fear that a knob ends live runs. A lower cap
  must only refuse new work.
- **Black (risks).** A missed comparison of a code cap. A stale cache. A
  test that leaks a global override into another test.
- **Yellow (benefits).** Three fields on an existing route, cache and
  table. No new event, no replay impact, no row pass and no change to a hot
  query.
- **Green (ideas).** `GET /admin/quotas` lists each type with an admission
  cap in `workflow_types`, so an override shows before any usage exists.
- **Blue (process).** Write the failing tests first. Then the code, then the
  docs. The tests are:
  - unit tests on the fields, the merge, the cache and the resolver;
  - DB tests on the store, the table check and the enforcement;
  - HTTP tests for each AC and each refusal;
  - a source guard over the cap reads.

## Design

- `quota::QuotaLimits` holds the three optional caps.
  `QuotaLimits::apply` returns the code policy with each set cap replaced,
  only when the code policy has an admission cap.
- `RunDefaultFields::quota` and `RunDefaultPatch::quota` carry the caps.
  `RunDefaultsCache::apply_quota` applies the cached override.
- `run_defaults::current_quota_policy` applies `GLOBAL_RUN_DEFAULTS`.
  `enforce_quota_admission` and the fork call it.
- Migration `20261105015009_harvest_run_default_quota_limits` adds three
  `BIGINT CHECK (> 0)` columns and widens the row `CHECK` to seven columns.
- `PATCH /admin/workflow-types/{workflow_name}/run-defaults` takes
  `quota_max_active_executions`, `quota_max_history_bytes` and
  `quota_max_dead_letters`. `code`, `override` and `effective` report them.
- `GET /admin/quotas` reports `limits` as the effective caps, and adds
  `code_limits` and `override` to each entry. A new top-level
  `workflow_types` list holds the same three objects for each registered
  type with an admission cap.

## Acceptance criteria

1. A lower limit refuses new work for a tenant over it. It never ends a run
   in flight.
2. `GET /admin/quotas` reports the effective limit and names the override.
3. Clearing the override restores the code value. Every change is audited.
