# DESIGN-2411: Override an activity type's max_concurrent cap at runtime

Issue #2411. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the activity default overrides of
#2410 and on the workflow concurrency override of #2407.

## Problem

`ActivityInfo::max_concurrent` caps the running tasks of one
`concurrency_key` across the fleet. During an incident an operator must
protect a downstream. The activity pause (#807) stops all work. A forced
open circuit (#369) fails all work. Neither one lowers the cap. An adaptive
limit (#1836) is a separate cap for each worker.

## Facts

- The worker resolves the key and the cap in `build_activity_enqueue_plan`.
  The key is `concurrency_key`, or the activity name. The row stores the
  cap as `concurrency_cap`.
- Each claim path admits a row only while the count of `RUNNING` rows of
  its key and task type is below the row's own `concurrency_cap`.
  `release_claim_if_over_cap` checks the same bound after a claim.
- A retry sets the same row back to `PENDING`. It keeps its stored cap.
- So a change that applies to new rows only leaves the pending rows under
  the old cap. One key then holds rows with two caps.
- The builder refuses two activities that share a key with two caps
  (`ConcurrencyKeyMismatch`).
- A local activity runs inline in the workflow task. It has no task row, so
  no cap applies to it.
- The cache of #2410 holds one `ActivityDefaultFields` per activity. Each
  process reloads it within `RUN_DEFAULTS_TTL` plus one loop period.
- No index covers `concurrency_cap`. An update of it can stay a HOT update.

## Brainstorm

1. Add `max_concurrent` to `ActivityDefaultFields`, a column to
   `harvest_activity_default_overrides` and a key to the #2410 route.
2. Make a new table, cache and route for the cap.
3. Make the claim read the override. Each claim then uses the current cap.
4. On a change, set the stored cap of the live rows of the activity to the
   effective cap, on every shard. Lower and raise.
5. Raise only, as #2407 does.
6. Rewrite `PENDING` rows only, and leave `RUNNING` rows.
7. Key the override by `concurrency_key`, not by activity name.
8. Let the override add a cap to an activity that declares none.
9. Repeat the rewrite after the cache TTL, to catch rows that a stale
   process wrote.

Decision: 1, 4 and 9.

- Option 2 duplicates the cache, the route and the audit row.
- Option 3 needs the override on every shard. It changes each claim
  variant of the hot claim query and its prepared plans.
- Option 5 fails AC 1: a lower cap must hold for the pending rows too.
- Option 6 leaves a running row with the old cap. Its retry then claims
  under the old cap.
- Option 7 adds a new identity that no other override uses. The route
  refuses an activity that shares its key instead.
- Option 8 needs a key on rows that have none, and a clear must remove it
  again. The activity pause covers an activity with no cap.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A pending row keeps the old cap, so a claim admits it over the new cap. | The `PATCH` rewrites each `PENDING` and `RUNNING` row of the activity on every shard. A DB test claims after a lower cap. |
| A running task stops, or a worker loses its claim. | The rewrite changes `concurrency_cap` only. It never changes the state, the worker or the attempt. A DB test completes each running task after the change. |
| A retry of a running task claims under the old cap. | The rewrite covers `RUNNING` rows. A DB test retries a rewritten row. |
| A process with a stale cache writes a row under the old cap. | Settle passes run again at 10 seconds, 1 minute and 5 minutes. A DB test rewrites a stale row. |
| Two changes race, and an old rewrite runs after a newer one. | Each batch takes the activity lock on the default shard and reads the stored override there. It stops when the effective cap changed. |
| Two activities share a key, and one key gets two caps. | The route refuses `max_concurrent` when another registered activity shares the key. |
| An override adds a cap to an activity with none. | The route refuses it. The resolver ignores it too. |
| An override applies to a local activity, which has no row. | The route refuses it. |
| A zero cap, or one past the `INTEGER` column. | The write and the decode refuse both. The table `CHECK` refuses zero. |
| A rewrite holds many row locks on the hot task table. | It updates 500 rows per transaction, with `SKIP LOCKED`. It retries a skipped row a few times, then reports it as unresolved for a settle pass. |
| A claim holds a row through the pass, so the row keeps the old cap. | The retries wait for the short claim transaction. The response reports a row that stays held. |
| The API process does not register a sharer of the key. | The worker resolver ignores the override when its registry holds a sharer. |
| A client disconnects and the rewrite stops. | Each pass runs in its own task. |
| Clearing the override drops the other fields of #2410. | The tri-state merge keeps each field that the patch does not name. |
| A change is not audited. | Each `PATCH` writes one audit row in the same transaction as the change. A refusal writes a failed audit row. |

## Six thinking hats

- **White (facts).** The cap lives on each row. The worker and the
  dead-letter replay and redrive set it.
  Each claim path compares the running count of the key to the row's cap.
- **Red (feeling).** An operator wants to slow a downstream, not to stop
  it. A lower cap must not kill a task in flight.
- **Black (risks).** A write on the hot task table. A stale cache. A shared
  key. A retry that comes back under the old cap.
- **Yellow (benefits).** One more field on an existing table, cache and
  route. No change to the claim query, no new event, no replay impact.
- **Green (ideas).** Reuse the shard pass and the settle waits of #2407.
  Report the rewritten rows in the response.
- **Blue (process).** Write the failing tests first, then the code, then
  the docs:
  - unit tests on the fields, the merge, the resolver and the refusals;
  - DB tests on the store, the table check, the rewrite and each AC;
  - HTTP tests on the route, each refusal and the audit rows.

## Design

- `ActivityDefaults` and `ActivityDefaultFields` get `max_concurrent`.
  `ActivityDefaultPatch` gets the tri-state field. The cap applies only
  over a code cap.
- `HandlerRegistry::activity_concurrency` resolves the key and the
  effective cap. The worker enqueue and the dead-letter replay and redrive
  call it. It ignores the override when another registered activity shares
  the key.
- Migration `20261107194243_harvest_activity_default_max_concurrent` adds
  `max_concurrent INTEGER CHECK (max_concurrent > 0)` and widens the row
  `CHECK`.
- `activity_defaults::set_live_activity_caps` sets the cap of the live rows
  of one activity on one shard. It runs in batches under the activity lock
  of the default shard. It skips a held row and retries it a few times. A
  row that stays held counts as unresolved, and a settle pass sets it.
- `PATCH /admin/activity-types/{activity_name}/defaults` takes
  `max_concurrent`, from 1 to 2147483647. It refuses the field for a local
  activity, for an activity with no code cap and for a shared key. A
  `PATCH` that names `max_concurrent` or `clear_override` runs the rewrite
  on every shard, now and at each settle wait. The response reports the
  first pass as `cap_rewrite`.

## Acceptance criteria

1. A lower cap holds for every claim after the change, pending tasks
   included.
2. A task in flight keeps running.
3. Clearing the override restores the code value. Every change is audited.
