# DESIGN-2401: Dead-letter reads use an index

Issue #2401. Parent census: #2366. Related: #2424 and #2367 (the earlier
census indexes), #510 (redrive), #921 (callback redrive).

## Problem

`harvest_dead_letters` has four indexes: the primary key, `owner`,
`(workflow_name, quota_key)` and `workflow_exec_id`. No index holds
`failed_at`, `original_task_id` or `queue_name`. So these statements read
the whole table:

- the DLQ list, `ORDER BY failed_at DESC LIMIT n`;
- the callback redrive delete, `WHERE original_task_id = $1 AND task_type = $2`;
- each filter on `queue_name`, in a count, a bulk read or a redrive read;
- the newest-age probe of the plugin status summary, `MAX(failed_at)`.

A bulk callback redrive sends the delete once per row. A redrive of N rows
therefore reads the table N times.

## Facts

- A row arrives only on a terminal failure. Retention deletes the rows of a
  run when it purges the run. So the table is small in steady state, and
  largest during an incident.
- Operators list, filter and redrive during that incident.
- `failed_at`, `queue_name` and `original_task_id` are `NOT NULL`.
- Only `completion_callback::redrive_delivery_at` filters on
  `original_task_id`. Diesel binds `task_type` as a parameter.
- Each read orders by `failed_at` alone. No read orders by `id`.
- The table is not hot (`docs/upgrading/online-migrations.md`, section 1).
  A plain build takes `SHARE`. That blocks each dead-letter insert, and each
  insert runs in the transaction that fails a task.
- `test_init_sql()` sends every `up.sql` in one batch, so a
  `CONCURRENTLY` migration does not run in the test bundle.
- No SQLite migration creates a dead-letter table.

## Brainstorm

1. The three indexes of the issue: `(failed_at DESC, id)`,
   `(original_task_id)` and `(queue_name, failed_at)`.
2. `(failed_at)` in place of `(failed_at DESC, id)`. A btree scans in both
   directions, so one index serves `DESC`, `ASC` and `MAX`.
3. A partial index on `original_task_id WHERE task_type = 'CALLBACK'`.
4. `(queue_name)` alone, with a sort above it.
5. A `pg_trgm` GIN index for `error ILIKE`.
6. Keyset pages on `(failed_at, id)` for the list.
7. A cached newest `failed_at` for the status summary.
8. One migration per index, each `CONCURRENTLY`.

Decision: option 2 for `failed_at`, plus the `original_task_id` and
`(queue_name, failed_at)` indexes of option 1. A guard wraps each build, and
one migration holds all three.

- 2 beats `(failed_at DESC, id)`. No statement orders by `id`, so the
  suffix only widens each entry.
- The design rejects 3. The delete binds `task_type`, so a generic plan cannot
  match the predicate. Issue #2367 found that defect. A full index needs no
  query change.
- The design rejects 4. `(queue_name, failed_at)` also serves the order and the
  `failed_at` range of each queue filter.
- The design rejects 5. It needs an extension, and the filter is rare.
- 6 and 7 change behaviour. The indexes make both unnecessary for now.
- The design rejects 8. The test bundle cannot run it. The guarded build gives the
  same result to an operator who prebuilds the index.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A plain build on a large DLQ during an incident blocks each task failure. The failure keeps its row locks, so signals on that run wait too. | The migration header and the upgrade guide give the `CONCURRENTLY` recipe. `lock_timeout` is 5 s. |
| An operator prebuilds a same-named index with another definition. | The guard compares the definition and fails. |
| A failed prebuild leaves an `INVALID` index. | The guard fails and names the fix. |
| A `blocking-index` annotation on a table that is not hot. | The lint reports `unused-annotation`. The migration has no annotation. |
| A partial index that a generic plan cannot use. | Each index is full. The gates force a generic plan. |
| A gate plans a copy of the SQL, so it passes after the engine changes. | Each engine gate captures the SQL that the engine function sends. |
| A gate skips when it has no database, so it passes over the defect. | `plan_gate_db` panics with no database. |
| A gate accepts a full scan of the primary key. | Each gate asserts the index name. |
| The redrive delete removes another row. | A test seeds three rows and checks that one goes. |
| A change to the list order. | No query changes. A test checks newest first and the limit. |
| A migration version that collides with another branch. | A second-precision UTC prefix. |
| Each failure in a storm writes three more index entries. | The perf page measures the insert cost, with random task ids. |
| A filter that no index serves walks the `failed_at` index from the wrong end. | The perf page measures it on a shuffled heap. The upgrade guide names the bound: a time window or a queue. |
| A gate accepts a full index walk for the list or the probe. | Those gates also require a backward scan under a `Limit`. |

## Six hats

- **White.** The facts above. The plan census and the issue comment name
  the statements.
- **Red.** The status summary runs on each request. That probe must not
  grow with the DLQ.
- **Black.** Three more entries for each insert. A plain build blocks task
  failures. The `error ILIKE` count still reads the table. A filter that no
  index serves can walk the `failed_at` index. On a shuffled heap, with the
  matches at the far end, that walk costs 16 times the old scan.
- **Yellow.** A list page reads `limit` rows. The newest-age probe reads one
  row. A redrive of N rows does N index lookups.
- **Green.** One `(failed_at)` index serves both orders and `MAX`. One
  composite index serves the queue filter, its order and its range.
- **Blue.** Red gates first. Then the migration. Then the docs, the
  measurement and the gates. Then a review.

## Decision

Migration `20261102143438_harvest_dead_letter_indexes` builds:

- `idx_harvest_dl_failed_at` on `harvest_dead_letters (failed_at)`;
- `idx_harvest_dl_original_task` on `harvest_dead_letters (original_task_id)`;
- `idx_harvest_dl_queue_failed` on `harvest_dead_letters (queue_name, failed_at)`.

A guard wraps each build, as in `20261030182800`. No query changes. The
`error ILIKE` filter and an unfiltered count still read the table. That
cost is inherent.

The design accepts the walk of the trade-off above. Before, those reads were
full scans too. The wins run on each list page, each status request and
each callback redrive row. Two follow-ups can remove the walk: an
`(activity_name, failed_at)` index, and a time window on an unbounded
redrive.

The plugin reads, `ui::query_dead_letters_for_ui` and the API bulk reads,
have no gate. They send the same filters and orders as the engine reads. The
Ledger plan pages record their access paths.

## Acceptance criteria

1. The list, the callback redrive delete, each queue filter and the
   newest-age probe plan an index scan, with `enable_seqscan` off and a
   generic plan.
2. Each engine gate plans the SQL that the engine sends.
3. The list keeps newest first and its limit. A queue redrive keeps oldest
   first. The redrive delete removes its own row only.
4. The migration passes `migration_lock_safety`.
5. A perf page measures the reads and the insert cost.
