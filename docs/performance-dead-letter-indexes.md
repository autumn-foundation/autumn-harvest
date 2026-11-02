# Dead-letter reads use an index (issue #2401)

The Ledger census found DLQ statements that read the whole of
`harvest_dead_letters`. No index held `failed_at`, `original_task_id` or
`queue_name`. Migration `20261102143438_harvest_dead_letter_indexes` adds
three indexes:

- `idx_harvest_dl_failed_at` on `(failed_at)`;
- `idx_harvest_dl_original_task` on `(original_task_id)`;
- `idx_harvest_dl_queue_failed` on `(queue_name, failed_at)`.

## 🎯 Workload

Operators use these statements during an incident, when the DLQ is largest:

- `dlq::list_dead_letters` reads the newest page. The plugin DLQ page,
  `ui::query_dead_letters_for_ui`, sends the same shape.
- `status_summary::dlq_newest_age_secs` reads the newest `failed_at` on each
  status request. It sends `dlq::DLQ_NEWEST_AGE_SQL`.
- `completion_callback::redrive_delivery_at` deletes the dead letter of one
  callback delivery. A bulk callback redrive sends it once per row.
- `dlq::count_redrive_filter_matches` and `dlq::count_bulk_filter_matches`
  count the rows of one queue.
- `dlq::query_dead_letters_for_redrive` and `dlq::query_dead_letters_for_bulk`
  read the oldest rows of one queue. Both cap the read at 100 rows by
  default.

The fixture has 100,000 dead letters in 20 queues, over 7 days. One row in
10 is a callback. Each task id is random. The table is 5,883 pages.
`statements.sql` sends the shape of each statement. It selects four columns,
not every column. The heap is read either way, so the counts barely change.

## 📈 Profile

PostgreSQL 16, `pg_stat_statements.track = all`. One pass loads the catalog
caches. The second pass is measured. A count is `shared_blks_hit +
shared_blks_read` for one call.

| Statement | Before | After |
|---|---:|---:|
| List, one page of 50 | 5,959 | 5 |
| Newest age | 5,883 | 3 |
| Callback redrive delete | 5,884 | 5 |
| Queue count, 5,000 rows | 5,883 | 24 |
| Queue read, 100 oldest | 5,883 | 125 |
| `error ILIKE` count | 5,883 | 5,883 |

Before, each statement read all 5,883 pages. After, the list and the probe
read only the rows they return. A bulk callback redrive of N rows drops from
N full reads to N index lookups. The queue count reads the queue index only.

A btree index cannot serve `error ILIKE '%...%'`, so that count still reads
the whole table. A `pg_trgm` index could serve it. The filter is rare, so
the issue declines it.

## ⚖️ Trade-off: a filter that no index serves

Some reads filter on a column that no index holds, and order by `failed_at`
with a `LIMIT`. Examples are an `activity_name` filter on a bulk replay, an
`error ILIKE` filter on a redrive, and a task-kind filter on the plugin DLQ
page. The planner can now walk `idx_harvest_dl_failed_at` and filter each
row. It expects the matches to spread evenly. When the matches sit at the
far end, the walk reads most of the index, and one heap fetch for each row.

`tradeoff.sql` reads the 100 oldest of the 5,000 newest rows by activity.
That is a redrive of the newest failures, oldest first, with no time window.

| Heap order | Before | After |
|---|---:|---:|
| In `failed_at` order, as inserts leave it | 6,001 | 5,856 |
| Shuffled, as churn leaves it | 5,883 | 95,393 |

With the heap in order, the walk reads about as much as the old scan. With a
shuffled heap, each fetch reads another page, so the walk costs 16 times
more. Both reads still grow with the table. Before, they were full scans
too. A time window, `failed_after` or `dead_lettered_after`, bounds the walk
to the window. A queue filter uses `idx_harvest_dl_queue_failed` instead.

## 💸 Write cost

Each dead-letter insert adds one entry to each new index. `write.sql` runs a
checkpoint first, so the first change to each page writes a full-page image,
as in production. The table shows the first of two runs. `before.txt` and
`after.txt` hold both.

| 1,000 inserts | Before | After |
|---|---:|---:|
| Buffers | 7,151 | 15,198 |
| WAL records | 3,009 | 6,043 |
| Full-page images | 914 | 1,285 |
| WAL bytes | 5,581,664 | 8,532,671 |

That is about 8 more buffers and 3 more WAL records for each insert. A task
id is random, so each insert to `idx_harvest_dl_original_task` lands on a
random leaf page. After a checkpoint, the first insert to each leaf writes a
full-page image. That adds about 3,000 WAL bytes for each insert here. The
time indexes append at their right edge, so they add few images. A row
arrives only on a terminal failure, so the cost does not touch the step
path.

The index sizes after a sorted build of the fixture:

| Index | Pages |
|---|---:|
| `idx_harvest_dl_failed_at` | 276 |
| `idx_harvest_dl_original_task` | 388 |
| `idx_harvest_dl_queue_failed` | 443 |

Together they add about 19% of the table size. Each vacuum of the table also
scans them.

## 🔁 Reproduce

The fixture and the statements are in
[`perf-artifacts/dead-letter-indexes/`](perf-artifacts/dead-letter-indexes/).
The results are in `before.txt` and `after.txt`. `tradeoff.sql` shuffles the
heap inside a transaction and rolls it back. A count varies by a few
percent between runs. Load `pg_stat_statements` in
`shared_preload_libraries` first.

```sh
new=autumn-harvest/migrations/20261102143438_harvest_dead_letter_indexes
createdb dl_before
for dir in autumn-harvest/migrations/*/; do
  [ "${dir%/}" = "$new" ] && break
  psql -d dl_before -1 -f "$dir/up.sql"
done
cd docs/perf-artifacts/dead-letter-indexes
psql -d dl_before -c 'CREATE EXTENSION pg_stat_statements' -f seed.sql
createdb -T dl_before dl_after
psql -d dl_after -1 -f "../../../$new/up.sql" -c 'ANALYZE harvest_dead_letters'
psql -d dl_before -f measure.sql -f write.sql -f tradeoff.sql
psql -d dl_after -f measure.sql -f write.sql -f tradeoff.sql
```

## 🔒 Gate

`census_scan_index_tests` plans each indexed statement with `enable_seqscan`
off and a generic plan. The `error ILIKE` count has no gate. Each engine
gate captures the SQL that the engine function sends. The engine gates cover
the list, the redrive delete, the redrive and bulk-replay counts and reads,
and the aggregate read. The newest-age gate plans `DLQ_NEWEST_AGE_SQL`. The
list and probe gates also require a backward scan under a `Limit`. The gates
check the list order, the probe age, the queue order, and the rows that the
redrive delete keeps.
