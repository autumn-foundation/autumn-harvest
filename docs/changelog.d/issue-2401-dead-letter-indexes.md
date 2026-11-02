## Performance — dead-letter reads use an index (issue #2401)

The Ledger census found DLQ statements that read the whole of
`harvest_dead_letters`. No index held `failed_at`, `original_task_id` or
`queue_name`:

- the newest-first list, `dlq::list_dead_letters`, and the plugin DLQ page;
- the newest-age probe of the status summary. It moves to
  `dlq::DLQ_NEWEST_AGE_SQL`;
- the callback redrive delete, `completion_callback::redrive_delivery_at`.
  A bulk callback redrive sends it once per row;
- each `queue_name` filter of a redrive, a bulk replay, a bulk discard and
  an aggregate, in the engine and the plugin API.

Migration `20261102143438_harvest_dead_letter_indexes` builds
`idx_harvest_dl_failed_at` on `(failed_at)`, `idx_harvest_dl_original_task`
on `(original_task_id)` and `idx_harvest_dl_queue_failed` on
`(queue_name, failed_at)`. Each index is full. The redrive delete binds
`task_type`, so a generic plan cannot use a partial index on it. On 100,000
rows, a list page falls from 5,959 to 5 buffers, the probe from 5,883 to 3,
and the redrive delete from 5,884 to 5. Each dead-letter insert costs about
8 more buffers and 330 more WAL bytes. See
`docs/performance-dead-letter-indexes.md`.

The `error ILIKE` filter still reads the whole table. A btree index cannot
serve it.

**Upgrade.** A plain `CREATE INDEX` takes `SHARE` on `harvest_dead_letters`.
It blocks each dead-letter insert, and so each terminal task failure, until
the build ends. On a large DLQ, build the indexes `CONCURRENTLY` first. The
header of the migration's `up.sql` gives the statements.

`census_scan_index_tests` gates six DLQ statements with `enable_seqscan` off
and a generic plan. Each engine gate captures the SQL that the engine sends.
The gates also check the list order, the probe age, the queue order, and the
rows that the redrive delete keeps. Design record:
`docs/design/DESIGN-2401.md`. No new `WorkflowEvent` variant and no replay
impact.
