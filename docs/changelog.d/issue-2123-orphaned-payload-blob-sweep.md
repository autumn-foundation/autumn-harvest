## Engine — Orphaned payload blob sweep (issue #2123)

**Behavior change.** None by default. The sweep is opt-in.

**The leak.** An append uploads its offloaded payloads before it opens its
transaction (issue #954). A rollback after the upload left the blobs in the
store with no `harvest_payload_refs` row. Retention finds blobs only through
those rows, so nothing deleted them.

**The sweep.** The retention runtime now runs `payload_orphans::sweep_orphaned_blobs`
once per tick.

- Turn it on with `RetentionConfig::with_payload_orphan_grace`. The grace
  period is at least one hour.
- It deletes a blob only when the blob is older than the grace period and no
  shard references it. It checks every database. It checks the age itself and
  does not trust the store to filter.
- A failed reference check stops the sweep. It never deletes on a guess.
- Each tick reads at most 100 pages. A cursor carries the listing across ticks.
- `dry_run` counts orphans and deletes nothing.

**New trait methods.** `PayloadStore::list(prefix, created_before, cursor)` is
optional. The default returns `None`, so existing stores compile and the sweep
stays off for them. The log says so once. `created_at` must be the time of the
latest `put`.

`PayloadStore::delete_if_older(key, created_before)` is also optional. It
deletes a blob only if its latest write is before the cutoff, in one atomic
step. The sweep uses it when the store offers it, so a writer that re-uploads
a key after the listing keeps its blob. The default returns `None`, and the
sweep then falls back to `delete`.

**Metric.** `harvest.retention.payload_orphans` counts `found`, `deleted` and
`failed` blobs per `store.id`.

**Limits.** The store must hold only harvest blobs. Do not share it with the
agent response cache. Each retention process must pool every shard database,
or the sweep misses references. A store without
`delete_if_older` keeps a race open: a re-upload of identical bytes during one
page of checks can lose to a plain delete. The `list` contract requires `put`
to refresh `created_at`, which closes the race for earlier uploads.

**Invariants.** No migration, no `WorkflowEvent` variant and no replay impact.
The sweep never writes `harvest_events`.

**Tests.** `payload_orphan_sweep_tests` runs against Postgres: a rolled-back
append, a blob that only one of two shards references, a young blob with a
faulty store, a store without listing, the page budget and cursor, `dry_run`,
and the retention runtime with its metrics.
