## Engine — Sweep of blobs from rolled-back writes (issue #2123)

**Behavior change.** The retention runtime now deletes payload blobs that a
rolled-back write uploaded. An append uploads its blobs before it opens its
transaction (#954). A rollback left those blobs in the store with no
`harvest_payload_refs` row, and retention finds blobs only through those rows.

**The sweep.** It runs once per tick when the payload store can list.

- `PayloadStore::list(after, created_before, limit)` is new and optional. The
  default returns `Ok(None)`, so existing stores keep working. The first `None`
  turns the sweep off and logs it once.
- A blob is an orphan when it is older than the grace period and no
  `harvest_payload_refs` row names it on any database.
- A failed reference check on any database deletes nothing from that page.
- Just before each delete, the sweep checks the references of that key and
  reads its creation time again. A re-upload since the listing keeps the blob.
- Each `list` and `delete` has the `archival_timeout` and ends at shutdown.
- It reads up to 10 pages of `batch_size` blobs per tick and keeps its cursor
  between ticks.
- Under `dry_run`, it counts orphans and deletes none.

**Store contract.** Implementing `list` is the opt-in. It returns only blobs
that `put` wrote, scoped to the store prefix. Do not share one store between
deployments with different databases. A `put` of an existing key must reset its
creation time. `delete` of a missing key must succeed. `ObjectPayloadStore` uses a new key
for each upload and needs no reset. No shipped backend lists yet, so the sweep
is inert until an embedder implements `list`.

**Config.** `RetentionConfig::orphan_blob_grace_secs`, default 1 day. The
floor is 1 hour. `without_orphan_blob_sweep()` turns the sweep off.

**Metrics.** `harvest.retention.orphan_blobs_found`,
`harvest.retention.orphan_blobs_deleted` and
`harvest.retention.orphan_blob_delete_failures`.

**No migration. No new `WorkflowEvent` variant. No change to the write path.**
See `DESIGN-2123.md`.
