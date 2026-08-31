### Added

- **Opt-in partitioned `harvest_events` for high-throughput retention (#958).**
  Native Postgres declarative partitioning, so a retention pass reclaims event
  storage by **dropping partitions** — an O(1) metadata operation — instead of
  row-level `DELETE`s that leave dead tuples, index bloat and vacuum debt.
  Measured at 200k rows: the row-`DELETE` path issued 99,740 deletes and left a
  15.41% dead-tuple ratio; the partitioned path issued **zero** and left
  **0.00%**, reclaiming the same 100,000 events in 99 ms, with concurrent append
  and task-claim p99 inside the ±5% budget.

  **Existing deployments are untouched.** The migration
  (`20260727000000_harvest_event_partitioning`) is inert: it ships the machinery
  but converts nothing. Opt in per shard with `harvest partition enable`, or
  `harvest partition plan` for a large live table (the plan keeps the index
  builds and constraint validation outside the `ACCESS EXCLUSIVE` window, so the
  swap itself is metadata-only). `harvest partition disable` reverts.

  Partition creation and reclamation are automated by the retention janitor — no
  operator cron pre-creates partitions. `harvest partition status` reports every
  cohort the sweeper left alone and why, which is the answer to "why has space
  not come back?".

  Zero new `WorkflowEvent` variants, no change to the event JSON at rest, and no
  change to any Diesel-generated statement: the partition key is a `cohort`
  column that `schema.rs` deliberately does not declare, so every read and write
  is byte-for-byte identical in both layouts. Per-execution event ids,
  `load_history`/`load_history_since` ordering and delta loads are unchanged —
  asserted by re-running the existing store, replay, retention, legal-hold and
  end-to-end suites against the partitioned layout in CI.

  Legal holds (#747) and per-type retention overrides (#737) block reclamation
  with no special-casing: a held or over-retained execution keeps its row, which
  keeps its events owned, which blocks the drop.

  Two honest limits, both documented in `docs/partitioned-events.md`: history
  reads do not partition-prune (keep the live partition count under ~32 by
  sizing the cohort width against your retention horizon), and the partitioned
  layout drops the `harvest_events` foreign key — its `ON DELETE CASCADE` is the
  delete storm being eliminated — replacing the insert-time half with a
  validate-only trigger.

### Fixed

- The PII-erasure tombstone `UPDATE` on `harvest_events` now keys on
  `workflow_exec_id` as well as the row id, so the planner prunes to a single
  partition instead of probing every partition's index. No behaviour change on
  the unpartitioned layout.
