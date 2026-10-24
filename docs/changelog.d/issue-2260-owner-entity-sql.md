## Feature — Owner-local SQLite projection for SQL over entity state (issue #2260)

Stage 3b of epic #2247. A decision owner can keep the last state snapshot
of each live entity in its disk cache file. User code then runs read-only
SQL over the state of one entity with `Worker::query_entity_sql`. See
`docs/adr/0012-owner-disk-cache.md` and the `entity_sql` module docs.

- **Opt-in.** Build with the `owner-disk-cache` feature. Set
  `WorkerConfig::with_owner_disk_cache(dir)` and
  `WorkerConfig::with_owner_entity_sql(true)`. The projection acts only in
  owner mode, on the partitions that the worker owns. The switch with no
  directory fails the worker start.
- **A projection, never a source of truth.** After each committed
  suspension, the owner seals the last snapshot event of the run into the
  `harvest_entity_projection` table of the partition file. A query reads
  that row only when the run and the event id match the Postgres row
  `harvest_entity_state`. Otherwise it reads the snapshot event from
  history, and an owner writes the row again.
- **The disk cache rules apply.** The row lives in the stage 2 partition
  file. So the memory-only node key, the wipe at start and at a tenure
  end, the validity stamp, the purges and the reconcile pass all apply. A
  file that left the directory reads as stale, and the next store makes a
  new file.
- **Read-only SQL.** Each query gets a fresh in-memory SQLite database with
  one table, `entity`. An authorizer allows reads and functions only.
  `ATTACH`, `VACUUM INTO`, pragmas and writes are refused, so no plaintext
  reaches the disk. A query stops after 50 million steps or past 10,000
  rows. Parameters bind to `?1`, `?2` and so on.
- **Any worker can query.** A worker that holds no tenure for the entity
  reads history and writes no file.

**Tests.** `deleting_the_file_mid_run_rebuilds_from_history` is the red
test of the issue. `an_erased_entity_leaves_no_local_bytes`,
`an_erasure_purges_an_entity_that_a_missed_purge_left` and
`a_rotated_codec_key_leaves_no_entity_ciphertext_on_disk` run the stage 2
erasure and key-rotation tests on the projection. `a_killed_owner_leaves_no_readable_bytes`
now stores an entity row too. The `entity_sql` module tests cover the
read-only rules and the limits.

No migration. No new `WorkflowEvent` variant. Off by default.
