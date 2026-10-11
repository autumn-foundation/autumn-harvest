## Storage — export aged partitions before the drop (issue #2009)

On the partitioned layout, the sweep dropped an aged `harvest_events`
partition, and its history was gone. A `PartitionArchiver` now keeps that
history in object storage.

**The seam.** `partition_archive::PartitionArchiver` has two calls,
`put(key, bytes)` and `get(key)`. Core owns the keys, the manifest and every
check. `HarvestBuilder::partition_archiver` registers one.
`DirectoryPartitionArchiver` is a working backend. The S3 and GCS backends of
#1983 fit the same two calls.

**The order.** For each partition that the ownership gate marks as
droppable:

1. Reuse an earlier export at the same key, if it still reads back clean.
2. Otherwise export. Segments of up to 10,000 rows or 8 MiB, one
   `to_jsonb(row)::text` line per row, in key order. The manifest goes last.
3. Verify. Read back each segment and the manifest. Compare the bytes, the
   SHA-256 hashes, the row count and a row checksum, and parse every row.
4. Drop. Under the partition's `SHARE` lock, compute the row count and the
   row checksum in one statement, bounded by `exact_scan_timeout`. Drop only
   when both match the manifest.

A failure keeps the partition, with a reason in `SweepOutcome::blocked`:
`export failed: ...`, `changed since export` or
`export check exceeded its budget`. `SweepOutcome::exported` lists the
manifest key of each dropped partition. Each backend call has the
`archival_timeout_secs` limit. `partition_archive::read_back` checks every
hash and returns typed rows. `ArchivedPartition::history` returns one run's
events.

**Guards.**

- **Marker.** Migration `20261011024658_harvest_partition_export_marker`
  adds `harvest_partition_export`, one row at most. A sweep with an
  archiver writes it. Then a sweep with no archiver on that shard drops
  nothing and reports `export required, but no archiver is set`. This
  covers `harvest partition maintain` and `RetentionRuntime::spawn`.
- **Lock.** A session advisory lock lets one process at a time export a
  shard. The others report `another process is exporting this shard`.
- **Keys.** A key holds the shard and the cohort bounds. Segment and
  manifest keys hold a SHA-256 prefix of their content, so a late upload
  cannot replace a finished object. `dropped.json` names the checked
  manifest, and `partition_archive::find_dropped` reads it.
- **Budget.** One pass makes at most 4 new exports.
- **Stragglers.** `try_build` refuses a partition archiver together with
  `partitions.straggler_grace_secs`. The sweep skips straggler deletes when
  it exports or the shard holds the marker.
- **Durability.** `DirectoryPartitionArchiver` syncs the file, its
  directory, and the parent of each directory it creates before `put`
  returns.

**Delta encoding: measured, declined.** On a 40-turn agent loop
(`autumn-harvest-agent/tests/history_delta_measure.rs`), a delta on the
previous transcript stores 11.3% to 11.4% of the plain bytes. Under the AES-GCM codec
it saves nothing, because each field gets a fresh nonce. gzip stores 12.6% to
47.9%, by text entropy. The repeat comes from the agent layer, which can
record only the new messages of each turn. See
`docs/rnd/2026-10-11-agent-history-delta-encoding.md`.

**Invariants.** One new, empty table. No new `WorkflowEvent` variant. No new
route.
The export reads rows and the drop is DDL, so the two sanctioned
`event_data` writers do not change.

**Also fixed.** #2102 and #2104 merged tests that set
`WorkflowResetRequest::refuse_erased_source`, which #2103 had removed. The
core `integration` target and `mcp_tasks_integration` did not compile. This
change removes the stale field from both tests.

**Tests.**

- `partition_archive_tests`, 15 DB tests: export, verify, drop and read
  back; the legacy partition; the retention runtime; a failed upload; a
  lost object; changed bytes; a slow backend; a row changed or deleted
  after the export; a live owner; no straggler deletes; the marker; the
  lock; reuse; the export budget.
- 17 unit tests in `partition_archive::tests`.
- The builder test
  `a_partition_archiver_with_straggler_deletes_fails_the_build`.
- `history_delta_measure` in `autumn-harvest-agent`.
